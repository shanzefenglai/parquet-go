package goparquet

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"io"
	"io/ioutil"
	"sync"
)

var (
	compressors    = make(map[parquet.CompressionCodec]BlockCompressor)
	compressorLock sync.RWMutex
)

type (
	// BlockCompressor is an interface to describe of a block compressor to be used
	// in compressing the content of parquet files.
	BlockCompressor interface {
		CompressBlock([]byte) ([]byte, error)
		DecompressBlock([]byte) ([]byte, error)
	}

	plainCompressor  struct{}
	snappyCompressor struct{}
	gzipCompressor   struct{}
	zstdCompressor   struct{}
)

func (plainCompressor) CompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (plainCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (snappyCompressor) CompressBlock(block []byte) ([]byte, error) {
	return snappy.Encode(nil, block), nil
}

func (snappyCompressor) DecompressBlock(block []byte) ([]byte, error) {
	return snappy.Decode(nil, block)
}

func (gzipCompressor) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	if _, err := w.Write(block); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (gzipCompressor) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ret, r.Close()
}

func (zstdCompressor) CompressBlock(block []byte) ([]byte, error) {
	var dst []byte
	var encoder *zstd.Encoder
	var err error
	encoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithZeroFrames(true),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		return dst[:0], err
	}
	return encoder.EncodeAll(block, dst[:0]), nil
}

func (zstdCompressor) DecompressBlock(block []byte) ([]byte, error) {
	var dst []byte
	var decoder *zstd.Decoder
	var err error
	decoder, err = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		return dst[:0], err
	}
	return decoder.DecodeAll(block, dst[:0])
}

func compressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, fmt.Errorf("method %q is not supported", method.String())
	}

	return c.CompressBlock(block)
}

func decompressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	compressorLock.RLock()
	defer compressorLock.RUnlock()

	c, ok := compressors[method]
	if !ok {
		return nil, fmt.Errorf("method %q is not supported", method.String())
	}

	return c.DecompressBlock(block)
}

func newBlockReader(buf []byte, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32, alloc *allocTracker) (io.Reader, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.New("invalid page data size")
	}

	if len(buf) != int(compressedSize) {
		return nil, fmt.Errorf("compressed data must be %d byte but its %d byte", compressedSize, len(buf))
	}

	alloc.test(uint64(uncompressedSize))
	res, err := decompressBlock(buf, codec)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}
	alloc.register(res, uint64(len(res)))

	if len(res) != int(uncompressedSize) {
		return nil, fmt.Errorf("decompressed data must be %d byte but its %d byte", uncompressedSize, len(res))
	}

	return bytes.NewReader(res), nil
}

// RegisterBlockCompressor is a function to to register additional block compressors to the package. By default,
// only UNCOMPRESSED, GZIP and SNAPPY are supported as parquet compression algorithms. The parquet file format
// supports more compression algorithms, such as LZO, BROTLI, LZ4 and ZSTD. To limit the amount of external dependencies,
// the number of supported algorithms was reduced to a core set. If you want to use any of the other compression
// algorithms, please provide your own implementation of it in a way that satisfies the BlockCompressor interface,
// and register it using this function from your code.
func RegisterBlockCompressor(method parquet.CompressionCodec, compressor BlockCompressor) {
	compressorLock.Lock()
	defer compressorLock.Unlock()

	compressors[method] = compressor
}

// GetRegisteredBlockCompressors returns a map of compression codecs to block compressors that
// are currently registered.
func GetRegisteredBlockCompressors() map[parquet.CompressionCodec]BlockCompressor {
	result := make(map[parquet.CompressionCodec]BlockCompressor)

	compressorLock.Lock()
	defer compressorLock.Unlock()

	for k, v := range compressors {
		result[k] = v
	}

	return result
}

func init() {
	RegisterBlockCompressor(parquet.CompressionCodec_UNCOMPRESSED, plainCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_GZIP, gzipCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_SNAPPY, snappyCompressor{})
	RegisterBlockCompressor(parquet.CompressionCodec_ZSTD, zstdCompressor{})
}
