#pragma once

#include "config.h"

#if USE_GPU

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** GDeflate (https://github.com/NVIDIA/libdeflate/tree/gdeflate) is a variant of Deflate whose bit stream is split
  * into 32 interleaved sub-streams, so that a GPU can expand it in parallel, as nvcomp and DirectStorage do, while the
  * CPU compresses and expands it about as fast as Deflate. The host compresses, and expands what it reads itself; the
  * GPU aggregation expands the chunks of the blocks it reads with nvcomp, with those of `LZ4` and `ZSTD`.
  *
  * A block is compressed in chunks of `chunk_bytes`, the last one shorter, each a GDeflate page of its own, which is
  * what a GPU expands in parallel with the others. The block holds the number of chunks, a little-endian 32-bit integer,
  * then the compressed size of each as one, then the chunks, each from a multiple of 4 bytes, as nvcomp reads them.
  */
class CompressionCodecGDeflate : public ICompressionCodec
{
public:
    static constexpr size_t chunk_bytes = 64 * 1024;

    /// The levels of libdeflate, from the fastest, 1, to the densest, 12.
    static constexpr int MIN_LEVEL = 1;
    static constexpr int MAX_LEVEL = 12;
    static constexpr int DEFAULT_LEVEL = 6;

    explicit CompressionCodecGDeflate(int level_);

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    String getDescription() const override
    {
        return "GDeflate, a Deflate that a GPU can expand in parallel, in chunks of 64 KiB. Levels 1..12.";
    }

private:
    const int level;
};

}

#endif
