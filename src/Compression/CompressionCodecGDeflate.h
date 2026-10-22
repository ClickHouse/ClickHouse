#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** GDeflate (https://github.com/NVIDIA/libdeflate/tree/gdeflate) is a variant of Deflate whose bit stream is split
  * into 32 interleaved sub-streams, so that a GPU can expand it in parallel, as nvcomp and DirectStorage do, while the
  * CPU compresses and expands it about as fast as Deflate.
  *
  * A block is compressed in chunks of `chunk_bytes`, the last one shorter, each a GDeflate page of its own, which is
  * what a GPU expands in parallel with the others. Of a column of integers, a chunk whose integers lie close together
  * is packed before it is compressed, where that makes it smaller: each integer as its difference from the least of
  * the chunk, in as many bits as the largest difference takes.
  *
  * The block holds the number of chunks, a little-endian 32-bit integer, then a descriptor of each chunk, then the
  * chunks, each from a multiple of 4 bytes, as nvcomp reads them. The descriptor is the size of the compressed chunk, a
  * 32-bit integer; where the highest bit of the number of chunks is set, it is followed by the bits each integer is
  * packed in, 0 for a chunk that is not packed, the size of an integer, 2 bytes to spare and the least integer, as 64
  * bits.
  */
class CompressionCodecGDeflate : public ICompressionCodec
{
public:
    static constexpr size_t chunk_bytes = 64 * 1024;

    /// The levels of libdeflate, from the fastest, 1, to the densest, 12.
    static constexpr int MIN_LEVEL = 1;
    static constexpr int MAX_LEVEL = 12;
    static constexpr int DEFAULT_LEVEL = 6;

    /// `element_size` 0 packs nothing.
    CompressionCodecGDeflate(int level_, UInt8 element_size_, bool is_signed_);

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
        return "GDeflate, a Deflate that a GPU can expand in parallel, in chunks of 64 KiB, with the integers of a chunk packed "
               "where that makes it smaller. Levels 1..12.";
    }

private:
    const int level;
    /// Of the integers whose chunks are packed; not a part of the description, as a block holds how it is packed.
    const UInt8 element_size;
    const bool is_signed;
};

}
