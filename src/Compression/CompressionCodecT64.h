#pragma once

#include <Core/Types.h>
#include <Compression/ICompressionCodec.h>


namespace DB
{

/// Get 64 integer valuses, makes 64x64 bit matrix, transpose it and crop unused bits (most significant zeroes).
/// In example, if we have UInt8 with only 0 and 1 inside 64xUInt8 would be compressed into 1xUInt64.
/// It detects unused bits by calculating min and max values of data part, saving them in header in compression phase.
/// There's a special case with signed integers parts with crossing zero data. Here it stores one more bit to detect sign of value.
class CompressionCodecT64 : public ICompressionCodec
{
public:
    static constexpr UInt32 HEADER_SIZE = 1 + 2 * sizeof(UInt64);
    static constexpr UInt32 MAX_COMPRESSED_BLOCK_SIZE = sizeof(UInt64) * 64;

    /// There're 2 compression variants:
    /// Byte - transpose bit matrix by bytes (only the last not full byte is transposed by bits). It's default.
    /// Bits - full bit-transpose of the bit matrix. It uses more resources and leads to better compression with ZSTD (but worse with LZ4).
    enum class Variant
    {
        Byte,
        Bit
    };

    CompressionCodecT64(TypeIndex type_idx_, Variant variant_)
        : type_idx(type_idx_)
        , variant(variant_)
    {}

    uint8_t getMethodByte() const override;
    String getCodecDesc() const override
    {
        return variant == Variant::Byte ? "T64" : "T64('bit')";
    }

protected:
    UInt32 doCompressData(const char * src, UInt32 src_size, char * dst) const override;
    void doDecompressData(const char * src, UInt32 src_size, char * dst, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        /// uncompressed_size - (uncompressed_size % (sizeof(T) * 64)) + sizeof(UInt64) * sizeof(T) + header_size
        return uncompressed_size + MAX_COMPRESSED_BLOCK_SIZE + HEADER_SIZE;
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

private:
    TypeIndex type_idx;
    Variant variant;
};

}
