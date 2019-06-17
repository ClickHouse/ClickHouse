#pragma once

#include <Core/Types.h>
#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecT64 : public ICompressionCodec
{
public:
    static constexpr UInt32 HEADER_SIZE = 1 + 2 * sizeof(UInt64);
    static constexpr UInt32 MAX_COMPRESSED_BLOCK_SIZE = sizeof(UInt64) * 64;

    CompressionCodecT64(TypeIndex type_idx_)
        : type_idx(type_idx_)
    {}

    UInt8 getMethodByte() const override;
    String getCodecDesc() const override { return "T64"; }

    void useInfoAboutType(DataTypePtr data_type) override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        /// uncompressed_size - (uncompressed_size % (sizeof(T) * 64)) + sizeof(UInt64) * sizeof(T) + header_size
        return uncompressed_size + MAX_COMPRESSED_BLOCK_SIZE + HEADER_SIZE;
    }

private:
    TypeIndex type_idx;
};

}
