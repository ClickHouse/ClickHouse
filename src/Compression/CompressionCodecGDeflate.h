#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecGDeflate : public ICompressionCodec
{
public:
    static constexpr size_t chunk_bytes = 64 * 1024;

    static constexpr int MIN_LEVEL = 1;
    static constexpr int MAX_LEVEL = 12;
    static constexpr int DEFAULT_LEVEL = 6;

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
