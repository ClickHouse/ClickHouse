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

    explicit CompressionCodecGDeflate(int level_);

    [[nodiscard]] uint8_t getMethodByte() const override;
    [[nodiscard]] ASTPtr getCodecDescription() const override;

    [[nodiscard]] UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    [[nodiscard]] bool isCompression() const override { return true; }
    [[nodiscard]] bool isGenericCompression() const override { return true; }

    [[nodiscard]] String getDescription() const override
    {
        return "GDeflate, a Deflate that a GPU can expand in parallel, in chunks of 64 KiB. Levels 1..12.";
    }

private:
    const int level;
};

}
