#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecZSTD : public ICompressionCodec
{
public:
    static constexpr auto ZSTD_DEFAULT_LEVEL = 1;
    static constexpr auto ZSTD_DEFAULT_LOG_WINDOW = 24;

    explicit CompressionCodecZSTD(int level_);
    CompressionCodecZSTD(int level_, int window_log);

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    std::string getDescription() const override
    {
        return "Good compression; pretty fast; best for high compression needs. Don’t use levels higher than 3.";
    }


private:
    const int level;
    const bool enable_long_range;
    const int window_log;
};

}
