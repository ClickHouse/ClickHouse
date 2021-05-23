#pragma once

#include <Compression/ICompressionCodec.h>


namespace DB
{
class CompressionCodecLizard : public ICompressionCodec
{
public:
    static constexpr auto LIZARD_DEFAULT_LEVEL = 1;

    CompressionCodecLizard(int level_);

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

private:
    const int level;
};

}
