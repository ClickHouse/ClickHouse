#pragma once

#include <Compression/ICompressionCodec.h>
#include <src/density_api.h>


namespace DB
{
class CompressionCodecDensity : public ICompressionCodec
{
public:
    static constexpr auto DENSITY_DEFAULT_ALGO = DENSITY_ALGORITHM_CHAMELEON; // by default aim on speed

    CompressionCodecDensity(DENSITY_ALGORITHM algo_);

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
    const DENSITY_ALGORITHM algo;
};

}
