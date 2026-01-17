#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecOpenZL : public ICompressionCodec
{
public:
    /// Profile: "le-u64" (little-endian unsigned 64-bit), "le-f64" (float64), etc.
    explicit CompressionCodecOpenZL(const String & profile_ = "generic");

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

    String getDescription() const override
    {
        return "Format-aware compression by Meta. Best for structured columnar data. Experimental.";
    }

private:
    const String profile;
};

}
