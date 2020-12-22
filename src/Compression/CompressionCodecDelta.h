#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecDelta : public ICompressionCodec
{
public:
    CompressionCodecDelta(UInt8 delta_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + 2; }

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }

private:
    UInt8 delta_bytes_size;
};

}
