#pragma once

#include <Compression/ICompressionCodec.h>


namespace DB
{

class CompressionCodecLZSSE : public ICompressionCodec
{
public:
    CompressionCodecLZSSE(UInt32 type_, UInt32 level_);

    uint8_t getMethodByte() const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    const UInt32 type;
    const UInt32 level;
};

}
