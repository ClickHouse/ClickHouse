#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class CompressionCodecNone final : public ICompressionCodec
{
public:
    CompressionCodecNone();

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    bool isNone() const override { return true; }

    std::string getDescription() const override
    {
        return "No compression at all. Can be used on the columns that can not be compressed anyway.";
    }

};

}
