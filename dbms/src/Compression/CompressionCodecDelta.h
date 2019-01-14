#pragma once

#include <Compression/ICompressionCodec.h>
namespace DB
{

class CompressionCodecDelta : public ICompressionCodec
{
public:
    static constexpr auto DEFAULT_BYTES_SIZE = 1;

    CompressionCodecDelta(UInt8 delta_bytes_size_);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + 2; }
private:
    const UInt8 delta_bytes_size;
};
}

