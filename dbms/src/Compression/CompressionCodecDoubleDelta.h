#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecDoubleDelta : public ICompressionCodec
{
public:
    CompressionCodecDoubleDelta(UInt8 data_bytes_size_);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

    void useInfoAboutType(DataTypePtr data_type) override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

private:
    UInt8 data_bytes_size;
};

}
