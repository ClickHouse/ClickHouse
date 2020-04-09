#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecDelta : public ICompressionCodec
{
public:
    CompressionCodecDelta(UInt8 delta_bytes_size_);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

    void useInfoAboutType(DataTypePtr data_type) override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + 2; }


private:
    UInt8 delta_bytes_size;
};

}
