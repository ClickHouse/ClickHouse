#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    CompressionCodecMultiple() = default;
    explicit CompressionCodecMultiple(Codecs codecs);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void useInfoAboutType(DataTypePtr data_type) override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    Codecs codecs;

};

}
