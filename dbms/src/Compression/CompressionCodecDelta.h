#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>
#include <DataTypes/DataTypeNumberBase.h>

namespace DB
{

template<typename WidthType>
class CompressionCodecDelta : public ICompressionCodec
{
public:
    CompressionCodecDelta(const DataTypePtr & delta_type);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    const DataTypePtr delta_type;
};

}
