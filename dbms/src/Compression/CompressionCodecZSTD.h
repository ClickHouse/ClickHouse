#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>

namespace DB
{

class CompressionCodecZSTD : public ICompressionCodec
{
public:
    static constexpr auto ZSTD_DEFAULT_LEVEL = 1;

    CompressionCodecZSTD(int level_);

    UInt8 getMethodByte() const override;

    String getCodecDesc() const override;

    UInt32 getCompressedDataSize(UInt32 uncompressed_size) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    int level;
};

}
