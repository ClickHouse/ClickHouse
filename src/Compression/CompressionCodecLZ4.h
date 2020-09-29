#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    CompressionCodecLZ4();

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    mutable LZ4::PerformanceStatistics lz4_stat;
    ASTPtr codec_desc;
};


class CompressionCodecLZ4HC : public CompressionCodecLZ4
{
public:
    CompressionCodecLZ4HC(int level_);

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

private:
    const int level;
};

}
