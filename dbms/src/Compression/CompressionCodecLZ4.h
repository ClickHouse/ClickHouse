#pragma once

#include <IO/WriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Parsers/StringRange.h>
#include <IO/LZ4_decompress_faster.h>

namespace DB
{

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    char getMethodByte() override;

    void getCodecDesc(String & codec_desc) override;

    size_t compress(char * source, size_t source_size, char * dest) override;

    size_t getCompressedReserveSize(size_t uncompressed_size) override;

    size_t decompress(char * source, size_t source_size, char * dest, size_t decompressed_size) override;

private:
    LZ4::PerformanceStatistics lz4_stat;
};

}