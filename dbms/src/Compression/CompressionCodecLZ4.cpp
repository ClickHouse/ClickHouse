#include <Compression/CompressionCodecLZ4.h>
#include <lz4.h>
#include <lz4hc.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>
#include <IO/LZ4_decompress_faster.h>
#include "CompressionCodecLZ4.h"


namespace DB
{

char CompressionCodecLZ4::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::LZ4);
}

void CompressionCodecLZ4::getCodecDesc(String & codec_desc)
{
    codec_desc = "LZ4";
}

size_t CompressionCodecLZ4::getCompressedReserveSize(size_t uncompressed_size)
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

size_t CompressionCodecLZ4::compress(char * source, size_t source_size, char * dest)
{
    return LZ4_compress_default(source, dest, source_size, LZ4_COMPRESSBOUND(source_size));
}

size_t CompressionCodecLZ4::decompress(char * source, size_t source_size, char * dest, size_t size_decompressed)
{
    LZ4::decompress(source, dest, source_size, size_decompressed, lz4_stat);
    return size_decompressed;
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("LZ4", static_cast<char>(CompressionMethodByte::LZ4), [&](){
        return std::make_shared<CompressionCodecLZ4>();
    });
}

}
