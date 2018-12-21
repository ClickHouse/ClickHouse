#include <Compression/CompressionCodecLZ4.h>
#include <lz4.h>
#include <lz4hc.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <IO/LZ4_decompress_faster.h>
#include "CompressionCodecLZ4.h"


namespace DB
{

UInt8 CompressionCodecLZ4::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::LZ4);
}

String CompressionCodecLZ4::getCodecDesc() const
{
    return "LZ4";
}

UInt32 CompressionCodecLZ4::getCompressedDataSize(UInt32 uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

UInt32 CompressionCodecLZ4::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return LZ4_compress_default(source, dest, source_size, LZ4_COMPRESSBOUND(source_size));
}

void CompressionCodecLZ4::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    LZ4::decompress(source, dest, source_size, uncompressed_size, lz4_stat);
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("LZ4", static_cast<UInt8>(CompressionMethodByte::LZ4), [&](){
        return std::make_shared<CompressionCodecLZ4>();
    });
}

}
