#include <Compression/CompressionCodecNone.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

UInt8 CompressionCodecNone::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::NONE);
}

String CompressionCodecNone::getCodecDesc() const
{
    return "NONE";
}

UInt32 CompressionCodecNone::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    memcpy(dest, source, source_size);
    return source_size;
}

void CompressionCodecNone::doDecompressData(const char * source, UInt32 /*source_size*/, char * dest, UInt32 uncompressed_size) const
{
    memcpy(dest, source, uncompressed_size);
}

void registerCodecNone(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("NONE", static_cast<char>(CompressionMethodByte::NONE), [&](){
        return std::make_shared<CompressionCodecNone>();
    });
}

}
