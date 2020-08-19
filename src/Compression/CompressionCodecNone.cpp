#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

uint8_t CompressionCodecNone::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::NONE);
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
    factory.registerSimpleCompressionCodec("NONE", static_cast<char>(CompressionMethodByte::NONE), [&] ()
    {
        return std::make_shared<CompressionCodecNone>();
    });
}

}
