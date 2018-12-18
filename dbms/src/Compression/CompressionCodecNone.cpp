#include <Compression/CompressionCodecNone.h>
#include <IO/CompressedStream.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

char CompressionCodecNone::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::NONE);
}

void CompressionCodecNone::getCodecDesc(String & codec_desc)
{
    codec_desc = "NONE";
}

size_t CompressionCodecNone::compress(char * source, size_t source_size, char * dest)
{
    memcpy(dest, source, source_size);
    return source_size;
}

size_t CompressionCodecNone::decompress(char * source, size_t /*source_size*/, char * dest, size_t size_decompressed)
{
    memcpy(dest, source, size_decompressed);
    return size_decompressed;
}

void registerCodecNone(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("NONE", static_cast<char>(CompressionMethodByte::NONE), [&](){
        return std::make_shared<CompressionCodecNone>();
    });
}

}
