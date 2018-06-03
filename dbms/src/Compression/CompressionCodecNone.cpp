#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionCodecFactory.h>

namespace DB
{

size_t CompressionCodecNone::getMaxCompressedSize(size_t uncompressed_size) const { return uncompressed_size; }

size_t CompressionCodecNone::compress(char * source, char * dest, size_t input_size, size_t)
{
    memcpy(dest, source, input_size);
    return input_size;
}

size_t CompressionCodecNone::decompress(char * source, char * dest, size_t input_size, size_t)
{
    memcpy(dest, source, input_size);
    return input_size;
}

size_t CompressionCodecNone::getCompressedSize() const { return 0; }

size_t CompressionCodecNone::getDecompressedSize() const { return 0; }

size_t CompressionCodecNone::parseHeader(const char *) { return 0; }

size_t CompressionCodecNone::writeHeader(char * header)
{
    *header = bytecode;
    return 1;
}

void registerCodecNone(CompressionCodecFactory & factory)
{
    auto creator = static_cast<CompressionCodecPtr(*)()>([] { return CompressionCodecPtr(std::make_shared<CompressionCodecNone>()); });

    factory.registerSimpleCodec("None", creator);
    factory.registerCodecBytecode(0x0, creator);
}

}