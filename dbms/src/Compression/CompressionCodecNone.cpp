#include <Common/PODArray.h>
#include <Compression/CompressionCodecFactory.h>
#include <Compression/CompressionCodecNone.h>

namespace DB
{

size_t CompressionCodecNone::getMaxCompressedSize(size_t uncompressed_size) const
{
    return uncompressed_size;
}

size_t CompressionCodecNone::compress(char* source, PODArray<char>& dest,
                                      int inputSize, int)
{
    memcpy(&dest[0], source, inputSize);
    return inputSize;
}

size_t CompressionCodecNone::decompress(char* source, char* dest,
                                        int inputSize, int)
{
    memcpy(dest, source, inputSize);
    return inputSize;
}

size_t CompressionCodecNone::getCompressedSize() const
{
    return 0;
}

size_t CompressionCodecNone::getDecompressedSize() const
{
    return 0;
}

size_t CompressionCodecNone::parseHeader(const char*)
{
    return 0;
}

size_t CompressionCodecNone::writeHeader(char* header) const
{
    *header = bytecode;
    return sizeof(char);
}

void registerCodecNone(CompressionCodecFactory &factory) {
    auto creator = static_cast<CodecPtr(*)()>([] { return CodecPtr(std::make_shared<CompressionCodecNone>()); });

    factory.registerSimpleCodec("None", creator);
    factory.registerCodecBytecode(0x0, creator);
}

}