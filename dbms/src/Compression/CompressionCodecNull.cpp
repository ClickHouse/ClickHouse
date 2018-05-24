#include <Compression/CompressionCodecFactory.h>
#include "CompressionCodecNull.h"

namespace DB
{

size_t CompressionCodecNone::getMaxCompressedSize(size_t uncompressed_size)
{
    return uncompressed_size;
}

size_t CompressionCodecNone::compress(const PODArray<char>& source, PODArray<char>& dest,
                                      int inputSize, int maxOutputSize)
{
    dest.assign(source);
    return inputSize;
}

size_t CompressionCodecNone::decompress(const PODArray<char>& source, PODArray<char>& dest,
                                        int inputSize, int maxOutputSize)
{
    dest.assign(source);
    return inputSize;
}

void registerCodecNone(CompressionCodecFactory &factory) {
    auto creator = static_cast<CodecPtr(*)()>([] { return CodecPtr(std::make_shared<CompressionCodecNone>()); });

    factory.registerSimpleCodec("None", creator);
    factory.registerCodecBytecode(0x0, creator);
}

}