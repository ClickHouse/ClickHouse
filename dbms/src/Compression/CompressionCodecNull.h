#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecNone : ICompressionCodec
{
public:
    const uint8_t bytecode = 0x0;

    std::string getName() const override
    {
        return "None()";
    }

    const char * getFamilyName() const override
    {
        return "None";
    }

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;

    size_t compress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const override;
    size_t decompress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const override;
};

}