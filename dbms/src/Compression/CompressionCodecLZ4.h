#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecLZ4 : ICompressionCodec
{
public:
    static const uint8_t bytecode = 0x84;
    static const bool is_hc = false;
    int8_t argument = 1;

    std::string getName() const override
    {
        return "LZ4()";
    }

    const char * getFamilyName() const override
    {
        return "LZ4";
    }

    size_t writeHeader(char* header) const override;
    size_t parseHeader(const char* header) const override;

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;

    size_t compress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const override;
    size_t decompress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const override;
};

class CompressionCodecLZ4HC : CompressionCodecLZ4
{
public:
    static const uint8_t bytecode = 0x82;
    static const bool is_hc = true;

    std::string getName() const override
    {
        return "LZ4HC()";
    }
};

}