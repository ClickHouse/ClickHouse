#include <Common/PODArray.h>
#include <Compression/CompressionCodecFactory.h>

namespace DB {

class CompressionCodecNone final : public ICompressionCodec
{
public:
    CompressionCodecNone() {}

    const uint8_t bytecode = 0x0;

    std::string getName() const
    {
        return "None()";
    }

    const char * getFamilyName() const override
    {
        return "None";
    }

    size_t getHeaderSize() const { return 0; }

    size_t writeHeader(char *) override;
    size_t parseHeader(const char *);

    size_t getCompressedSize() const override;
    size_t getDecompressedSize() const override;

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;
    size_t getMaxDecompressedSize(size_t uncompressed_size) const override;

    size_t compress(char* source, PODArray<char>& dest, int inputSize, int maxOutputSize) override;
    size_t decompress(char* source, char* dest, int inputSize, int maxOutputSize) override;

    size_t decompress(char*, PODArray<char>&, int, int) override
    {
        return 0;
    }

    ~CompressionCodecNone() {}
};

}