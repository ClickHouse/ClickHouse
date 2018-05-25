#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecZSTD final : public ICompressionCodec
{
public:
    CompressionCodecZSTD() {}
    CompressionCodecZSTD(uint8_t _argument)
            : argument(_argument)
    {}

    static const uint8_t bytecode = 0x94;
    static const bool is_hc = false;
    uint8_t argument = 1;

    std::string getName() const
    {
        return "ZSTD(" + std::to_string(argument) + ")";
    }

    const char * getFamilyName() const override
    {
        return "ZSTD";
    }

    size_t writeHeader(char* header) override;
    size_t parseHeader(const char* header);
    size_t getHeaderSize() const;

    size_t getCompressedSize() const { return 0; };
    size_t getDecompressedSize() const { return 0; };

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;
    size_t getMaxDecompressedSize(size_t) const { return 0; };

    size_t compress(char* source, PODArray<char>& dest, int inputSize, int maxOutputSize) override;
    size_t decompress(char* source, PODArray<char>& dest, int inputSize, int maxOutputSize) override;

    size_t decompress(char *, char *, int, int)
    {
        return 0;
    }

    ~CompressionCodecZSTD() {}
};

}