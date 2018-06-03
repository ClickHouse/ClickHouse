#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecZSTD final : public ICompressionCodec
{
public:
    CompressionCodecZSTD() {}
    CompressionCodecZSTD(uint8_t argument_)
            : argument(argument_)
    {}

    static const uint8_t bytecode = 0x90;
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

    size_t writeHeader(char * header) override;
    size_t parseHeader(const char * header);
    size_t getHeaderSize() const;

    size_t getCompressedSize() const { return 0; };
    size_t getDecompressedSize() const { return 0; };

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;

    size_t compress(char * source, char * dest, size_t input_size, size_t max_output_size) override;
    size_t decompress(char * source, char * dest, size_t inupt_size, size_t max_output_size) override;

    ~CompressionCodecZSTD() {}
};

}