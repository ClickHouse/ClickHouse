#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    static const uint8_t bytecode = 0x82;
    uint8_t argument = 1;

    CompressionCodecLZ4() {}
    CompressionCodecLZ4(uint8_t argument_)
    : argument(argument_)
    {}

    std::string getName() const
    {
        return "LZ4(" + std::to_string(argument) + ")";
    }

    const char * getFamilyName() const override
    {
        return "LZ4";
    }

    size_t writeHeader(char * header) override;
    size_t parseHeader(const char * header);

    size_t getHeaderSize() const { return 0; };

    size_t getCompressedSize() const { return 0; };
    size_t getDecompressedSize() const { return 0; };

    size_t getMaxCompressedSize(size_t uncompressed_size) const override;

    size_t compress(char *source, char *dest, size_t input_size, size_t max_output_size) override;
    size_t decompress(char *, char *, size_t, size_t) override;

    ~CompressionCodecLZ4() {}
};

class CompressionCodecLZ4HC final : public CompressionCodecLZ4
{
public:
    static const uint8_t bytecode = 0x82;
    uint8_t argument = 1;

    CompressionCodecLZ4HC() {}
    CompressionCodecLZ4HC(uint8_t _argument)
    : argument(_argument)
    {}

    std::string getName() const
    {
        return "LZ4HC(" + std::to_string(argument) + ")";
    }
    size_t compress(char *source, char *dest, size_t input_size, size_t max_output_size) override;

    ~CompressionCodecLZ4HC() {}
};

}