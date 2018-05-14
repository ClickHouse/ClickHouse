#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecLZ4 : ICompressionCodec
{
private:
public:
    static constexpr bool is_hc = false;
    std::string getName() const override
    {
        return "LZ4()"
    }

    const char * getFamilyName() const override
    {
        return "LZ4"
    }

    std::vector<uint8_t> getHeader() const override;
    size_t getCompressedSize(size_t uncompressed_size) const override;

    size_t compress(const char* source, char* dest, int inputSize, int maxOutputSize) const override;
    size_t decompress(void* dest, size_t maxOutputSize, const void* source, size_t inputSize) const override;

    bool equals(const ICompressionCodec & rhs) const override;
};

class CompressionCodecLZ4HC : CompressionCodecLZ4
{
public:
    static constexpr bool is_hc = true;

    std::string getName() const override
    {
        return "LZ4HC()"
    }
};

}