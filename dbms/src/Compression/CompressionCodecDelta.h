#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionCodecFactory.h>

namespace DB
{

class CompressionCodecDelta final : public ICompressionCodec
{
private:
    uint16_t element_size = 1;
    /// 0 - bytes, 1 - int, 2 - uint, 3 - float
    uint8_t delta_type = 100;
public:
    static const uint8_t bytecode = 0x40;

    CompressionCodecDelta() {}
    CompressionCodecDelta(uint16_t element_size_, uint8_t delta_type_)
        : element_size(element_size_)
        , delta_type(delta_type_)
    {}

    std::string getName() const
    {
        return "Delta(" + std::to_string(element_size) + ", " + std::to_string(delta_type) + ")";
    }

    const char * getFamilyName() const override
    {
        return "Delta";
    }

    size_t writeHeader(char * header) override;
    size_t parseHeader(const char * header);

    size_t getHeaderSize() const { return sizeof(delta_type) + sizeof(element_size); };

    size_t getCompressedSize() const { return 0; };
    size_t getDecompressedSize() const { return 0; };

    /// Does not change size, only values
    size_t getMaxCompressedSize(size_t uncompressed_size) const { return uncompressed_size; };

    template <typename T>
    void compress_num(char * source, char * dest, size_t size) const;
    template <typename T>
    void decompress_num(char * source, char * dest, size_t size) const;

    void compress_bytes(char * source, char * dest, size_t size) const;
    void decompress_bytes(char * source, char * dest, size_t size) const;

    size_t compress(char * source, char * dest, size_t input_size, size_t max_output_size) override;
    size_t decompress(char *, char *, size_t, size_t) override;

    void setDataType(DataTypePtr data_type);

    ~CompressionCodecDelta() {}

};

}