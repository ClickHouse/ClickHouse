
namespace DB {

class CompressionCodecNone : ICompressionCodec
{
private:
public:
};


void registerCodecNone(CompressionCodecFactory & factory)
{
    auto creator = static_cast<CodecPtr(*)()>([] { return CodecPtr(std::make_shared<CompressionCodecNone>()); });

    factory.registerSimpleDataType("None", creator);
}

}