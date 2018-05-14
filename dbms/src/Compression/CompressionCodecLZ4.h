#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecLZ4 : ICompressionCodec
{
private:
public:
    static constexpr bool is_hc = false;

};

class CompressionCodecLZ4HC : CompressionCodecLZ4
{
public:
    static constexpr bool is_hc = true;
};

template < typename T >
static CodecPtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<T>();

    if (arguments->children.size() != 1)
        throw Exception("LZ4 codec can optionally have only one argument - ???", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::String)
        throw Exception("Parameter for LZ4 codec must be string literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<T>(arg->value.get<String>());
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerDataType("LZ4", create<CompressionCodecLZ4>);
}

void registerCodecLZ4HC(CompressionCodecFactory & factory)
{
    factory.registerDataType("LZ4HC", create<CompressionCodecLZ4HC>);
}

}