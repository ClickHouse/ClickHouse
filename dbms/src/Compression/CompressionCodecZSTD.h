#include <Compression/ICompressionCodec.h>

namespace DB {

class CompressionCodecZSTD : ICompressionCodec
{
private:
public:
};


static CodecPtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<CompressionCodecZSTD>();

    if (arguments->children.size() != 1)
        throw Exception("ZSTD codec can optionally have only one argument - ???", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::String)
        throw Exception("Parameter for ZSTD codec must be string literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<CompressionCodecZSTD>(arg->value.get<String>());
}

void registerCodecZSTD(CompressionCodecFactory & factory)
{
    factory.registerDataType("ZSTD", create);
}

}