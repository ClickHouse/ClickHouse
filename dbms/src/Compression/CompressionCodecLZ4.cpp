#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/ICompressionCodec.h>

#include <Compression/CompressionCodecLZ4.h>
#include <Compression/CompressionCodecFactory.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


template<typename T>
static CodecPtr create(const ASTPtr &arguments)
{
    if (!arguments)
        return std::make_shared<T>();

    if (arguments->children.size() != 1)
        throw Exception("LZ4 codec can optionally have only one argument - ???",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral *arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::String)
        throw Exception("Parameter for LZ4 codec must be string literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<T>(arg->value.get<String>());
}

void registerCodecLZ4(CompressionCodecFactory &factory)
{
    factory.registerDataType("LZ4", create<CompressionCodecLZ4>);
}

void registerCodecLZ4HC(CompressionCodecFactory &factory)
{
    factory.registerDataType("LZ4HC", create<CompressionCodecLZ4HC>);
}

}