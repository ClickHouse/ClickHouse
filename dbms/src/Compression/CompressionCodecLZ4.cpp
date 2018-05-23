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

size_t CompressionCodecLZ4::writeHeader(char* header)
{
    *header = bytecode;
    *(header + 1) = reinterpret_cast<char>(argument);
    return 2;
}

size_t CompressionCodecLZ4::parseHeader(const char* header)
{
    argument = reinterpret_cast<int8_t>(*header);
    return 1;
}

size_t CompressionCodecLZ4::getMaxCompressedSize(size_t uncompressed_size)
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

size_t CompressionCodecLZ4::compress(const PODArray<char>& source, PODArray<char>& dest,
                                     int inputSize, int maxOutputSize)
{
    auto wrote = LZ4_compress_default(
            source.begin(),
            dest,
            inputSize,
            maxOutputSize
    );
    return wrote;
}

size_t CompressionCodecLZ4::decompress(const PODArray<char>& source, PODArray<char>& dest,
                                       int inputSize, int maxOutputSize)
{
    auto read = LZ4_decompress_fast(source, dest, maxOutputSize);
    if (read < 0)
        throw Exception("Cannot LZ4_decompress_fast", ErrorCodes::CANNOT_DECOMPRESS);
    return read;
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

template<typename T>
CodecPtr createSimple()
{
    return std::make_shared<T>();
}

void registerCodecLZ4(CompressionCodecFactory &factory)
{
    factory.registerCodec("LZ4", create<CompressionCodecLZ4>);
    factory.registerSimpleCodec("LZ4", createSimple<CompressionCodecLZ4>);
    factory.registerCodecBytecode(CompressionCodecLZ4::bytecode, createSimple<CompressionCodecLZ4>);
}

void registerCodecLZ4HC(CompressionCodecFactory &factory)
{
    factory.registerCodec("LZ4HC", create<CompressionCodecLZ4HC>);
    factory.registerSimpleCodec("LZ4HC", createSimple<CompressionCodecLZ4HC>);
    factory.registerCodecBytecode(CompressionCodecLZ4HC::bytecode, createSimple<CompressionCodecLZ4HC>);
}

}