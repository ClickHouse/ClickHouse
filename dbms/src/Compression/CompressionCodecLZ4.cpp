#include <Compression/CompressionCodecLZ4.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>

#include <lz4.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>
#include <Compression/CompressionCodecFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_DECOMPRESS;
}

size_t CompressionCodecLZ4::writeHeader(char* header)
{
    *header = bytecode;
    *(header + 1) = static_cast<char>(argument);
    return 2;
}

size_t CompressionCodecLZ4::parseHeader(const char* header)
{
    argument = static_cast<uint8_t>(*header);
    return 1;
}

size_t CompressionCodecLZ4::getMaxCompressedSize(size_t uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

size_t CompressionCodecLZ4::compress(char* source, PODArray<char>& dest,
                                     int inputSize, int maxOutputSize)
{
    auto wrote = LZ4_compress_default(
            &source[0],
            &dest[0],
            inputSize,
            maxOutputSize
    );
    return wrote;
}

size_t CompressionCodecLZ4::decompress(char* source, PODArray<char>& dest,
                                       int inputSize, int maxOutputSize)
{
    auto read = LZ4_decompress_fast(reinterpret_cast<char*>(source), &dest[0], inputSize);
    if (read > maxOutputSize || read < 0)
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
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Parameter for LZ4 codec must be UInt64 literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<T>(static_cast<uint8_t>(arg->value.get<UInt64>()));
}

template<typename T>
static CodecPtr createSimple()
{
    return std::make_shared<T>();
}

void registerCodecLZ4(CompressionCodecFactory &factory)
{
    factory.registerCodec("LZ4", create<CompressionCodecLZ4>);
    factory.registerCodecBytecode(CompressionCodecLZ4::bytecode, createSimple<CompressionCodecLZ4>);
}

void registerCodecLZ4HC(CompressionCodecFactory &factory)
{
    factory.registerCodec("LZ4HC", create<CompressionCodecLZ4HC>);
    factory.registerCodecBytecode(CompressionCodecLZ4HC::bytecode, createSimple<CompressionCodecLZ4HC>);
}

}
