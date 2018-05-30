#include <Compression/CompressionCodecLZ4.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>

#include <lz4.h>
#include <lz4hc.h>
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
    extern const int CANNOT_COMPRESS;
}

size_t CompressionCodecLZ4::writeHeader(char* header)
{
    *header = bytecode;
    return 1;
}

size_t CompressionCodecLZ4::parseHeader(const char*)
{
    return 0;
}

size_t CompressionCodecLZ4::getMaxCompressedSize(size_t uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

size_t CompressionCodecLZ4::compress(char *source, char *dest,
                                     size_t inputSize, size_t maxOutputSize)
{
    auto wrote = LZ4_compress_default(
            source,
            dest,
            inputSize,
            maxOutputSize
    );
    if (!wrote)
        throw Exception("Cannot LZ4_compress_default", ErrorCodes::CANNOT_COMPRESS);
    return wrote;
}

size_t CompressionCodecLZ4::decompress(char *source, char *dest,
                                       size_t inputSize, size_t maxOutputSize)
{
    auto read = LZ4_decompress_fast(source, dest, maxOutputSize);
    if (read > static_cast<int64_t>(inputSize) || read < 0)
        throw Exception("Cannot LZ4_decompress_fast", ErrorCodes::CANNOT_DECOMPRESS);
    return read;
}

size_t CompressionCodecLZ4HC::compress(char* source, char* dest, size_t inputSize, size_t maxOutputSize)
{
    auto wrote = LZ4_compress_HC(
            source,
            dest,
            inputSize,
            maxOutputSize,
            argument
    );

    if (!wrote)
        throw Exception("Cannot LZ4_compress_HC", ErrorCodes::CANNOT_COMPRESS);
    return wrote;
}

template<typename T>
static CompressionCodecPtr create(const ASTPtr &arguments)
{
    if (!arguments)
        return std::make_shared<T>();

    if (arguments->children.size() != 1)
        throw Exception("LZ4 codec can optionally have only one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral *arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Parameter for LZ4 codec must be UInt64 literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<T>(static_cast<uint8_t>(arg->value.get<UInt64>()));
}

template<typename T>
static CompressionCodecPtr createSimple()
{
    return std::make_shared<T>();
}

void registerCodecLZ4(CompressionCodecFactory &factory)
{
    factory.registerCodec("LZ4", create<CompressionCodecLZ4>);
    factory.registerCodecBytecode(CompressionCodecLZ4::bytecode, createSimple<CompressionCodecLZ4>);
    factory.registerCodec("LZ4HC", create<CompressionCodecLZ4HC>);
}

}
