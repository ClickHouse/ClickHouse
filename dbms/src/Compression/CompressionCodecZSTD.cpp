#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionCodecFactory.h>

#include <zstd.h>

#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int CANNOT_DECOMPRESS;
extern const int CANNOT_COMPRESS;
}


size_t CompressionCodecZSTD::writeHeader(char* header) const
{
    *header = bytecode;
    *(header + 1) = static_cast<char>(argument);
    return 2;
}

size_t CompressionCodecZSTD::parseHeader(const char* header)
{
    argument = static_cast<int8_t>(*header);
    return 1;
}

size_t CompressionCodecZSTD::getMaxCompressedSize(size_t uncompressed_size) const
{
    return ZSTD_compressBound(uncompressed_size);
}

size_t CompressionCodecZSTD::compress(char* source, PODArray<char>& dest,
                                      int inputSize, int maxOutputSize)
{
    size_t res = ZSTD_compress(&dest[0], maxOutputSize,
                               source, inputSize,
                               argument);

    if (ZSTD_isError(res))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

    return res;
}

size_t CompressionCodecZSTD::decompress(char* source, PODArray<char>& dest,
                                        int inputSize, int maxOutputSize)
{
    size_t res = ZSTD_decompress(&dest[0], maxOutputSize, &source[0], inputSize);

    if (ZSTD_isError(res))
        throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_DECOMPRESS);

    return res;
}

size_t CompressionCodecZSTD::getHeaderSize() const
{
    return sizeof(argument);
}

static CodecPtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<CompressionCodecZSTD>();

    if (arguments->children.size() != 1)
        throw Exception("ZSTD codec can optionally have only one argument - ???", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Parameter for ZSTD codec must be UInt64 literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<CompressionCodecZSTD>(static_cast<uint8_t>(arg->value.get<UInt64>()));
}

void registerCodecZSTD(CompressionCodecFactory &factory)
{
    factory.registerCodec("ZSTD", create);
    factory.registerCodecBytecode(CompressionCodecZSTD::bytecode, static_cast<CodecPtr(*)()>([] { return CodecPtr(std::make_shared<CompressionCodecZSTD>()); }));
}

}