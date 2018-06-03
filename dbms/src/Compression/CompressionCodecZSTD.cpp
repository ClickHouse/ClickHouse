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


size_t CompressionCodecZSTD::writeHeader(char * header)
{
    *header = bytecode;
    return 1;
}

size_t CompressionCodecZSTD::parseHeader(const char *)
{
    return 0;
}

size_t CompressionCodecZSTD::getMaxCompressedSize(size_t uncompressed_size) const
{
    return ZSTD_compressBound(uncompressed_size);
}

size_t CompressionCodecZSTD::compress(char * source, char * dest, size_t input_size, size_t max_output_size)
{
    size_t res = ZSTD_compress(dest, max_output_size, source, input_size, argument);

    if (ZSTD_isError(res))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)),
                        ErrorCodes::CANNOT_COMPRESS);

    return res;
}

size_t CompressionCodecZSTD::decompress(char * source, char * dest, size_t input_size, size_t max_output_size)
{
    size_t res = ZSTD_decompress(dest, max_output_size, source, input_size);

    if (ZSTD_isError(res))
        throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)),
                        ErrorCodes::CANNOT_DECOMPRESS);

    return res;
}

size_t CompressionCodecZSTD::getHeaderSize() const
{
    return 0;
}

static CompressionCodecPtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<CompressionCodecZSTD>();

    if (arguments->children.size() != 1)
        throw Exception("ZSTD codec can optionally have only one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception("Parameter for ZSTD codec must be UInt8 literal", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<CompressionCodecZSTD>(arg->value.get<UInt8>());
}

static CompressionCodecPtr simpleCreate()
{
    return std::make_shared<CompressionCodecZSTD>();
}

void registerCodecZSTD(CompressionCodecFactory &factory)
{
    factory.registerCodec("ZSTD", create);
    factory.registerCodecBytecode(CompressionCodecZSTD::bytecode, simpleCreate);
}

}