#include <Compression/CompressionCodecZSTD.h>
#include <IO/CompressedStream.h>
#include <IO/ReadHelpers.h>
#include <Compression/CompressionFactory.h>
#include <zstd.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

char CompressionCodecZSTD::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::ZSTD);
}

void CompressionCodecZSTD::getCodecDesc(String & codec_desc)
{
    codec_desc = "ZSTD";
}

size_t CompressionCodecZSTD::getCompressedReserveSize(size_t uncompressed_size)
{
    return ZSTD_compressBound(uncompressed_size);
}

size_t CompressionCodecZSTD::compress(char * source, size_t source_size, char * dest)
{
    size_t compressed_size = ZSTD_compress(dest, ZSTD_compressBound(source_size), source, source_size, level);

    if (ZSTD_isError(compressed_size))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(compressed_size)), ErrorCodes::CANNOT_COMPRESS);

    return compressed_size;
}

size_t CompressionCodecZSTD::decompress(char * source, size_t source_size, char * dest, size_t size_decompressed)
{
    size_t res = ZSTD_decompress(dest, size_decompressed, source, source_size);

    if (ZSTD_isError(res))
        throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_DECOMPRESS);

    return size_decompressed;
}

CompressionCodecZSTD::CompressionCodecZSTD(int level)
    :level(level)
{
}

void registerCodecZSTD(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<char>(CompressionMethodByte::ZSTD);
    factory.registerCompressionCodec("ZSTD", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = 0;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception("ZSTD codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

            const auto children = arguments->children;
            const ASTLiteral * literal = static_cast<const ASTLiteral *>(children[0].get());
            level = literal->value.safeGet<UInt64>();
            if (level > ZSTD_maxCLevel())
                throw Exception("ZSTD codec can't have level more that " + toString(ZSTD_maxCLevel()) + ", given " + toString(level), ErrorCodes::ILLEGAL_CODEC_PARAMETER);
        }

        return std::make_shared<CompressionCodecZSTD>(level);
    });
}

}
