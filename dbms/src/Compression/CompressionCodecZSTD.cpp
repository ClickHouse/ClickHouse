#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionInfo.h>
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

UInt8 CompressionCodecZSTD::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::ZSTD);
}

String CompressionCodecZSTD::getCodecDesc() const
{
    return "ZSTD(" + toString(level) + ")";
}

UInt32 CompressionCodecZSTD::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return ZSTD_compressBound(uncompressed_size);
}


UInt32 CompressionCodecZSTD::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    size_t compressed_size = ZSTD_compress(dest, ZSTD_compressBound(source_size), source, source_size, level);

    if (ZSTD_isError(compressed_size))
        throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(compressed_size)), ErrorCodes::CANNOT_COMPRESS);

    return compressed_size;
}


void CompressionCodecZSTD::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    size_t res = ZSTD_decompress(dest, uncompressed_size, source, source_size);

    if (ZSTD_isError(res))
        throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_DECOMPRESS);
}

CompressionCodecZSTD::CompressionCodecZSTD(int level_)
    :level(level_)
{
}

void registerCodecZSTD(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::ZSTD);
    factory.registerCompressionCodec("ZSTD", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecZSTD::ZSTD_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception("ZSTD codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            level = literal->value.safeGet<UInt64>();
            if (level > ZSTD_maxCLevel())
                throw Exception("ZSTD codec can't have level more that " + toString(ZSTD_maxCLevel()) + ", given " + toString(level), ErrorCodes::ILLEGAL_CODEC_PARAMETER);
        }

        return std::make_shared<CompressionCodecZSTD>(level);
    });
}

}
