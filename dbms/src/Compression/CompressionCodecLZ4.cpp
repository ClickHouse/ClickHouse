#include <Compression/CompressionCodecLZ4.h>
#include <lz4.h>
#include <lz4hc.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <IO/LZ4_decompress_faster.h>
#include "CompressionCodecLZ4.h"
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>



namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
}


UInt8 CompressionCodecLZ4::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::LZ4);
}

String CompressionCodecLZ4::getCodecDesc() const
{
    return "LZ4";
}

UInt32 CompressionCodecLZ4::getCompressedDataSize(UInt32 uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

UInt32 CompressionCodecLZ4::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return LZ4_compress_default(source, dest, source_size, LZ4_COMPRESSBOUND(source_size));
}

void CompressionCodecLZ4::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    LZ4::decompress(source, dest, source_size, uncompressed_size, lz4_stat);
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("LZ4", static_cast<UInt8>(CompressionMethodByte::LZ4), [&] () {
        return std::make_shared<CompressionCodecLZ4>();
    });
}


String CompressionCodecLZ4HC::getCodecDesc() const
{
    return "LZ4HC";
}

UInt32 CompressionCodecLZ4HC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    auto success = LZ4_compress_HC(source, dest, source_size, LZ4_COMPRESSBOUND(source_size), level);

    if (!success)
        throw Exception("Cannot LZ4_compress_HC", ErrorCodes::CANNOT_COMPRESS);

    return success;
}

void registerCodecLZ4HC(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodec("LZ4HC", {}, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = LZ4HC_CLEVEL_DEFAULT;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception("LZ4HC codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

            const auto children = arguments->children;
            const ASTLiteral * literal = static_cast<const ASTLiteral *>(children[0].get());
            level = literal->value.safeGet<UInt64>();
            if (level > LZ4HC_CLEVEL_MAX || level < LZ4HC_CLEVEL_MIN)
                throw Exception("LZ4HC codec can't have level more than "
                    + std::to_string(LZ4HC_CLEVEL_MAX) + " and less than "
                    + std::to_string(LZ4HC_CLEVEL_MIN) + ", given "
                    + std::to_string(level), ErrorCodes::ILLEGAL_CODEC_PARAMETER);
        }

        return std::make_shared<CompressionCodecLZ4HC>(level);
    });
}

CompressionCodecLZ4HC::CompressionCodecLZ4HC(int level_)
    : level(level_)
{
}

}

