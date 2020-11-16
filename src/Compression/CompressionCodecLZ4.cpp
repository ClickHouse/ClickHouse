#include "CompressionCodecLZ4.h"

#include <lz4.h>
#include <lz4hc.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/WriteHelpers.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecLZ4::CompressionCodecLZ4()
{
    setCodecDescription("LZ4");
}

uint8_t CompressionCodecLZ4::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::LZ4);
}

void CompressionCodecLZ4::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecLZ4::getMaxCompressedDataSize(UInt32 uncompressed_size) const
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
    factory.registerSimpleCompressionCodec("LZ4", static_cast<UInt8>(CompressionMethodByte::LZ4), [&] ()
    {
        return std::make_shared<CompressionCodecLZ4>();
    });
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
        int level = 0;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception("LZ4HC codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception("LZ4HC codec argument must be integer", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

            level = literal->value.safeGet<UInt64>();
        }

        return std::make_shared<CompressionCodecLZ4HC>(level);
    });
}

CompressionCodecLZ4HC::CompressionCodecLZ4HC(int level_)
    : level(level_)
{
    setCodecDescription("LZ4HC", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

}
