#include <Compression/CompressionCodecLZSSE4.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTLiteral.h>
#include <lzsse4/lzsse4.h>
#include <Common/ErrorCodes.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecLZSSE4::CompressionCodecLZSSE4(int level_) : level(level_)
{
    setCodecDescription("LZSSE4", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecLZSSE4::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::LZSSE4);
}

void CompressionCodecLZSSE4::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecLZSSE4::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size;
}

UInt32 CompressionCodecLZSSE4::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt32 res;
    LZSSE4_OptimalParseState * state = LZSSE4_MakeOptimalParseState(source_size);
    res = LZSSE4_CompressOptimalParse(state, source, source_size, dest, source_size, level);
    LZSSE4_FreeOptimalParseState(state);

    if (res == 0)
        throw Exception("Cannot compress block with LZSSE4", ErrorCodes::CANNOT_COMPRESS);

    return res;
}

void CompressionCodecLZSSE4::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    UInt32 res;
    res = LZSSE4_Decompress(source, source_size, dest, uncompressed_size);

    if (res < uncompressed_size)
        throw Exception("Cannot decompress block with LZSSE4", ErrorCodes::CANNOT_DECOMPRESS);
}

void registerCodecLZSSE4(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::LZSSE4);
    factory.registerCompressionCodec(
        "LZSSE4",
        method_code,
        [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            int level = 1;
            if (arguments && !arguments->children.empty())
            {
                if (arguments->children.size() != 1)
                    throw Exception(
                        "LZSSE4 codec must have 1 parameter, given " + std::to_string(arguments->children.size()),
                        ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const auto children = arguments->children;
                const auto * level_literal = children[0]->as<ASTLiteral>();
                if (!level_literal)
                    throw Exception("LZSSE4 first codec argument must be integer", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

                level = level_literal->value.safeGet<UInt64>();

            }

            return std::make_shared<CompressionCodecLZSSE4>(level);
        });
}

}
