#include <Compression/CompressionCodecLZSSE8.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTLiteral.h>
#include <lzsse8/lzsse8.h>
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

CompressionCodecLZSSE8::CompressionCodecLZSSE8(int level_) : level(level_)
{
    setCodecDescription("LZSSE8", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecLZSSE8::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::LZSSE8);
}

void CompressionCodecLZSSE8::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecLZSSE8::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size;
}

UInt32 CompressionCodecLZSSE8::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt32 res;
    LZSSE8_OptimalParseState * state = LZSSE8_MakeOptimalParseState(source_size);
    res = LZSSE8_CompressOptimalParse(state, source, source_size, dest, source_size, level);
    LZSSE8_FreeOptimalParseState(state);

    if (res == 0)
        throw Exception("Cannot compress block with LZSSE", ErrorCodes::CANNOT_COMPRESS);

    return res;
}

void CompressionCodecLZSSE8::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    UInt32 res;
    res = LZSSE8_Decompress(source, source_size, dest, uncompressed_size);

    if (res < uncompressed_size)
        throw Exception("Cannot decompress block with LZSSE", ErrorCodes::CANNOT_DECOMPRESS);
}

void registerCodecLZSSE8(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::LZSSE8);
    factory.registerCompressionCodec(
        "LZSSE8",
        method_code,
        [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            int level = 1;
            if (arguments && !arguments->children.empty())
            {
                if (arguments->children.size() != 1)
                    throw Exception(
                        "LZSSE8 codec must have 1 parameter, given " + std::to_string(arguments->children.size()),
                        ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const auto children = arguments->children;
                const auto * level_literal = children[0]->as<ASTLiteral>();
                if (!level_literal)
                    throw Exception("LZSSE8 first codec argument must be integer", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

                level = level_literal->value.safeGet<UInt64>();

            }

            return std::make_shared<CompressionCodecLZSSE8>(level);
        });
}

}
