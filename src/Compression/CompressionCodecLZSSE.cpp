#include <Compression/CompressionCodecLZSSE.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTLiteral.h>

#include <lzsse2/lzsse2.h>
#include <lzsse4/lzsse4.h>
#include <lzsse8/lzsse8.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int LOGICAL_ERROR;
}

CompressionCodecLZSSE::CompressionCodecLZSSE(UInt32 type_, UInt32 level_) : type(type_), level(level_)
{
    if (type != 2 && type != 4 && type != 8)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no LZSSE{} codec", type);

    setCodecDescription(fmt::format("LZSSE{}", type), {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecLZSSE::getMethodByte() const
{
    switch (type)
    {
        case 2: return static_cast<uint8_t>(CompressionMethodByte::LZSSE2);
        case 4: return static_cast<uint8_t>(CompressionMethodByte::LZSSE4);
        case 8: return static_cast<uint8_t>(CompressionMethodByte::LZSSE8);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no LZSSE{} codec", type);
    }
}

void CompressionCodecLZSSE::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecLZSSE::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size;
}

UInt32 CompressionCodecLZSSE::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt32 res = 0;
    switch (type)
    {
        case 2:
        {
            LZSSE2_OptimalParseState * state = LZSSE2_MakeOptimalParseState(source_size);
            res = LZSSE2_CompressOptimalParse(state, source, source_size, dest, source_size, level);
            LZSSE2_FreeOptimalParseState(state);
            break;
        }
        case 4:
        {
            LZSSE2_OptimalParseState * state = LZSSE2_MakeOptimalParseState(source_size);
            res = LZSSE2_CompressOptimalParse(state, source, source_size, dest, source_size, level);
            LZSSE2_FreeOptimalParseState(state);
            break;
        }
        case 8:
        {
            LZSSE2_OptimalParseState * state = LZSSE2_MakeOptimalParseState(source_size);
            res = LZSSE2_CompressOptimalParse(state, source, source_size, dest, source_size, level);
            LZSSE2_FreeOptimalParseState(state);
            break;
        }
        default:
            break;
    }

    if (res == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress block with LZSSE{}", type);
    return res;
}

void CompressionCodecLZSSE::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    UInt32 res = LZSSE2_Decompress(source, source_size, dest, uncompressed_size);
    if (res < uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress block with LZSSE{}", type);
}

void registerCodecsLZSSE(CompressionCodecFactory & factory)
{
    for (auto [type, method_byte] : std::initializer_list<std::tuple<int, CompressionMethodByte>>
    {
        {2, CompressionMethodByte::LZSSE2},
        {4, CompressionMethodByte::LZSSE4},
        {8, CompressionMethodByte::LZSSE8}
    })
    {
        factory.registerCompressionCodec(
            fmt::format("LZSSE{}", type),
            uint8_t(method_byte),
            [type = type](const ASTPtr & arguments) -> CompressionCodecPtr
            {
                int level = 1;
                if (arguments && !arguments->children.empty())
                {
                    if (arguments->children.size() != 1)
                        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                            "LZSSE{} codec must have 1 parameter, {} given", type, arguments->children.size());

                    const auto children = arguments->children;
                    const auto * level_literal = children[0]->as<ASTLiteral>();
                    if (!level_literal)
                        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                            "LZSSE{} first codec argument must be integer", type);

                    level = level_literal->value.safeGet<UInt64>();
                }

                return std::make_shared<CompressionCodecLZSSE>(type, level);
            });
    }
}

}
