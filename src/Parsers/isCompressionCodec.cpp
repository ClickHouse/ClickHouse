#include <Parsers/isCompressionCodec.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/StringUtils.h>

#include <array>
#include <string_view>


namespace DB
{

namespace
{

/// Registered codec families, including the ones that depend on the build options, and the alias `Default`.
constexpr std::array<std::string_view, 21> codec_family_names
{
    "Default", "NONE", "Multiple",
    "LZ4", "LZ4HC", "ZSTD", "ZSTD_QAT", "DEFLATE_QPL", "ZXC",
    "Delta", "DoubleDelta", "Gorilla", "FPC", "T64", "GCD", "ALP", "PCO", "SZ3", "Quantized",
    "AES_128_GCM_SIV", "AES_256_GCM_SIV",
};

bool isCodecFamilyName(const String & name)
{
    for (const auto & family_name : codec_family_names)
        if (equalsCaseInsensitive(name, family_name))
            return true;
    return false;
}

}

bool isCompressionCodec(const ASTPtr & ast)
{
    const auto * codec = ast ? ast->as<ASTFunction>() : nullptr;
    if (!codec || codec->name != "CODEC" || !codec->arguments || codec->arguments->children.empty())
        return false;

    for (const auto & element : codec->arguments->children)
    {
        if (const auto * identifier = element->as<ASTIdentifier>())
        {
            if (!isCodecFamilyName(identifier->name()))
                return false;
        }
        else if (const auto * function = element->as<ASTFunction>())
        {
            if (!isCodecFamilyName(function->name))
                return false;
        }
        else
            return false;
    }

    return true;
}

}
