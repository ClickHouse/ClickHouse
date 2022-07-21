#include "config_core.h"

#include <Compression/CompressionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/String.h>
#include <IO/ReadBuffer.h>
#include <Parsers/queryToString.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionCodecNone.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_CODEC;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
}

CompressionCodecPtr CompressionCodecFactory::getDefaultCodec() const
{
    return default_codec;
}


CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, std::optional<int> level) const
{
    if (level)
    {
        auto level_literal = std::make_shared<ASTLiteral>(static_cast<UInt64>(*level));
        return get(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), level_literal)), {});
    }
    else
    {
        auto identifier = std::make_shared<ASTIdentifier>(Poco::toUpper(family_name));
        return get(makeASTFunction("CODEC", identifier), {});
    }
}


CompressionCodecPtr CompressionCodecFactory::get(
    const ASTPtr & ast, const IDataType * column_type, CompressionCodecPtr current_default, bool only_generic) const
{
    if (current_default == nullptr)
        current_default = default_codec;

    if (const auto * func = ast->as<ASTFunction>())
    {
        Codecs codecs;
        codecs.reserve(func->arguments->children.size());
        for (const auto & inner_codec_ast : func->arguments->children)
        {
            String codec_family_name;
            ASTPtr codec_arguments;
            if (const auto * family_name = inner_codec_ast->as<ASTIdentifier>())
            {
                codec_family_name = family_name->name();
                codec_arguments = {};
            }
            else if (const auto * ast_func = inner_codec_ast->as<ASTFunction>())
            {
                codec_family_name = ast_func->name;
                codec_arguments = ast_func->arguments;
            }
            else
                throw Exception("Unexpected AST element for compression codec", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            CompressionCodecPtr codec;
            if (codec_family_name == DEFAULT_CODEC_NAME)
                codec = current_default;
            else
                codec = getImpl(codec_family_name, codec_arguments, column_type);

            if (only_generic && !codec->isGenericCompression())
                continue;

            codecs.emplace_back(codec);
        }

        CompressionCodecPtr res;

        if (codecs.size() == 1)
            return codecs.back();
        else if (codecs.size() > 1)
            return std::make_shared<CompressionCodecMultiple>(codecs);
        else
            return std::make_shared<CompressionCodecNone>();
    }

    throw Exception("Unexpected AST structure for compression codec: " + queryToString(ast), ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}


CompressionCodecPtr CompressionCodecFactory::get(uint8_t byte_code) const
{
    const auto family_code_and_creator = family_code_with_codec.find(byte_code);

    if (family_code_and_creator == family_code_with_codec.end())
        throw Exception("Unknown codec family code: " + toString(byte_code), ErrorCodes::UNKNOWN_CODEC);

    return family_code_and_creator->second({}, nullptr);
}


CompressionCodecPtr CompressionCodecFactory::getImpl(const String & family_name, const ASTPtr & arguments, const IDataType * column_type) const
{
    if (family_name == "Multiple")
        throw Exception("Codec Multiple cannot be specified directly", ErrorCodes::UNKNOWN_CODEC);

    const auto family_and_creator = family_name_with_codec.find(family_name);

    if (family_and_creator == family_name_with_codec.end())
        throw Exception("Unknown codec family: " + family_name, ErrorCodes::UNKNOWN_CODEC);

    return family_and_creator->second(arguments, column_type);
}

void CompressionCodecFactory::registerCompressionCodecWithType(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    CreatorWithType creator)
{
    if (creator == nullptr)
        throw Exception("CompressionCodecFactory: the codec family " + family_name + " has been provided a null constructor",
                        ErrorCodes::LOGICAL_ERROR);

    if (!family_name_with_codec.emplace(family_name, creator).second)
        throw Exception("CompressionCodecFactory: the codec family name '" + family_name + "' is not unique", ErrorCodes::LOGICAL_ERROR);

    if (byte_code)
        if (!family_code_with_codec.emplace(*byte_code, creator).second)
            throw Exception("CompressionCodecFactory: the codec family code '" + std::to_string(*byte_code) + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}

void CompressionCodecFactory::registerCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, Creator creator)
{
    registerCompressionCodecWithType(family_name, byte_code, [family_name, creator](const ASTPtr & ast, const IDataType * /* data_type */)
    {
        return creator(ast);
    });
}

void CompressionCodecFactory::registerSimpleCompressionCodec(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    SimpleCreator creator)
{
    registerCompressionCodec(family_name, byte_code, [family_name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS, "Compression codec {} cannot have arguments", family_name);
        return creator();
    });
}


void registerCodecNone(CompressionCodecFactory & factory);
void registerCodecLZ4(CompressionCodecFactory & factory);
void registerCodecLZ4HC(CompressionCodecFactory & factory);
void registerCodecZSTD(CompressionCodecFactory & factory);
void registerCodecMultiple(CompressionCodecFactory & factory);


/// Keeper use only general-purpose codecs, so we don't need these special codecs
/// in standalone build
#ifndef KEEPER_STANDALONE_BUILD

void registerCodecDelta(CompressionCodecFactory & factory);
void registerCodecT64(CompressionCodecFactory & factory);
void registerCodecDoubleDelta(CompressionCodecFactory & factory);
void registerCodecGorilla(CompressionCodecFactory & factory);
void registerCodecEncrypted(CompressionCodecFactory & factory);
void registerCodecFPC(CompressionCodecFactory & factory);

#endif

CompressionCodecFactory::CompressionCodecFactory()
{
    registerCodecNone(*this);
    registerCodecLZ4(*this);
    registerCodecZSTD(*this);
    registerCodecLZ4HC(*this);
    registerCodecMultiple(*this);

#ifndef KEEPER_STANDALONE_BUILD
    registerCodecDelta(*this);
    registerCodecT64(*this);
    registerCodecDoubleDelta(*this);
    registerCodecGorilla(*this);
    registerCodecEncrypted(*this);
    registerCodecFPC(*this);
#endif

    default_codec = get("LZ4", {});
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

}
