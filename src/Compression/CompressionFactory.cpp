#include <Compression/CompressionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/String.h>
#include <IO/ReadBuffer.h>
#include <Parsers/queryToString.h>
#include <Compression/CompressionCodecMultiple.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_CODEC;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
}

CompressionCodecPtr CompressionCodecFactory::getDefaultCodec() const
{
    return default_codec;
}


CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, std::optional<int> level, bool sanity_check) const
{
    if (level)
    {
        auto literal = std::make_shared<ASTLiteral>(static_cast<UInt64>(*level));
        return get(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), literal)), {}, sanity_check);
    }
    else
    {
        auto identifier = std::make_shared<ASTIdentifier>(Poco::toUpper(family_name));
        return get(makeASTFunction("CODEC", identifier), {}, sanity_check);
    }
}


CompressionCodecPtr CompressionCodecFactory::get(const ASTPtr & ast, DataTypePtr column_type, bool sanity_check) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        Codecs codecs;
        codecs.reserve(func->arguments->children.size());
        for (const auto & inner_codec_ast : func->arguments->children)
        {
            if (const auto * family_name = inner_codec_ast->as<ASTIdentifier>())
                codecs.emplace_back(getImpl(family_name->name, {}, column_type));
            else if (const auto * ast_func = inner_codec_ast->as<ASTFunction>())
                codecs.emplace_back(getImpl(ast_func->name, ast_func->arguments, column_type));
            else
                throw Exception("Unexpected AST element for compression codec", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
        }

        CompressionCodecPtr res;

        if (codecs.size() == 1)
            res = codecs.back();
        else if (codecs.size() > 1)
            res = std::make_shared<CompressionCodecMultiple>(codecs, sanity_check);

        /// Allow to explicitly specify single NONE codec if user don't want any compression.
        /// But applying other transformations solely without compression (e.g. Delta) does not make sense.
        if (sanity_check && !res->isCompression() && !res->isNone())
            throw Exception("Compression codec " + res->getCodecDesc() + " does not compress anything."
                " You may want to add generic compression algorithm after other transformations, like: " + res->getCodecDesc() + ", LZ4."
                " (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).",
                ErrorCodes::BAD_ARGUMENTS);

        return res;
    }

    throw Exception("Unknown codec family: " + queryToString(ast), ErrorCodes::UNKNOWN_CODEC);
}

CompressionCodecPtr CompressionCodecFactory::get(const uint8_t byte_code) const
{
    const auto family_code_and_creator = family_code_with_codec.find(byte_code);

    if (family_code_and_creator == family_code_with_codec.end())
        throw Exception("Unknown codec family code: " + toString(byte_code), ErrorCodes::UNKNOWN_CODEC);

    return family_code_and_creator->second({}, nullptr);
}


CompressionCodecPtr CompressionCodecFactory::getImpl(const String & family_name, const ASTPtr & arguments, DataTypePtr column_type) const
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
    registerCompressionCodecWithType(family_name, byte_code, [family_name, creator](const ASTPtr & ast, DataTypePtr /* data_type */)
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
            throw Exception("Compression codec " + family_name + " cannot have arguments", ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS);
        return creator();
    });
}


void registerCodecNone(CompressionCodecFactory & factory);
void registerCodecLZ4(CompressionCodecFactory & factory);
void registerCodecLZ4HC(CompressionCodecFactory & factory);
void registerCodecZSTD(CompressionCodecFactory & factory);
void registerCodecDelta(CompressionCodecFactory & factory);
void registerCodecT64(CompressionCodecFactory & factory);
void registerCodecDoubleDelta(CompressionCodecFactory & factory);
void registerCodecGorilla(CompressionCodecFactory & factory);
void registerCodecMultiple(CompressionCodecFactory & factory);

CompressionCodecFactory::CompressionCodecFactory()
{
    registerCodecLZ4(*this);
    registerCodecNone(*this);
    registerCodecZSTD(*this);
    registerCodecLZ4HC(*this);
    registerCodecDelta(*this);
    registerCodecT64(*this);
    registerCodecDoubleDelta(*this);
    registerCodecGorilla(*this);
    registerCodecMultiple(*this);

    default_codec = get("LZ4", {}, false);
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

}
