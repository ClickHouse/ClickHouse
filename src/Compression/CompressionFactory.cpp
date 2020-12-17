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
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
}

static constexpr auto DEFAULT_CODEC_NAME = "Default";

CompressionCodecPtr CompressionCodecFactory::getDefaultCodec() const
{
    return default_codec;
}


CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, std::optional<int> level) const
{
    if (level)
    {
        auto literal = std::make_shared<ASTLiteral>(static_cast<UInt64>(*level));
        return get(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), literal)), {});
    }
    else
    {
        auto identifier = std::make_shared<ASTIdentifier>(Poco::toUpper(family_name));
        return get(makeASTFunction("CODEC", identifier), {});
    }
}

void CompressionCodecFactory::validateCodec(const String & family_name, std::optional<int> level, bool sanity_check) const
{
    if (level)
    {
        auto literal = std::make_shared<ASTLiteral>(static_cast<UInt64>(*level));
        validateCodecAndGetPreprocessedAST(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), literal)), {}, sanity_check);
    }
    else
    {
        auto identifier = std::make_shared<ASTIdentifier>(Poco::toUpper(family_name));
        validateCodecAndGetPreprocessedAST(makeASTFunction("CODEC", identifier), {}, sanity_check);
    }
}

ASTPtr CompressionCodecFactory::validateCodecAndGetPreprocessedAST(const ASTPtr & ast, const IDataType * column_type, bool sanity_check) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        ASTPtr codecs_descriptions = std::make_shared<ASTExpressionList>();

        bool is_compression = false;
        bool has_none = false;
        std::optional<size_t> generic_compression_codec_pos;

        bool can_substitute_codec_arguments = true;
        for (size_t i = 0; i < func->arguments->children.size(); ++i)
        {
            const auto & inner_codec_ast = func->arguments->children[i];
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

            /// Default codec replaced with current default codec which may depend on different
            /// settings (and properties of data) in runtime.
            CompressionCodecPtr result_codec;
            if (codec_family_name == DEFAULT_CODEC_NAME)
            {
                if (codec_arguments != nullptr)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "{} codec cannot have any arguments, it's just an alias for codec specified in config.xml", DEFAULT_CODEC_NAME);

                result_codec = default_codec;
                codecs_descriptions->children.emplace_back(std::make_shared<ASTIdentifier>(DEFAULT_CODEC_NAME));
            }
            else
            {
                if (column_type)
                {
                    CompressionCodecPtr prev_codec;
                    IDataType::StreamCallback callback = [&](const IDataType::SubstreamPath & substream_path, const IDataType & substream_type)
                    {
                        if (IDataType::isSpecialCompressionAllowed(substream_path))
                        {
                            result_codec = getImpl(codec_family_name, codec_arguments, &substream_type);

                            /// Case for column Tuple, which compressed with codec which depends on data type, like Delta.
                            /// We cannot substitute parameters for such codecs.
                            if (prev_codec && prev_codec->getHash() != result_codec->getHash())
                                can_substitute_codec_arguments = false;
                            prev_codec = result_codec;
                        }
                    };

                    IDataType::SubstreamPath stream_path;
                    column_type->enumerateStreams(callback, stream_path);

                    if (!result_codec)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find any substream with data type for type {}. It's a bug", column_type->getName());
                }
                else
                {
                    result_codec = getImpl(codec_family_name, codec_arguments, nullptr);
                }

                codecs_descriptions->children.emplace_back(result_codec->getCodecDesc());
            }

            is_compression |= result_codec->isCompression();
            has_none |= result_codec->isNone();

            if (!generic_compression_codec_pos && result_codec->isGenericCompression())
                generic_compression_codec_pos = i;
        }

        String codec_description = queryToString(codecs_descriptions);

        if (sanity_check)
        {
            if (codecs_descriptions->children.size() > 1 && has_none)
                throw Exception(
                    "It does not make sense to have codec NONE along with other compression codecs: " + codec_description
                        + ". (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).",
                    ErrorCodes::BAD_ARGUMENTS);

            /// Allow to explicitly specify single NONE codec if user don't want any compression.
            /// But applying other transformations solely without compression (e.g. Delta) does not make sense.
            if (!is_compression && !has_none)
                throw Exception(
                    "Compression codec " + codec_description
                        + " does not compress anything."
                          " You may want to add generic compression algorithm after other transformations, like: "
                        + codec_description
                        + ", LZ4."
                          " (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).",
                    ErrorCodes::BAD_ARGUMENTS);

            /// It does not make sense to apply any transformations after generic compression algorithm
            /// So, generic compression can be only one and only at the end.
            if (generic_compression_codec_pos && *generic_compression_codec_pos != codecs_descriptions->children.size() - 1)
                throw Exception("The combination of compression codecs " + codec_description + " is meaningless,"
                    " because it does not make sense to apply any transformations after generic compression algorithm."
                    " (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).", ErrorCodes::BAD_ARGUMENTS);

        }
        /// For columns with nested types like Tuple(UInt32, UInt64) we
        /// obviously cannot substitute parameters for codecs which depend on
        /// data type, because for the first column Delta(4) is suitable and
        /// Delta(8) for the second. So we should leave codec description as is
        /// and deduce them in get method for each subtype separately. For all
        /// other types it's better to substitute parameters, for better
        /// readability and backward compatibility.
        if (can_substitute_codec_arguments)
        {
            std::shared_ptr<ASTFunction> result = std::make_shared<ASTFunction>();
            result->name = "CODEC";
            result->arguments = codecs_descriptions;
            return result;
        }
        else
        {
            return ast;
        }
    }

    throw Exception("Unknown codec family: " + queryToString(ast), ErrorCodes::UNKNOWN_CODEC);
}

CompressionCodecPtr CompressionCodecFactory::get(const ASTPtr & ast, const IDataType * column_type, CompressionCodecPtr current_default, bool only_generic) const
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

CompressionCodecPtr CompressionCodecFactory::get(const uint8_t byte_code) const
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

    default_codec = get("LZ4", {});
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

}
