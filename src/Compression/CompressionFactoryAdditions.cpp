/**
 * This file contains a part of CompressionCodecFactory methods definitions and
 * is needed only because they have dependencies on DataTypes.
 * They are not useful for fuzzers, so we leave them in other translation unit.
 */

#include <Compression/CompressionFactory.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNested.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_CODEC;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


void CompressionCodecFactory::validateCodec(
    const String & family_name, std::optional<int> level, bool sanity_check, bool allow_experimental_codecs) const
{
    if (family_name.empty())
        throw Exception("Compression codec name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

    if (level)
    {
        auto literal = std::make_shared<ASTLiteral>(static_cast<UInt64>(*level));
        validateCodecAndGetPreprocessedAST(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), literal)),
            {}, sanity_check, allow_experimental_codecs);
    }
    else
    {
        auto identifier = std::make_shared<ASTIdentifier>(Poco::toUpper(family_name));
        validateCodecAndGetPreprocessedAST(makeASTFunction("CODEC", identifier),
            {}, sanity_check, allow_experimental_codecs);
    }
}


ASTPtr CompressionCodecFactory::validateCodecAndGetPreprocessedAST(
    const ASTPtr & ast, const DataTypePtr & column_type, bool sanity_check, bool allow_experimental_codecs) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        ASTPtr codecs_descriptions = std::make_shared<ASTExpressionList>();

        bool is_compression = false;
        bool has_none = false;
        std::optional<size_t> generic_compression_codec_pos;
        std::set<size_t> encryption_codecs;

        bool can_substitute_codec_arguments = true;
        auto it = func->arguments->children.begin();
        for (size_t i = 0, size = func->arguments->children.size(); i < size; ++i, ++it)
        {
            const auto & inner_codec_ast = *it;
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
                    ISerialization::StreamCallback callback = [&](const auto & substream_path)
                    {
                        assert(!substream_path.empty());
                        if (ISerialization::isSpecialCompressionAllowed(substream_path))
                        {
                            const auto & last_type = substream_path.back().data.type;
                            result_codec = getImpl(codec_family_name, codec_arguments, last_type.get());

                            /// Case for column Tuple, which compressed with codec which depends on data type, like Delta.
                            /// We cannot substitute parameters for such codecs.
                            if (prev_codec && prev_codec->getHash() != result_codec->getHash())
                                can_substitute_codec_arguments = false;
                            prev_codec = result_codec;
                        }
                    };

                    ISerialization::SubstreamPath path;
                    column_type->getDefaultSerialization()->enumerateStreams(path, callback, column_type);

                    if (!result_codec)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find any substream with data type for type {}. It's a bug", column_type->getName());
                }
                else
                {
                    result_codec = getImpl(codec_family_name, codec_arguments, nullptr);
                }

                if (!allow_experimental_codecs && result_codec->isExperimental())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Codec {} is experimental and not meant to be used in production."
                        " You can enable it with the 'allow_experimental_codecs' setting.",
                        codec_family_name);

                codecs_descriptions->children.emplace_back(result_codec->getCodecDesc());
            }

            is_compression |= result_codec->isCompression();
            has_none |= result_codec->isNone();

            if (!generic_compression_codec_pos && result_codec->isGenericCompression())
                generic_compression_codec_pos = i;

            if (result_codec->isEncryption())
                encryption_codecs.insert(i);
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
            /// It's okay to apply encryption codecs solely without anything else.
            if (!is_compression && !has_none && encryption_codecs.size() != codecs_descriptions->children.size())
                throw Exception(
                    "Compression codec " + codec_description
                        + " does not compress anything."
                          " You may want to add generic compression algorithm after other transformations, like: "
                        + codec_description
                        + ", LZ4."
                          " (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).",
                    ErrorCodes::BAD_ARGUMENTS);

            /// It does not make sense to apply any non-encryption codecs
            /// after encryption one.
            if (!encryption_codecs.empty() &&
                *encryption_codecs.begin() != codecs_descriptions->children.size() - encryption_codecs.size())
                throw Exception("The combination of compression codecs " + codec_description + " is meaningless,"
                                " because it does not make sense to apply any non-post-processing codecs after"
                                " post-processing ones. (Note: you can enable setting 'allow_suspicious_codecs'"
                                " to skip this check).", ErrorCodes::BAD_ARGUMENTS);

            /// It does not make sense to apply any transformations after generic compression algorithm
            /// So, generic compression can be only one and only at the end.
            if (generic_compression_codec_pos &&
                *generic_compression_codec_pos != codecs_descriptions->children.size() - 1 - encryption_codecs.size())
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


}
