#include <Compression/CompressionFactory.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionCodecNone.h>
#include <Compression/registerCompressionCodecs.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>

#include <Columns/IColumn.h>
#include <algorithm>

#include <boost/algorithm/string/join.hpp>

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_CODEC;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
    extern const int BAD_ARGUMENTS;
}

CompressionCodecPtr CompressionCodecFactory::getDefaultCodec() const
{
    return default_codec;
}


CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, std::optional<int> level) const
{
    if (level)
    {
        auto level_literal = make_intrusive<ASTLiteral>(static_cast<UInt64>(*level));
        return get(makeASTFunction("CODEC", makeASTFunction(Poco::toUpper(family_name), level_literal)), {});
    }

    auto identifier = make_intrusive<ASTIdentifier>(Poco::toUpper(family_name));
    return get(makeASTFunction("CODEC", identifier), {});
}

CompressionCodecPtr CompressionCodecFactory::get(const String & compression_codec) const
{
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return CompressionCodecFactory::instance().get(ast, nullptr);
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
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST element for compression codec");

            CompressionCodecPtr codec;
            if (codec_family_name == DEFAULT_CODEC_NAME)
                codec = current_default;
            else
                codec = getImpl(codec_family_name, codec_arguments, column_type);

            if (only_generic && !codec->isGenericCompression())
                continue;

            /// Lossy codecs (e.g. SZ3) reinterpret the raw bytes as floating-point values. When the data type
            /// is unknown we can not verify the column is floating-point, so applying a lossy codec would
            /// silently corrupt the data. This happens for the marks, primary key and default compression codec
            /// settings, which build codecs with a null type. Non-generic lossy codecs are already filtered out
            /// above for structural substreams (the `only_generic` path), so this rejects only codecs that would
            /// actually be used. The decompression path (`get(uint8_t)`) builds codecs directly through the
            /// creator and never reaches this point, so reading existing data is unaffected.
            if (!column_type && codec->isLossyCompression())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec {} is lossy and can only be applied to Float32/Float64 columns (or arrays/tuples/nullables "
                    "of them); it can not be used as a marks, primary key or default compression codec, or in any "
                    "other context where the column data type is unknown",
                    codec_family_name);

            codecs.emplace_back(codec);
        }

        CompressionCodecPtr res;

        if (codecs.size() == 1)
            return codecs.back();
        if (codecs.size() > 1)
            return std::make_shared<CompressionCodecMultiple>(codecs);
        return std::make_shared<CompressionCodecNone>();
    }

    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST structure for compression codec: {}", ast->formatForErrorMessage());
}


CompressionCodecPtr CompressionCodecFactory::get(uint8_t byte_code) const
{
    const auto family_code_and_creator = family_code_with_codec.find(byte_code);

    if (family_code_and_creator == family_code_with_codec.end())
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Unknown codec family code: {}", toString(byte_code));

    return family_code_and_creator->second({}, nullptr);
}

void CompressionCodecFactory::fillCodecDescriptions(MutableColumns & res_columns) const
{
    std::for_each(
        family_name_with_codec.begin(),
        family_name_with_codec.end(),
        [&](const auto &it)
        {
            const std::string &name = it.first;
            CompressionCodecPtr tmp = it.second({}, nullptr);

            res_columns[0]->insert(name);
            res_columns[1]->insert(tmp->getMethodByte());
            res_columns[2]->insert(tmp->isCompression());
            res_columns[3]->insert(tmp->isGenericCompression());
            res_columns[4]->insert(tmp->isEncryption());
            res_columns[5]->insert(tmp->isFloatingPointTimeSeriesCodec());
            res_columns[6]->insert(tmp->isExperimental());
            res_columns[7]->insert(tmp->getDescription());
        }
    );
}

VectorWithMemoryTracking<std::pair<String, Documentation>> CompressionCodecFactory::getCodecDocumentations() const
{
    VectorWithMemoryTracking<std::pair<String, Documentation>> result;
    result.reserve(family_name_with_codec.size());
    for (const auto & [name, creator] : family_name_with_codec)
    {
        CompressionCodecPtr codec;
        try
        {
            codec = creator({}, nullptr);
        }
        catch (...) // Ok: some codecs cannot be instantiated in this build configuration (e.g. the encryption codecs
                    // register a creator that throws when the server is built without SSL support). They have no
                    // documentation to expose, so skip them rather than failing the whole system.documentation query.
        {
            continue;
        }

        Documentation documentation;
        documentation.description = codec->getDescription();
        /// The codec carries its description through `getDescription` rather than a `Documentation` object, so the
        /// source is not captured automatically; use the registration site recorded in `registerCompressionCodec*`.
        if (auto it = family_name_with_source.find(name); it != family_name_with_source.end())
            documentation.source = it->second;
        result.emplace_back(name, std::move(documentation));
    }
    return result;
}

CompressionCodecPtr CompressionCodecFactory::getImpl(const String & family_name, const ASTPtr & arguments, const IDataType * column_type) const
{
    if (family_name == "Multiple")
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Codec Multiple cannot be specified directly");

    const auto family_and_creator = family_name_with_codec.find(family_name);

    if (family_and_creator == family_name_with_codec.end())
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Unknown codec family: {}", family_name);

    return family_and_creator->second(arguments, column_type);
}

void CompressionCodecFactory::registerCompressionCodecWithType(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    CreatorWithType creator,
    std::source_location source)
{
    if (creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFactory: "
                        "the codec family {} has been provided a null constructor", family_name);

    if (!family_name_with_codec.emplace(family_name, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFactory: the codec family name '{}' is not unique", family_name);

    family_name_with_source.emplace(family_name, source.file_name());

    if (byte_code)
        if (!family_code_with_codec.emplace(*byte_code, creator).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "CompressionCodecFactory: the codec family code '{}' is not unique",
                            std::to_string(*byte_code));
}

void CompressionCodecFactory::registerCompressionCodec(const String & family_name, std::optional<uint8_t> byte_code, Creator creator, std::source_location source)
{
    registerCompressionCodecWithType(family_name, byte_code, [family_name, creator](const ASTPtr & ast, const IDataType * /* data_type */)
    {
        return creator(ast);
    }, source);
}

void CompressionCodecFactory::registerSimpleCompressionCodec(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    SimpleCreator creator,
    std::source_location source)
{
    registerCompressionCodec(family_name, byte_code, [family_name, creator](const ASTPtr & ast)
    {
        if (ast)
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS, "Compression codec {} cannot have arguments", family_name);
        return creator();
    }, source);
}


Strings CompressionCodecFactory::getAllRegisteredNames() const
{
    Strings result;
    result.reserve(family_name_with_codec.size());
    for (const auto & pair : family_name_with_codec)
        result.push_back(pair.first);
    return result;
}


/// Defined in individual CompressionCodec*.cpp files
/// and declared in registerCompressionCodecs.h

CompressionCodecFactory::CompressionCodecFactory()
{
    registerCodecNone(*this);
    registerCodecLZ4(*this);
    registerCodecZSTD(*this);
    registerCodecLZ4HC(*this);
    registerCodecMultiple(*this);
    registerCodecDelta(*this);
    registerCodecT64(*this);
    registerCodecDoubleDelta(*this);
    registerCodecGorilla(*this);
    registerCodecEncrypted(*this);
    registerCodecFPC(*this);
    registerCodecGCD(*this);
    registerCodecALP(*this);
    registerCodecQuantized(*this);
#if USE_SZ3
    registerCodecSZ3(*this);
#endif

    default_codec = get("LZ4", {});
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

}
