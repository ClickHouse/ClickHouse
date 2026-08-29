#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Core/Settings.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>

#include <algorithm>
#include <Columns/IColumn.h>

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
    extern const int OPENSSL_ERROR;
}

void CompressionCodecFactory::upperCaseCodecFamilyNames(const ASTPtr & codec_ast)
{
    const auto * func = codec_ast->as<ASTFunction>();
    if (!func || !func->arguments)
        return;

    for (auto & child : func->arguments->children)
    {
        if (const auto * identifier = child->as<ASTIdentifier>())
            child = make_intrusive<ASTIdentifier>(Poco::toUpper(identifier->name()));
        else if (auto * inner_func = child->as<ASTFunction>())
        {
            inner_func->name = Poco::toUpper(inner_func->name);

            /// Identifier-valued codec arguments (e.g. the `ALP` variant in `ALP(auto)`) were also
            /// upper-cased by the old whole-string normalization these string entry points replace, and
            /// the codec builders expect them upper-case, so a stored `'ALP(auto)'` must keep loading.
            /// Literal arguments (e.g. `T64('bit')`) are case-sensitive and stay as written.
            if (inner_func->arguments)
            {
                for (auto & argument : inner_func->arguments->children)
                {
                    if (const auto * argument_identifier = argument->as<ASTIdentifier>())
                        argument = make_intrusive<ASTIdentifier>(Poco::toUpper(argument_identifier->name()));
                }
            }
        }
    }
}

CompressionCodecPtr CompressionCodecFactory::getDefaultCodec() const
{
    return default_codec;
}

bool CompressionCodecFactory::isDefaultCodec(const ASTPtr & codec)
{
    /// No CODEC(...) clause: the default codec.
    if (codec == nullptr)
        return true;
    /// CODEC(Default)
    const auto * func = codec->as<ASTFunction>();
    if (!func || func->name != "CODEC" || !func->arguments || func->arguments->children.size() != 1)
        return false;
    const auto * ident = func->arguments->children[0]->as<ASTIdentifier>();
    return ident && ident->name() == DEFAULT_CODEC_NAME;
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
    auto ast = parseQuery(codec_parser, "(" + compression_codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    upperCaseCodecFamilyNames(ast);
    return CompressionCodecFactory::instance().get(ast, nullptr);
}

String CompressionCodecFactory::getReasonUnsafeForUntypedData(const String & compression_codec) const
{
    if (compression_codec.empty())
        return {};

    ParserCodec codec_parser;
    auto ast = parseQuery(
        codec_parser, "(" + compression_codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    upperCaseCodecFamilyNames(ast);

    return getReasonUnsafeForUntypedData(ast);
}

void CompressionCodecFactory::checkCodecStringSafeForUntypedData(
    const String & compression_codec, std::string_view setting_name) const
{
    if (auto reason = getReasonUnsafeForUntypedData(compression_codec); !reason.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Setting '{}' cannot use the codec {} because {}", setting_name, compression_codec, reason);
}

bool CompressionCodecFactory::isDefaultCodecAlias(const ASTPtr & codec_ast)
{
    if (!codec_ast)
        return false;

    const auto * func = codec_ast->as<ASTFunction>();
    if (!func || !func->arguments || func->arguments->children.size() != 1)
        return false;

    const auto & inner_codec_ast = func->arguments->children.front();
    if (const auto * family_name = inner_codec_ast->as<ASTIdentifier>())
        return family_name->name() == DEFAULT_CODEC_NAME;
    if (const auto * ast_func = inner_codec_ast->as<ASTFunction>())
        return ast_func->name == DEFAULT_CODEC_NAME && (!ast_func->arguments || ast_func->arguments->children.empty());
    return false;
}

bool CompressionCodecFactory::containsDefaultCodecAlias(const ASTPtr & codec_ast)
{
    if (!codec_ast)
        return false;

    const auto * func = codec_ast->as<ASTFunction>();
    if (!func || !func->arguments)
        return false;

    for (const auto & inner_codec_ast : func->arguments->children)
    {
        if (const auto * family_name = inner_codec_ast->as<ASTIdentifier>())
        {
            if (family_name->name() == DEFAULT_CODEC_NAME)
                return true;
        }
        else if (const auto * ast_func = inner_codec_ast->as<ASTFunction>())
        {
            if (ast_func->name == DEFAULT_CODEC_NAME)
                return true;
        }
    }
    return false;
}

String CompressionCodecFactory::getReasonUnsafeForUntypedData(const ASTPtr & codec_ast) const
{
    if (!codec_ast)
        return {};

    const auto * func = codec_ast->as<ASTFunction>();
    if (!func)
        throw Exception(
            ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST structure for compression codec: {}", codec_ast->formatForErrorMessage());

    /// Build each codec in the chain individually via `getImpl`, which (unlike `get(ast, column_type)`)
    /// does not apply the null-column-type lossy guard. That guard throws for lossy codecs such as `SZ3`,
    /// but here we must be able to classify them (so the caller can reset the offending setting) rather
    /// than throw. `getImpl` never reaches the guard, so we inspect `isLossyCompression` ourselves.
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

        /// `Default` is an alias for the server default codec, which is always safe for untyped data.
        if (codec_family_name == DEFAULT_CODEC_NAME)
            continue;

        /// Experimentality is deliberately not classified here: it is a policy gate, not a data-safety
        /// property. It is enforced with the session `allow_experimental_codecs` setting at the points
        /// where fresh user input enters (`validateCodecAndGetPreprocessedAST` and the codec-valued
        /// MergeTree settings gates in `registerStorageMergeTree` / `MergeTreeData::checkAlterIsPossible`),
        /// while stored metadata carrying an experimental codec must remain loadable and writable.
        auto codec = getImpl(codec_family_name, codec_arguments, nullptr);
        if (codec->requiresColumnTypeToCompress())
            return "it requires a column type and can not be applied to untyped data";
        if (codec->isLossyCompression())
            return "it is lossy and can only be applied to floating-point columns, not to untyped data";
    }

    return {};
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

String CompressionCodecFactory::getGateSettingName(const String & family_name)
{
    return fmt::format("enable_{}_codec", Poco::toLower(family_name));
}

std::optional<SettingsTierType> CompressionCodecFactory::getGateTier(const String & gate_setting_name)
{
    const std::optional<SettingsTierType> tier = Settings::tryGetTierOfBuiltin(gate_setting_name);
    if (tier == SettingsTierType::OBSOLETE)
        return std::nullopt;
    return tier;
}

Strings CompressionCodecFactory::getGateSettingNames() const
{
    Strings result;
    for (const auto & family : family_name_with_codec)
    {
        if (String gate_setting_name = getGateSettingName(family.first); getGateTier(gate_setting_name))
            result.push_back(std::move(gate_setting_name));
    }
    return result;
}

void CompressionCodecFactory::fillCodecDescriptions(MutableColumns & res_columns) const
{
    std::for_each(
        family_name_with_codec.begin(),
        family_name_with_codec.end(),
        [&](const auto &it)
        {
            const std::string &name = it.first;
            CompressionCodecPtr tmp;
            try
            {
                tmp = it.second({}, nullptr);
            }
            catch (const Exception & e)
            {
                /// Ok: the encryption codecs register a creator that throws `OPENSSL_ERROR` when the server is built
                /// without SSL support. They cannot expose a description, so skip them rather than failing the whole
                /// `system.codecs` query. Any other failure is unexpected and must propagate.
                if (e.code() == ErrorCodes::OPENSSL_ERROR)
                    return;
                throw;
            }

            const SettingsTierType tier = getGateTier(getGateSettingName(name)).value_or(SettingsTierType::PRODUCTION);

            res_columns[0]->insert(name);
            res_columns[1]->insert(tmp->getMethodByte());
            res_columns[2]->insert(tmp->isCompression());
            res_columns[3]->insert(tmp->isGenericCompression());
            res_columns[4]->insert(tmp->isEncryption());
            res_columns[5]->insert(tmp->isFloatingPointTimeSeriesCodec());
            res_columns[6]->insert(tier == SettingsTierType::EXPERIMENTAL);
            res_columns[7]->insert(tier);
            res_columns[8]->insert(tmp->getDescription());
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
        catch (const Exception & e)
        {
            /// Ok: the encryption codecs register a creator that throws `OPENSSL_ERROR` when the server is built
            /// without SSL support. They have no documentation to expose, so skip them rather than failing the whole
            /// `system.documentation` query. Any other failure is unexpected and must propagate.
            if (e.code() == ErrorCodes::OPENSSL_ERROR)
                continue;
            throw;
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
    registerCodecZXC(*this);
    registerCodecPco(*this);

    default_codec = get("LZ4", {});
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

}
