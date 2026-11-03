#include <span>
#include <Compression/CompressionCodecFirstApplicable.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/StringUtils.h>
#include <Poco/String.h>
#include <Common/typeid_cast.h>

#include <Columns/IColumn.h>

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

namespace
{

/// The family name and the arguments of an element of a codec chain.
std::pair<String, ASTPtr> getCodecNameAndArguments(const ASTPtr & element)
{
    if (const auto * identifier = element->as<ASTIdentifier>())
        return {identifier->name(), nullptr};
    if (const auto * function = element->as<ASTFunction>())
        return {function->name, function->arguments};
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST element for compression codec: {}", element->formatForErrorMessage());
}

bool isFirstApplicable(const String & name)
{
    return equalsCaseInsensitive(name, FIRST_APPLICABLE_CODEC_NAME);
}

bool isMultiple(const String & name)
{
    return equalsCaseInsensitive(name, "Multiple");
}

/// The arguments of `FirstApplicable` or `Multiple`, which must not be empty.
const ASTs & getCombinatorArguments(const String & name, const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec {} requires at least one codec as an argument", name);
    return arguments->children;
}

/// A codec chain that has a `FirstApplicable` resolved without a data type depends on the data type.
bool codecHasFirstApplicable(const CompressionCodecPtr & codec)
{
    if (typeid_cast<const CompressionCodecFirstApplicable *>(codec.get()))
        return true;
    if (const auto * multiple = typeid_cast<const CompressionCodecMultiple *>(codec.get()))
        return std::ranges::any_of(multiple->getCodecs(), [](const auto & inner) { return codecHasFirstApplicable(inner); });
    return false;
}

/// A copy of the chain element `element` with every `Default` inside replaced by the description of `current_default`.
ASTPtr replaceDefaultCodec(const ASTPtr & element, const CompressionCodecPtr & current_default)
{
    const auto [name, arguments] = getCodecNameAndArguments(element);
    if (equalsCaseInsensitive(name, DEFAULT_CODEC_NAME))
    {
        ASTPtr description = current_default->getCodecDescription();
        /// The description of a chain is a list of codecs, which is `Multiple` as an alternative of `FirstApplicable`.
        if (const auto * list = description->as<ASTExpressionList>())
            return makeASTFunction("Multiple", ASTs(list->children.begin(), list->children.end()));
        return description;
    }

    if (!arguments || !(isFirstApplicable(name) || isMultiple(name)))
        return element->clone();

    ASTs replaced_arguments;
    replaced_arguments.reserve(arguments->children.size());
    for (const auto & argument : arguments->children)
        replaced_arguments.push_back(replaceDefaultCodec(argument, current_default));
    return makeASTFunction(name, std::move(replaced_arguments));
}

}

bool CompressionCodecFactory::isApplicable(const ASTPtr & element, const IDataType * type) const
{
    const auto [name, arguments] = getCodecNameAndArguments(element);

    /// `Default` is resolved for the data type by itself.
    if (equalsCaseInsensitive(name, DEFAULT_CODEC_NAME))
        return true;

    if (isMultiple(name))
        return std::ranges::all_of(getCombinatorArguments(name, arguments), [&](const auto & codec) { return isApplicable(codec, type); });

    if (isFirstApplicable(name))
        return std::ranges::any_of(getCombinatorArguments(name, arguments), [&](const auto & codec) { return isApplicable(codec, type); });

    const String * canonical_family_name = tryGetCanonicalFamilyName(name);
    if (!canonical_family_name)
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Unknown codec family: {}", name);

    const auto & properties = family_name_with_properties.at(*canonical_family_name);
    /// A declarative codec takes effect only through the serialization of the column, which is chosen from the codecs
    /// of the column declaration, not from the alternatives of `FirstApplicable`.
    if (properties.is_declarative)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec {} cannot be used inside {}", name, FIRST_APPLICABLE_CODEC_NAME);

    if (!properties.is_applicable_to_type)
        return true;
    return type && properties.is_applicable_to_type(*type);
}

void CompressionCodecFactory::appendResolvedChainElement(const ASTPtr & element, const IDataType * type, ASTs & out) const
{
    const auto [name, arguments] = getCodecNameAndArguments(element);

    if (isMultiple(name))
    {
        for (const auto & codec : getCombinatorArguments(name, arguments))
            appendResolvedChainElement(codec, type, out);
        return;
    }

    if (isFirstApplicable(name))
    {
        for (const auto & alternative : getCombinatorArguments(name, arguments))
        {
            if (isApplicable(alternative, type))
            {
                appendResolvedChainElement(alternative, type, out);
                return;
            }
        }

        if (type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "None of the codecs in {} is applicable to the data type {}",
                element->formatForErrorMessage(), type->getName());
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "None of the codecs in {} is applicable when the data type is unknown, e.g. as a default, marks or primary key "
            "compression codec. Add a codec that is applicable to any data type as the last alternative, like ZSTD",
            element->formatForErrorMessage());
    }

    out.push_back(element);
}

ASTPtr CompressionCodecFactory::resolveFirstApplicable(const ASTPtr & ast, const IDataType * type) const
{
    const auto * func = ast->as<ASTFunction>();
    if (!func || !func->arguments)
        return ast;

    const auto & elements = func->arguments->children;
    const auto is_first_applicable = [](const ASTPtr & element) { return isFirstApplicable(getCodecNameAndArguments(element).first); };
    if (!std::ranges::any_of(elements, is_first_applicable))
        return ast;

    ASTs resolved;
    for (const auto & element : elements)
    {
        if (is_first_applicable(element))
            appendResolvedChainElement(element, type, resolved);
        else
            resolved.push_back(element);
    }
    return makeASTFunction("CODEC", std::move(resolved));
}

void CompressionCodecFactory::checkFirstApplicableHasNoLossyAlternative(const ASTPtr & element) const
{
    const auto [name, arguments] = getCodecNameAndArguments(element);

    if (isFirstApplicable(name) || isMultiple(name))
    {
        for (const auto & codec : getCombinatorArguments(name, arguments))
            checkFirstApplicableHasNoLossyAlternative(codec);
        return;
    }

    /// `Default` is not allowed without a data type, and the default codec itself is validated where it is set.
    if (equalsCaseInsensitive(name, DEFAULT_CODEC_NAME))
        return;

    if (getImpl(name, arguments, nullptr)->isLossyCompression())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codec {} is lossy and cannot be an alternative of {} where the data type is unknown, e.g. as a default or "
            "TTL recompression codec: it would be selected for the type of every column, including the ones where lossy "
            "compression is not allowed (a sorting key, the keys of a Map or a LowCardinality dictionary)",
            name, FIRST_APPLICABLE_CODEC_NAME);
}

CompressionCodecPtr CompressionCodecFactory::getFirstApplicableWithoutType(const ASTPtr & element, const CompressionCodecPtr & current_default) const
{
    ASTs selected_codecs;
    appendResolvedChainElement(element, nullptr, selected_codecs);
    CompressionCodecPtr selected = get(makeASTFunction("CODEC", std::move(selected_codecs)), nullptr, current_default);
    return std::make_shared<CompressionCodecFirstApplicable>(replaceDefaultCodec(element, current_default), std::move(selected));
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
    return ident && equalsCaseInsensitive(ident->name(), DEFAULT_CODEC_NAME);
}


CompressionCodecPtr CompressionCodecFactory::get(const String & family_name, std::optional<int> level) const
{
    checkCodecIsNotColumnLevelOnly(family_name);

    if (level)
    {
        auto level_literal = make_intrusive<ASTLiteral>(static_cast<UInt64>(*level));
        return get(makeASTFunction("CODEC", makeASTFunction(family_name, level_literal)), {});
    }

    auto identifier = make_intrusive<ASTIdentifier>(family_name);
    return get(makeASTFunction("CODEC", identifier), {});
}

CompressionCodecPtr CompressionCodecFactory::get(const String & compression_codec) const
{
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + compression_codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    checkCodecChainIsNotColumnLevelOnly(ast);
    /// A codec given as a string (e.g. `default_compression_codec`) has no data type, and `FirstApplicable` in it is
    /// selected again for the type of every column, where the restrictions of lossy codecs are not checked.
    if (hasFirstApplicable(ast))
        for (const auto & element : ast->as<const ASTFunction &>().arguments->children)
            if (isFirstApplicable(getCodecNameAndArguments(element).first))
                checkFirstApplicableHasNoLossyAlternative(element);
    return CompressionCodecFactory::instance().get(ast, nullptr);
}

CompressionCodecPtr CompressionCodecFactory::get(
    const ASTPtr & ast, const IDataType * column_type, CompressionCodecPtr current_default, bool only_generic) const
{
    if (current_default == nullptr)
        current_default = default_codec;

    /// With a known data type, `FirstApplicable` is replaced by the codecs it selects; without one, it is kept as
    /// `CompressionCodecFirstApplicable`, so that it can be resolved again once the data type is known.
    const ASTPtr resolved_ast = column_type ? resolveFirstApplicable(ast, column_type) : ast;

    if (const auto * func = resolved_ast->as<ASTFunction>())
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
            if (equalsCaseInsensitive(codec_family_name, DEFAULT_CODEC_NAME))
            {
                codec = current_default;
                /// E.g. `default_compression_codec = 'FirstApplicable(PCO, ZSTD(3))'`: select for the data type.
                if (column_type && codecHasFirstApplicable(codec))
                    codec = get(codec->getFullCodecDescription(), column_type);
            }
            else if (isFirstApplicable(codec_family_name))
            {
                /// Only without a data type: with one, `resolveFirstApplicable` has replaced it.
                chassert(!column_type);
                /// A structural substream (e.g. the sizes of an array) keeps only generic codecs. If no alternative is
                /// applicable without a data type, there is no generic codec to keep, the same as for a specialized codec.
                if (only_generic && !isApplicable(inner_codec_ast, nullptr))
                    continue;
                codec = getFirstApplicableWithoutType(inner_codec_ast, current_default);
            }
            else
                codec = getImpl(codec_family_name, codec_arguments, column_type);

            /// The structural substreams keep only the generic codecs of the ones that actually compress.
            if (only_generic)
                if (const auto * first_applicable = typeid_cast<const CompressionCodecFirstApplicable *>(codec.get()))
                    codec = first_applicable->getSelected();

            std::span<const CompressionCodecPtr> expanded_codecs(&codec, 1);

            /// `CODEC(ALP, Default)` with `default_compression_codec = 'LZ4, AES_128_GCM_SIV'` must become `ALP, LZ4, AES_128_GCM_SIV`.
            /// Not a chain within a chain.
            if (const auto * multiple = typeid_cast<const CompressionCodecMultiple *>(codec.get()))
                expanded_codecs = multiple->getCodecs();

            for (const auto & expanded_codec : expanded_codecs)
            {
                if (only_generic && !expanded_codec->isGenericCompression() && !expanded_codec->isEncryption())
                    continue;

                if (!column_type && expanded_codec->isLossyCompression())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Codec {} is lossy and can only be applied to Float32/Float64 columns (or arrays/tuples/nullables "
                        "of them); it can not be used as a marks, primary key or default compression codec, or in any "
                        "other context where the column data type is unknown",
                        codec_family_name);

                codecs.emplace_back(expanded_codec);
            }
        }

        if (codecs.size() == 1)
            return codecs.back();
        if (codecs.size() > 1)
            return std::make_shared<CompressionCodecMultiple>(std::move(codecs));
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
            res_columns[6]->insert(tier);
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
    if (equalsCaseInsensitive(family_name, "Multiple"))
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Codec Multiple cannot be specified directly");

    const String * canonical_family_name = tryGetCanonicalFamilyName(family_name);
    if (!canonical_family_name)
        throw Exception(ErrorCodes::UNKNOWN_CODEC, "Unknown codec family: {}", family_name);

    const auto family_and_creator = family_name_with_codec.find(*canonical_family_name);

    return family_and_creator->second(arguments, column_type);
}

const String * CompressionCodecFactory::tryGetCanonicalFamilyName(const String & family_name) const
{
    if (const auto exact = family_name_with_codec.find(family_name); exact != family_name_with_codec.end())
        return &exact->first;

    const auto it = lowercase_family_name_to_canonical.find(Poco::toLower(family_name));
    if (it == lowercase_family_name_to_canonical.end())
        return nullptr;
    return &it->second;
}

bool CompressionCodecFactory::isDeclarativeCodec(const String & family_name) const
{
    const String * canonical_family_name = tryGetCanonicalFamilyName(family_name);
    if (!canonical_family_name)
        return false;
    return family_name_with_properties.at(*canonical_family_name).is_declarative;
}

void CompressionCodecFactory::registerCompressionCodecWithType(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    CreatorWithType creator,
    CompressionCodecFamilyProperties properties,
    std::source_location source)
{
    if (creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFactory: "
                        "the codec family {} has been provided a null constructor", family_name);

    if (!family_name_with_codec.emplace(family_name, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFactory: the codec family name '{}' is not unique", family_name);

    if (!lowercase_family_name_to_canonical.emplace(Poco::toLower(family_name), family_name).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "CompressionCodecFactory: the codec family name '{}' differs only in case from another one", family_name);

    family_name_with_source.emplace(family_name, source.file_name());
    family_name_with_properties.emplace(family_name, properties);

    if (byte_code)
        if (!family_code_with_codec.emplace(*byte_code, creator).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "CompressionCodecFactory: the codec family code '{}' is not unique",
                            std::to_string(*byte_code));
}

void CompressionCodecFactory::registerCompressionCodec(
    const String & family_name,
    std::optional<uint8_t> byte_code,
    Creator creator,
    CompressionCodecFamilyProperties properties,
    std::source_location source)
{
    registerCompressionCodecWithType(family_name, byte_code, [family_name, creator](const ASTPtr & ast, const IDataType * /* data_type */)
    {
        return creator(ast);
    }, properties, source);
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
    }, {}, source);
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
    registerCodecGDeflate(*this);
#if USE_PCO
    registerCodecPco(*this);
#endif

    default_codec = get("ZSTD", 3);
}

CompressionCodecFactory & CompressionCodecFactory::instance()
{
    static CompressionCodecFactory ret;
    return ret;
}

size_t roundCompressBlockSizeToWholeValues(size_t block_size, const IDataType & type)
{
    if (!type.isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        return block_size;
    const size_t value_size = type.getSizeOfValueInMemory();
    return std::max(value_size, block_size - block_size % value_size);
}

}
