#include <Compression/CompressionCodecFirstApplicable.h>

#include <Compression/CompressionFactory.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isFirstApplicableName(const String & name)
{
    return equalsCaseInsensitive(name, FIRST_APPLICABLE_CODEC_NAME);
}

bool isMultipleName(const String & name)
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

/// True if the alternative `element` is applicable to `type`. Throws if it is malformed.
bool isApplicable(const ASTPtr & element, const IDataType * type)
{
    const auto [name, arguments] = CompressionCodecFactory::getCodecNameAndArguments(element);

    /// `Default` is resolved for the data type by itself.
    if (equalsCaseInsensitive(name, DEFAULT_CODEC_NAME))
        return true;

    if (isMultipleName(name))
        return std::ranges::all_of(getCombinatorArguments(name, arguments), [&](const auto & codec) { return isApplicable(codec, type); });

    if (isFirstApplicableName(name))
        return std::ranges::any_of(getCombinatorArguments(name, arguments), [&](const auto & codec) { return isApplicable(codec, type); });

    const auto & properties = CompressionCodecFactory::instance().getFamilyProperties(name);
    /// A declarative codec takes effect only through the serialization of the column, which is chosen from the codecs
    /// of the column declaration, not from the alternatives of `FirstApplicable`.
    if (properties.is_declarative)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec {} cannot be used inside {}", name, FIRST_APPLICABLE_CODEC_NAME);

    if (!properties.is_applicable_to_type)
        return true;
    return type && properties.is_applicable_to_type(*type);
}

/// Appends the codecs that the chain element `element` stands for with the data type `type` to `out`:
/// the codec itself, the codecs of a `Multiple`, or the ones of the alternative selected by a `FirstApplicable`.
void appendResolvedChainElement(const ASTPtr & element, const IDataType * type, ASTs & out)
{
    const auto [name, arguments] = CompressionCodecFactory::getCodecNameAndArguments(element);

    if (isMultipleName(name))
    {
        for (const auto & codec : getCombinatorArguments(name, arguments))
            appendResolvedChainElement(codec, type, out);
        return;
    }

    if (isFirstApplicableName(name))
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

/// A copy of the chain element `element` with every `Default` inside replaced by the description of `current_default`.
ASTPtr replaceDefaultCodec(const ASTPtr & element, const CompressionCodecPtr & current_default)
{
    const auto [name, arguments] = CompressionCodecFactory::getCodecNameAndArguments(element);
    if (equalsCaseInsensitive(name, DEFAULT_CODEC_NAME))
    {
        ASTPtr description = current_default->getCodecDescription();
        /// The description of a chain is a list of codecs, which is `Multiple` as an alternative of `FirstApplicable`.
        if (const auto * list = description->as<ASTExpressionList>())
            return makeASTFunction("Multiple", ASTs(list->children.begin(), list->children.end()));
        return description;
    }

    if (!arguments || !(isFirstApplicableName(name) || isMultipleName(name)))
        return element->clone();

    ASTs replaced_arguments;
    replaced_arguments.reserve(arguments->children.size());
    for (const auto & argument : arguments->children)
        replaced_arguments.push_back(replaceDefaultCodec(argument, current_default));
    return makeASTFunction(name, std::move(replaced_arguments));
}

}

CompressionCodecFirstApplicable::CompressionCodecFirstApplicable(ASTPtr description_, CompressionCodecPtr selected_)
    : description(std::move(description_))
    , selected(std::move(selected_))
{
    if (!description || !selected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecFirstApplicable requires a description and a selected codec");
}

CompressionCodecPtr CompressionCodecFirstApplicable::create(
    const ASTPtr & element, const IDataType * column_type, const CompressionCodecPtr & current_default, bool only_generic)
{
    if (only_generic && !column_type && !isApplicable(element, nullptr))
        return nullptr;

    ASTs selected_codecs;
    appendResolvedChainElement(element, column_type, selected_codecs);
    /// The caller keeps only the generic codecs of the selected chain for `only_generic`.
    CompressionCodecPtr selected_codec = CompressionCodecFactory::instance().get(
        makeASTFunction("CODEC", std::move(selected_codecs)), column_type, current_default);

    if (column_type || only_generic)
        return selected_codec;
    return std::shared_ptr<CompressionCodecFirstApplicable>(
        new CompressionCodecFirstApplicable(replaceDefaultCodec(element, current_default), std::move(selected_codec)));
}

bool CompressionCodecFirstApplicable::isFirstApplicable(const ASTPtr & element)
{
    /// Not `getCodecNameAndArguments`: a malformed element is reported by the validation of the chain.
    if (const auto * identifier = element->as<ASTIdentifier>())
        return isFirstApplicableName(identifier->name());
    if (const auto * function = element->as<ASTFunction>())
        return isFirstApplicableName(function->name);
    return false;
}

bool CompressionCodecFirstApplicable::isInChain(const ASTPtr & codec)
{
    const auto * func = codec ? codec->as<ASTFunction>() : nullptr;
    if (!func || !func->arguments)
        return false;

    return std::ranges::any_of(func->arguments->children, isFirstApplicable);
}

ASTPtr CompressionCodecFirstApplicable::resolveInChain(const ASTPtr & ast, const IDataType * type)
{
    if (!isInChain(ast))
        return ast;

    ASTs resolved;
    for (const auto & element : ast->as<const ASTFunction &>().arguments->children)
    {
        if (isFirstApplicable(element))
            appendResolvedChainElement(element, type, resolved);
        else
            resolved.push_back(element);
    }
    return makeASTFunction("CODEC", std::move(resolved));
}

ASTPtr CompressionCodecFirstApplicable::getCodecDescription() const
{
    return description->clone();
}

void CompressionCodecFirstApplicable::updateHash(SipHash & hash) const
{
    /// The description determines both the selection without a type and the selections with a type.
    hash.update(description->formatWithSecretsOneLine());
}

}
