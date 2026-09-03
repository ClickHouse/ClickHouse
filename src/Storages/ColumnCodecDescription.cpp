#include <Storages/ColumnCodecDescription.h>

#include <Compression/CompressionCodecQuantized.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTupleDataType.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

ColumnCodecDescription::ColumnCodecDescription(const ColumnCodecDescription & other)
{
    *this = other;
}

ColumnCodecDescription & ColumnCodecDescription::operator=(const ColumnCodecDescription & other)
{
    if (this == &other)
        return *this;
    codecs.clear();
    for (const auto & [path, codec] : other.codecs)
        codecs.emplace(path, codec->clone());
    return *this;
}

const ASTPtr & ColumnCodecDescription::getRoot() const
{
    static const ASTPtr null_codec;
    auto it = codecs.find(CodecPath{});
    return it == codecs.end() ? null_codec : it->second;
}

void ColumnCodecDescription::setRoot(const ASTPtr & ast)
{
    if (ast)
        codecs[CodecPath{}] = ast->clone();
    else
        resetRoot();
}

void ColumnCodecDescription::set(CodecPath path, const ASTPtr & ast)
{
    if (!ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A codec declaration requires a codec expression");
    codecs[std::move(path)] = ast->clone();
}

void ColumnCodecDescription::erase(const CodecPath & path)
{
    codecs.erase(path);
}

ColumnCodecDescription::Resolved ColumnCodecDescription::resolve(const CodecPath & logical_path, const ASTPtr & part_default) const
{
    CodecPath candidate = logical_path;
    while (true)
    {
        if (auto it = codecs.find(candidate); it != codecs.end())
        {
            bool use_default = CompressionCodecFactory::isDefaultCodec(it->second);
            return {use_default && part_default ? part_default : it->second, candidate, use_default};
        }
        if (candidate.empty())
            break;
        candidate.pop_back();
    }
    return {part_default, {}, true};
}

bool ColumnCodecDescription::operator==(const ColumnCodecDescription & rhs) const
{
    auto format = [](const ASTPtr & ast) { return ast ? ast->formatWithSecretsOneLine() : String{}; };
    if (codecs.size() != rhs.codecs.size())
        return false;
    auto lhs_it = codecs.begin();
    auto rhs_it = rhs.codecs.begin();
    for (; lhs_it != codecs.end(); ++lhs_it, ++rhs_it)
        if (lhs_it->first != rhs_it->first || format(lhs_it->second) != format(rhs_it->second))
            return false;
    return true;
}

namespace
{

CodecPath canonicalizeCodecPath(const DataTypePtr & root_type, const CodecPath & input)
{
    DataTypePtr current = root_type;
    CodecPath result;
    for (const auto & segment : input)
    {
        const auto * tuple = typeid_cast<const DataTypeTuple *>(current.get());
        if (!tuple)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec path reaches non-Tuple type {} before element '{}'", current->getName(), segment);
        auto position = tuple->tryGetPositionByName(segment);
        if (!position)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple type {} has no element '{}'", current->getName(), segment);
        result.push_back(tuple->getNameByPosition(*position + 1));
        current = tuple->getElements()[*position];
    }
    return result;
}

}

CodecPath getCodecPath(const ISerialization::SubstreamPath & path)
{
    CodecPath result;
    for (const auto & entry : path)
        if (entry.type == ISerialization::Substream::TupleElement)
            result.push_back(entry.name_of_substream);
    return result;
}

CodecPath getCodecPathForStream(
    const NameAndTypePair & written_column,
    const DataTypePtr & owning_type,
    const ISerialization::SubstreamPath & stream_path)
{
    CodecPath suffix = getCodecPath(stream_path);
    if (!written_column.isSubcolumn())
        return suffix;

    CodecPath prefix;
    bool found = false;
    IDataType::forEachSubcolumn(
        [&](const auto & path, const auto & name, const auto &)
        {
            if (name == written_column.getSubcolumnName())
            {
                prefix = getCodecPath(path);
                found = true;
            }
        },
        ISerialization::SubstreamData(owning_type->getDefaultSerialization()).withType(owning_type));
    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot resolve subcolumn {} in type {}", written_column.getSubcolumnName(), owning_type->getName());

    if (suffix.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), suffix.begin()))
        return suffix;
    prefix.insert(prefix.end(), suffix.begin(), suffix.end());
    return prefix;
}

namespace
{
void collectTupleCodecs(
    const ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    ColumnCodecDescription & result)
{
    const auto * tuple_ast = type_ast ? type_ast->as<ASTTupleDataType>() : nullptr;
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(logical_type.get());
    if (!tuple_ast || !tuple_type)
        return;
    auto arguments = tuple_ast->getArguments();
    if (!arguments)
        return;
    for (size_t i = 0; i < arguments->children.size(); ++i)
    {
        path.push_back(tuple_type->getNameByPosition(i + 1));
        const auto & subtype = tuple_type->getElements()[i];
        if (i < tuple_ast->element_codecs.size() && tuple_ast->element_codecs[i])
        {
            if (tryExtractQuantizedCodecParams(tuple_ast->element_codecs[i]))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Quantized codec on tuple subcolumns is not supported yet because its custom serialization must be path-aware");
            if (result.getCodecs().contains(path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for tuple subcolumn");
            result.set(path, tuple_ast->element_codecs[i]);
        }
        collectTupleCodecs(arguments->children[i], subtype, path, result);
        path.pop_back();
    }
}

size_t countTupleCodecAnnotations(const ASTPtr & type_ast)
{
    if (!type_ast)
        return 0;

    size_t count = 0;
    if (const auto * tuple = type_ast->as<ASTTupleDataType>())
        count += std::count_if(tuple->element_codecs.begin(), tuple->element_codecs.end(), [](const auto & codec) { return codec != nullptr; });
    /// Walk every AST child: Nested and typed JSON interpose ASTNameTypePair and
    /// ASTObjectTypeArgument nodes, which must not hide tuple codec annotations.
    for (const auto & child : type_ast->children)
        count += countTupleCodecAnnotations(child);
    return count;
}

size_t countTupleCodecRemovals(const ASTPtr & type_ast)
{
    if (!type_ast)
        return 0;

    size_t count = 0;
    if (const auto * tuple = type_ast->as<ASTTupleDataType>())
        count += std::count(tuple->element_codec_removals.begin(), tuple->element_codec_removals.end(), true);
    /// Walk every AST child: Nested and typed JSON interpose ASTNameTypePair and
    /// ASTObjectTypeArgument nodes, which must not hide tuple codec operations.
    for (const auto & child : type_ast->children)
        count += countTupleCodecRemovals(child);
    return count;
}

using DeclarationTypes = std::map<CodecPath, std::vector<DataTypePtr>>;

void collectEffectiveDeclarationTypes(
    const DataTypePtr & type,
    CodecPath & path,
    const ColumnCodecDescription & policy,
    DeclarationTypes & declaration_types)
{
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (size_t i = 0; i < tuple->getElements().size(); ++i)
        {
            path.push_back(tuple->getNameByPosition(i + 1));
            collectEffectiveDeclarationTypes(tuple->getElements()[i], path, policy, declaration_types);
            path.pop_back();
        }
        return;
    }

    auto resolved = policy.resolve(path, nullptr);
    if (resolved.codec)
        declaration_types[resolved.declaration_path].push_back(type);
}

ColumnCodecDescription validateEffectivePolicy(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings,
    const ColumnCodecDescription * declarations_to_admit = nullptr)
{
    ColumnCodecDescription canonical_policy;
    for (const auto & [declaration_path, codec] : policy.getCodecs())
    {
        auto canonical_path = declaration_path.empty() ? CodecPath{} : canonicalizeCodecPath(logical_type, declaration_path);
        if (canonical_policy.getCodecs().contains(canonical_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for tuple subcolumn");
        canonical_policy.set(std::move(canonical_path), codec);
    }

    DeclarationTypes declaration_types;
    CodecPath path;
    collectEffectiveDeclarationTypes(logical_type, path, canonical_policy, declaration_types);

    ColumnCodecDescription normalized;
    const auto trusted_settings = CodecValidationSettings::trusted();
    const auto validate_declaration = [&](const CodecPath & declaration_path, const ASTPtr & ast)
    {
        const bool use_session_settings = !declarations_to_admit
            || declarations_to_admit->getCodecs().contains(declaration_path);
        const auto & declaration_settings = use_session_settings ? settings : trusted_settings;
        auto types_it = declaration_types.find(declaration_path);
        if (types_it == declaration_types.end() || types_it->second.empty())
        {
            CompressionCodecFactory::instance().validateCodecDeclaration(ast, declaration_settings);
            normalized.set(declaration_path, ast);
            return;
        }

        ASTPtr common_normalized;
        bool all_normalized_equal = true;
        for (const auto & type : types_it->second)
        {
            auto candidate = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(ast, type, declaration_settings);
            if (!common_normalized)
                common_normalized = candidate;
            else if (common_normalized->formatWithSecretsOneLine() != candidate->formatWithSecretsOneLine())
                all_normalized_equal = false;
        }

        ASTPtr stored = all_normalized_equal ? common_normalized : ast;
        normalized.set(declaration_path, stored);
    };

    for (const auto & [declaration_path, codec] : canonical_policy.getCodecs())
        validate_declaration(declaration_path, codec);
    return normalized;
}

void installTupleCodecs(ASTPtr & type_ast, CodecPath & path, const ColumnCodecDescription & codec)
{
    auto * tuple = type_ast ? type_ast->as<ASTTupleDataType>() : nullptr;
    if (!tuple)
        return;
    auto arguments = tuple->getArguments();
    if (!arguments)
        return;
    tuple->element_codecs.assign(arguments->children.size(), nullptr);
    tuple->element_codec_removals.clear();
    for (size_t i = 0; i < arguments->children.size(); ++i)
    {
        const String segment = tuple->element_names.empty() ? std::to_string(i + 1) : tuple->element_names[i];
        path.push_back(segment);
        if (auto it = codec.getCodecs().find(path); it != codec.getCodecs().end())
            tuple->element_codecs[i] = it->second->clone();
        installTupleCodecs(arguments->children[i], path, codec);
        path.pop_back();
    }
}
}

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    return validateEffectivePolicy(policy, logical_type, settings);
}

ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & declarations_to_admit,
    const CodecValidationSettings & settings)
{
    return validateEffectivePolicy(policy, logical_type, settings, &declarations_to_admit);
}

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    if (countTupleCodecRemovals(declaration.getType()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "REMOVE CODEC on a tuple element is allowed only in ALTER TABLE ... MODIFY COLUMN");
    ColumnCodecDescription result;
    if (auto root = declaration.getCodec())
        result.setRoot(root);
    CodecPath path;
    collectTupleCodecs(declaration.getType(), logical_type, path, result);
    if (countTupleCodecAnnotations(declaration.getType())
        != result.getCodecs().size() - static_cast<size_t>(result.hasRoot()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Tuple element codec declarations through non-Tuple wrapper types are not supported");
    return validateEffectivePolicy(result, logical_type, settings);
}

void applyCodecDescriptionToAST(ASTColumnDeclaration & declaration, const ColumnCodecDescription & codec)
{
    if (countTupleCodecRemovals(declaration.getType()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot format persisted codec metadata from an AST containing pending tuple element REMOVE CODEC operations");
    if (codec.hasRoot())
        declaration.setCodec(codec.getRoot()->clone());
    ASTPtr type_ast = declaration.getType();
    CodecPath path;
    installTupleCodecs(type_ast, path, codec);
}

}
