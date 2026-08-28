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
    root = other.root ? other.root->clone() : nullptr;
    subcolumns.clear();
    for (const auto & [path, ast] : other.subcolumns)
        subcolumns.emplace(path, ast->clone());
    return *this;
}

void ColumnCodecDescription::setRoot(const ASTPtr & ast)
{
    root = ast ? ast->clone() : nullptr;
}

void ColumnCodecDescription::set(CodecPath path, const ASTPtr & ast)
{
    if (path.empty() || !ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A subcolumn codec requires a non-empty path and codec expression");
    subcolumns[std::move(path)] = ast->clone();
}

void ColumnCodecDescription::erase(const CodecPath & path)
{
    subcolumns.erase(path);
}

ColumnCodecDescription::Resolved ColumnCodecDescription::resolve(const CodecPath & logical_path, const ASTPtr & part_default) const
{
    for (size_t size = logical_path.size(); size; --size)
    {
        CodecPath candidate(logical_path.begin(), logical_path.begin() + size);
        if (auto it = subcolumns.find(candidate); it != subcolumns.end())
        {
            bool use_default = CompressionCodecFactory::isDefaultCodec(it->second);
            return {use_default && part_default ? part_default : it->second, std::move(candidate), use_default};
        }
    }
    if (root)
    {
        bool use_default = CompressionCodecFactory::isDefaultCodec(root);
        return {use_default && part_default ? part_default : root, {}, use_default};
    }
    return {part_default, {}, true};
}

bool ColumnCodecDescription::operator==(const ColumnCodecDescription & rhs) const
{
    auto format = [](const ASTPtr & ast) { return ast ? ast->formatWithSecretsOneLine() : String{}; };
    if (format(root) != format(rhs.root) || subcolumns.size() != rhs.subcolumns.size())
        return false;
    auto lhs_it = subcolumns.begin();
    auto rhs_it = rhs.subcolumns.begin();
    for (; lhs_it != subcolumns.end(); ++lhs_it, ++rhs_it)
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
            if (result.getSubcolumns().contains(path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for tuple subcolumn");
            result.set(path, tuple_ast->element_codecs[i]);
        }
        collectTupleCodecs(arguments->children[i], subtype, path, result);
        path.pop_back();
    }
}

size_t countTupleCodecAnnotations(const ASTPtr & type_ast)
{
    const auto * data_type = type_ast ? type_ast->as<ASTDataType>() : nullptr;
    if (!data_type)
        return 0;
    size_t count = 0;
    if (const auto * tuple = type_ast->as<ASTTupleDataType>())
        count += std::count_if(tuple->element_codecs.begin(), tuple->element_codecs.end(), [](const auto & codec) { return codec != nullptr; });
    if (auto arguments = data_type->getArguments())
        for (const auto & argument : arguments->children)
            count += countTupleCodecAnnotations(argument);
    return count;
}

size_t countTupleCodecRemovals(const ASTPtr & type_ast)
{
    const auto * data_type = type_ast ? type_ast->as<ASTDataType>() : nullptr;
    if (!data_type)
        return 0;
    size_t count = 0;
    if (const auto * tuple = type_ast->as<ASTTupleDataType>())
        count += std::count(tuple->element_codec_removals.begin(), tuple->element_codec_removals.end(), true);
    if (auto arguments = data_type->getArguments())
        for (const auto & argument : arguments->children)
            count += countTupleCodecRemovals(argument);
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
    if (resolved.ast)
        declaration_types[resolved.declaration_path].push_back(type);
}

ColumnCodecDescription validateEffectivePolicy(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    ColumnCodecDescription canonical_policy;
    if (policy.hasRoot())
        canonical_policy.setRoot(policy.getRoot());
    for (const auto & [declaration_path, ast] : policy.getSubcolumns())
    {
        auto canonical_path = canonicalizeCodecPath(logical_type, declaration_path);
        if (canonical_policy.getSubcolumns().contains(canonical_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for tuple subcolumn");
        canonical_policy.set(std::move(canonical_path), ast);
    }

    DeclarationTypes declaration_types;
    CodecPath path;
    collectEffectiveDeclarationTypes(logical_type, path, canonical_policy, declaration_types);

    ColumnCodecDescription normalized;
    const auto validate_declaration = [&](const CodecPath & declaration_path, const ASTPtr & ast)
    {
        auto types_it = declaration_types.find(declaration_path);
        if (types_it == declaration_types.end() || types_it->second.empty())
        {
            CompressionCodecFactory::instance().validateCodecDeclaration(ast, settings);
            if (declaration_path.empty())
                normalized.setRoot(ast);
            else
                normalized.set(declaration_path, ast);
            return;
        }

        ASTPtr common_normalized;
        bool all_normalized_equal = true;
        for (const auto & type : types_it->second)
        {
            auto candidate = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(ast, type, settings);
            if (!common_normalized)
                common_normalized = candidate;
            else if (common_normalized->formatWithSecretsOneLine() != candidate->formatWithSecretsOneLine())
                all_normalized_equal = false;
        }

        ASTPtr stored = all_normalized_equal ? common_normalized : ast;
        if (declaration_path.empty())
            normalized.setRoot(stored);
        else
            normalized.set(declaration_path, stored);
    };

    if (canonical_policy.hasRoot())
        validate_declaration({}, canonical_policy.getRoot());
    for (const auto & [declaration_path, ast] : canonical_policy.getSubcolumns())
        validate_declaration(declaration_path, ast);
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
        if (auto it = codec.getSubcolumns().find(path); it != codec.getSubcolumns().end())
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
    if (countTupleCodecAnnotations(declaration.getType()) != result.getSubcolumns().size())
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
