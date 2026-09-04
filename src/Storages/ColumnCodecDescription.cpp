#include <Storages/ColumnCodecDescription.h>

#include <Compression/CompressionCodecQuantized.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTupleDataType.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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

ASTDataType & getDataTypeAST(const ASTPtr & ast, std::string_view context)
{
    if (auto * data_type = ast ? ast->as<ASTDataType>() : nullptr)
        return *data_type;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not a data type AST", context);
}

const ASTPtr & getOnlyTypeArgument(const ASTDataType & ast, std::string_view wrapper)
{
    const auto arguments = ast.getArguments();
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} AST does not have one type argument", wrapper);
    getDataTypeAST(arguments->children[0], wrapper);
    return arguments->children[0];
}

const ASTPtr & getSimpleAggregateFunctionStorageTypeAST(const ASTDataType & ast)
{
    const auto arguments = ast.getArguments();
    if (!arguments || arguments->children.size() < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SimpleAggregateFunction AST has no storage type argument");
    getDataTypeAST(arguments->children[1], "SimpleAggregateFunction storage type");
    return arguments->children[1];
}

DataTypePtr getCodecTransparentNestedType(const DataTypePtr & type)
{
    /// Nested also uses DataTypeArray, but its named fields are separate columns and are not transparent here.
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()); array && !isNested(type))
        return array->getNestedType();
    return {};
}

DataTypePtr unwrapCodecTransparentTypes(DataTypePtr type)
{
    while (auto nested = getCodecTransparentNestedType(type))
        type = std::move(nested);
    return type;
}

/// Normalize each path segment using the element name from the logical Tuple type.
CodecPath canonicalizeCodecPath(const DataTypePtr & root_type, const CodecPath & input)
{
    DataTypePtr current = root_type;
    CodecPath result;
    for (const auto & segment : input)
    {
        current = unwrapCodecTransparentTypes(std::move(current));
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

void forEachTupleElementInCodecType(
    const ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    const TupleCodecElementVisitor & visitor)
{
    auto & data_type_ast = getDataTypeAST(type_ast, "Codec declaration type");

    if (const auto * tuple_ast = type_ast->as<ASTTupleDataType>())
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(logical_type.get());
        if (!tuple_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tuple AST corresponds to non-Tuple type {}", logical_type->getName());

        const auto arguments = tuple_ast->getArguments();
        const size_t argument_count = arguments ? arguments->children.size() : 0;
        if (argument_count != tuple_type->getElements().size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Tuple AST has {} elements but logical type {} has {}",
                argument_count,
                logical_type->getName(),
                tuple_type->getElements().size());

        for (size_t i = 0; i < argument_count; ++i)
        {
            auto & element_ast = getDataTypeAST(arguments->children[i], "Tuple element");
            const auto & element_type = tuple_type->getElements()[i];
            path.push_back(tuple_type->getNameByPosition(i + 1));
            visitor(element_ast, element_type, path);
            forEachTupleElementInCodecType(arguments->children[i], element_type, path, visitor);
            path.pop_back();
        }
        return;
    }

    if (data_type_ast.name == "Array")
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(logical_type.get());
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array AST corresponds to non-Array type {}", logical_type->getName());
        forEachTupleElementInCodecType(
            getOnlyTypeArgument(data_type_ast, "Array"), array_type->getNestedType(), path, visitor);
        return;
    }

    if (data_type_ast.name == "SimpleAggregateFunction")
    {
        if (!typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(logical_type->getCustomName()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "SimpleAggregateFunction AST corresponds to type without SimpleAggregateFunction custom name: {}",
                logical_type->getName());
        /// The first syntax argument is the function. The second is the type used for storage.
        forEachTupleElementInCodecType(
            getSimpleAggregateFunctionStorageTypeAST(data_type_ast), logical_type, path, visitor);
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
/// Collect declarations from every supported Tuple element.
void collectTupleCodecs(
    const ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    ColumnCodecDescription & result)
{
    forEachTupleElementInCodecType(
        type_ast,
        logical_type,
        path,
        [&](ASTDataType & element_ast, const DataTypePtr &, const CodecPath & element_path)
        {
            const auto element_codec = element_ast.getCodec();
            if (!element_codec)
                return;
            if (tryExtractQuantizedCodecParams(element_codec))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Quantized codec on tuple subcolumns is not supported yet because its custom serialization must be path-aware");
            if (result.getCodecs().contains(element_path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate codec declaration for tuple subcolumn");
            result.set(element_path, element_codec);
        });
}

/// Count annotations through every AST child so unsupported wrappers cannot hide a declaration.
size_t countTupleCodecAnnotations(const ASTPtr & type_ast)
{
    if (!type_ast)
        return 0;

    size_t count = 0;
    if (const auto * data_type = type_ast->as<ASTDataType>(); data_type && data_type->hasCodec())
        ++count;
    for (const auto & child : type_ast->children)
        count += countTupleCodecAnnotations(child);
    return count;
}

/// Count removals anywhere in the AST so persisted metadata can reject them.
size_t countTupleCodecRemovals(const ASTPtr & type_ast)
{
    if (!type_ast)
        return 0;

    size_t count = 0;
    if (const auto * data_type = type_ast->as<ASTDataType>(); data_type && data_type->hasCodecRemoval())
        ++count;
    for (const auto & child : type_ast->children)
        count += countTupleCodecRemovals(child);
    return count;
}

using DeclarationTypes = std::map<CodecPath, std::vector<DataTypePtr>>;

bool hasCodecDeclarationBelow(const ColumnCodecDescription & policy, const CodecPath & path)
{
    /// Descend through an Array only when a more specific declaration must be validated below it.
    for (const auto & entry : policy.getCodecs())
    {
        const auto & declaration_path = entry.first;
        if (declaration_path.size() > path.size()
            && std::equal(path.begin(), path.end(), declaration_path.begin()))
            return true;
    }
    return false;
}

/// Record the leaf types that each declaration controls after child overrides are applied.
/// A declaration is validated only against leaves where it is effective.
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

    if (auto nested = getCodecTransparentNestedType(type))
    {
        if (hasCodecDeclarationBelow(policy, path))
        {
            collectEffectiveDeclarationTypes(nested, path, policy, declaration_types);
            return;
        }
    }

    auto resolved = policy.resolve(path, nullptr);
    if (resolved.codec)
        declaration_types[resolved.declaration_path].push_back(type);
}

/// Normalize a complete policy. If a changed-codec map is provided, apply session settings only
/// to those paths. Other paths are retained metadata and use trusted settings.
ColumnCodecDescription validateEffectivePolicy(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings,
    const ColumnCodecDescription::CodecsByPath * declarations_to_admit = nullptr)
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
            || declarations_to_admit->contains(declaration_path);
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

/// Put tuple-element declarations back into a type AST for metadata and query formatting.
/// The column-level declaration is installed on ASTColumnDeclaration by the caller.
void installTupleCodecs(
    ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    const ColumnCodecDescription & codec)
{
    forEachTupleElementInCodecType(
        type_ast,
        logical_type,
        path,
        [&](ASTDataType & element_ast, const DataTypePtr &, const CodecPath & element_path)
        {
            element_ast.resetCodecOperation();
            if (auto it = codec.getCodecs().find(element_path); it != codec.getCodecs().end())
                element_ast.setCodec(it->second->clone());
        });
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
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
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
            "Tuple element codec declarations through this wrapper type are not supported");
    return validateEffectivePolicy(result, logical_type, settings);
}

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & codec)
{
    if (countTupleCodecRemovals(declaration.getType()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot format persisted codec metadata from an AST containing pending tuple element REMOVE CODEC operations");
    if (codec.hasRoot())
        declaration.setCodec(codec.getRoot()->clone());
    ASTPtr type_ast = declaration.getType();
    CodecPath path;
    installTupleCodecs(type_ast, logical_type, path, codec);
}

}
