#include <Storages/ColumnCodecAST.h>

#include <Compression/CompressionCodecQuantized.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTTupleElementCodecOperation.h>
#include <Storages/ColumnCodecValidation.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <functional>
#include <string_view>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

using TupleElementVisitor = std::function<void(
    ASTTupleDataType &,
    size_t,
    const ASTTupleElementCodecOperation *,
    const DataTypePtr &,
    const CodecPath &)>;

/// Return a datatype AST, including specialized Tuple and Enum nodes.
ASTDataType & getDataTypeAST(const ASTPtr & ast, std::string_view context)
{
    if (auto * data_type = ast ? dynamic_cast<ASTDataType *>(ast.get()) : nullptr)
        return *data_type;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is not a data type AST", context);
}

/// Return the only datatype argument of a transparent one-argument wrapper.
const ASTPtr & getOnlyTypeArgument(const ASTDataType & ast, std::string_view wrapper)
{
    const auto arguments = ast.getArguments();
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} AST does not have one type argument", wrapper);
    getDataTypeAST(arguments->children[0], wrapper);
    return arguments->children[0];
}

/// Return the storage datatype argument of SimpleAggregateFunction(function, type).
const ASTPtr & getSimpleAggregateFunctionStorageTypeAST(const ASTDataType & ast)
{
    const auto arguments = ast.getArguments();
    if (!arguments || arguments->children.size() < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SimpleAggregateFunction AST has no storage type argument");
    getDataTypeAST(arguments->children[1], "SimpleAggregateFunction storage type");
    return arguments->children[1];
}

/// Remove one transparent codec wrapper. Nested is deliberately not transparent.
DataTypePtr unwrapTransparentCodecWrapper(const DataTypePtr & type)
{
    /// Nested uses DataTypeArray too, but its named fields are separate columns.
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()); array && !isNested(type))
        return array->getNestedType();
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        return nullable->getNestedType();
    return {};
}

/// Visit Tuple slots through the supported transparent wrappers.
void forEachTupleElement(
    const ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    const TupleElementVisitor & visitor)
{
    if (auto * tuple_ast = type_ast->as<ASTTupleDataType>())
    {
        const auto codec_operations = tuple_ast->getCodecOperationsByElement();
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
            const auto & element_type = tuple_type->getElements()[i];
            path.push_back(tuple_type->getNameByPosition(i + 1));
            const auto * operation = codec_operations.empty() ? nullptr : codec_operations[i];
            visitor(*tuple_ast, i, operation, element_type, path);
            forEachTupleElement(arguments->children[i], element_type, path, visitor);
            path.pop_back();
        }
        return;
    }

    auto & data_type_ast = getDataTypeAST(type_ast, "Codec declaration type");
    if (data_type_ast.name == "Array")
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(logical_type.get());
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array AST corresponds to non-Array type {}", logical_type->getName());
        forEachTupleElement(getOnlyTypeArgument(data_type_ast, "Array"), array_type->getNestedType(), path, visitor);
    }
    else if (data_type_ast.name == "Nullable")
    {
        const auto * nullable_type = typeid_cast<const DataTypeNullable *>(logical_type.get());
        if (!nullable_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nullable AST corresponds to non-Nullable type {}", logical_type->getName());
        forEachTupleElement(getOnlyTypeArgument(data_type_ast, "Nullable"), nullable_type->getNestedType(), path, visitor);
    }
    else if (data_type_ast.name == "SimpleAggregateFunction")
    {
        if (!typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(logical_type->getCustomName()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "SimpleAggregateFunction AST corresponds to type without SimpleAggregateFunction custom name: {}",
                logical_type->getName());
        forEachTupleElement(getSimpleAggregateFunctionStorageTypeAST(data_type_ast), logical_type, path, visitor);
    }
}

/// Count all operations, including those hidden below an unsupported wrapper.
size_t countTupleElementCodecOperations(const ASTPtr & ast)
{
    if (!ast)
        return 0;
    size_t count = ast->as<ASTTupleElementCodecOperation>() ? 1 : 0;
    for (const auto & child : ast->children)
        count += countTupleElementCodecOperations(child);
    return count;
}

/// Recreate sparse Tuple operations from a complete stored policy.
void installTupleCodecs(
    ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    const ColumnCodecDescription & codec)
{
    std::unordered_map<ASTTupleDataType *, ASTs> operations_by_tuple;
    forEachTupleElement(
        type_ast,
        logical_type,
        path,
        [&](
            ASTTupleDataType & tuple_ast,
            size_t element_index,
            const ASTTupleElementCodecOperation *,
            const DataTypePtr &,
            const CodecPath & element_path)
        {
            auto & operations = operations_by_tuple[&tuple_ast];
            if (auto it = codec.getCodecs().find(element_path); it != codec.getCodecs().end())
            {
                auto operation = make_intrusive<ASTTupleElementCodecOperation>();
                operation->element_index = element_index;
                operation->kind = TupleElementCodecOperationKind::Set;
                operation->children.push_back(it->second->clone());
                operations.push_back(std::move(operation));
            }
        });

    for (auto & [tuple_ast, operations] : operations_by_tuple)
        tuple_ast->setCodecOperations(std::move(operations));
}

}

CodecPath canonicalizeCodecPath(const DataTypePtr & root_type, const CodecPath & input)
{
    DataTypePtr current = root_type;
    CodecPath result;
    for (const auto & segment : input)
    {
        while (auto nested = unwrapTransparentCodecWrapper(current))
            current = std::move(nested);

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

ColumnCodecPatch tupleElementCodecPatchFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type)
{
    ColumnCodecPatch result;
    if (!declaration.getType())
        return result;
    CodecPath path;
    forEachTupleElement(
        declaration.getType(),
        logical_type,
        path,
        [&](
            ASTTupleDataType &,
            size_t,
            const ASTTupleElementCodecOperation * operation,
            const DataTypePtr &,
            const CodecPath & element_path)
        {
            if (!operation)
                return;

            if (operation->kind == TupleElementCodecOperationKind::Remove)
            {
                if (!result.emplace(element_path, ColumnCodecPatchOperation{ColumnCodecPatchKind::Remove, nullptr}).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate CODEC operation for Tuple element");
                return;
            }

            const auto & element_codec = operation->getCodec();
            if (tryExtractQuantizedCodecParams(element_codec))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Quantized codec on Tuple elements is not supported yet because its custom serialization must be path-aware");
            if (!result.emplace(element_path, ColumnCodecPatchOperation{ColumnCodecPatchKind::Set, element_codec}).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate CODEC operation for Tuple element");
        });

    if (countTupleElementCodecOperations(declaration.getType()) != result.size())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Tuple element codec operations through this wrapper type are not supported");
    return result;
}

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings)
{
    return codecDescriptionFromAST(declaration, logical_type, logical_type, settings);
}

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & declared_type,
    const DataTypePtr & resulting_type,
    const CodecValidationSettings & settings)
{
    ColumnCodecDescription result;
    if (auto root = declaration.getCodec())
        result.setRoot(root);

    for (const auto & [path, operation] : tupleElementCodecPatchFromAST(declaration, declared_type))
    {
        if (operation.kind == ColumnCodecPatchKind::Remove)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "REMOVE CODEC on a Tuple element is allowed only in ALTER TABLE ... MODIFY COLUMN");
        result.set(path, operation.codec);
    }
    return validateColumnCodecDescription(result, resulting_type, settings);
}

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & codec)
{
    if (codec.hasRoot())
        declaration.setCodec(codec.getRoot()->clone());
    ASTPtr type_ast = declaration.getType();
    if (!type_ast)
        return;
    CodecPath path;
    installTupleCodecs(type_ast, logical_type, path, codec);
}

}
