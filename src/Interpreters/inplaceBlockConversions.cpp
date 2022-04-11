#include "inplaceBlockConversions.h"

#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <utility>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Common/checkStackSize.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

namespace ErrorCode
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Add all required expressions for missing columns calculation
void addDefaultRequiredExpressionsRecursively(
    const Block & block, const String & required_column_name, DataTypePtr required_column_type,
    const ColumnsDescription & columns, ASTPtr default_expr_list_accum, NameSet & added_columns, bool null_as_default)
{
    checkStackSize();

    bool is_column_in_query = block.has(required_column_name);
    bool convert_null_to_default = false;

    if (is_column_in_query)
        convert_null_to_default = null_as_default && block.findByName(required_column_name)->type->isNullable() && !required_column_type->isNullable();

    if ((is_column_in_query && !convert_null_to_default) || added_columns.count(required_column_name))
        return;

    auto column_default = columns.getDefault(required_column_name);

    if (column_default)
    {
        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        auto column_default_expr = column_default->expression->clone();

        /// Our default may depend on columns with default expr which not present in block
        /// we have to add them to block too
        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(column_default_expr);
        NameSet required_columns_names = columns_context.requiredColumns();

        auto expr = makeASTFunction("_CAST", column_default_expr, std::make_shared<ASTLiteral>(columns.get(required_column_name).type->getName()));

        if (is_column_in_query && convert_null_to_default)
            expr = makeASTFunction("ifNull", std::make_shared<ASTIdentifier>(required_column_name), std::move(expr));
        default_expr_list_accum->children.emplace_back(setAlias(expr, required_column_name));

        added_columns.emplace(required_column_name);

        for (const auto & next_required_column_name : required_columns_names)
        {
            /// Required columns of the default expression should not be converted to NULL,
            /// since this map value to default and MATERIALIZED values will not work.
            ///
            /// Consider the following structure:
            /// - A Nullable(Int64)
            /// - X Int64 materialized coalesce(A, -1)
            ///
            /// With recursive_null_as_default=true you will get:
            ///
            ///     _CAST(coalesce(A, -1), 'Int64') AS X, NULL AS A
            ///
            /// And this will ignore default expression.
            bool recursive_null_as_default = false;
            addDefaultRequiredExpressionsRecursively(block,
                next_required_column_name, required_column_type,
                columns, default_expr_list_accum, added_columns,
                recursive_null_as_default);
        }
    }
    else if (columns.has(required_column_name))
    {
        /// In case of dictGet function we allow to use it with identifier dictGet(identifier, 'column_name', key_expression)
        /// and this identifier will be in required columns. If such column is not in ColumnsDescription we ignore it.

        /// This column is required, but doesn't have default expression, so lets use "default default"
        auto column = columns.get(required_column_name);
        auto default_value = column.type->getDefault();
        auto default_ast = std::make_shared<ASTLiteral>(default_value);
        default_expr_list_accum->children.emplace_back(setAlias(default_ast, required_column_name));
        added_columns.emplace(required_column_name);
    }
}

ASTPtr defaultRequiredExpressions(const Block & block, const NamesAndTypesList & required_columns, const ColumnsDescription & columns, bool null_as_default)
{
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    NameSet added_columns;
    for (const auto & column : required_columns)
        addDefaultRequiredExpressionsRecursively(block, column.name, column.type, columns, default_expr_list, added_columns, null_as_default);

    if (default_expr_list->children.empty())
        return nullptr;

    return default_expr_list;
}

ASTPtr convertRequiredExpressions(Block & block, const NamesAndTypesList & required_columns)
{
    ASTPtr conversion_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & required_column : required_columns)
    {
        if (!block.has(required_column.name))
            continue;

        auto column_in_block = block.getByName(required_column.name);
        if (column_in_block.type->equals(*required_column.type))
            continue;

        auto cast_func = makeASTFunction(
            "_CAST", std::make_shared<ASTIdentifier>(required_column.name), std::make_shared<ASTLiteral>(required_column.type->getName()));

        conversion_expr_list->children.emplace_back(setAlias(cast_func, required_column.name));

    }
    return conversion_expr_list;
}

ActionsDAGPtr createExpressions(
    const Block & header,
    ASTPtr expr_list,
    bool save_unneeded_columns,
    ContextPtr context)
{
    if (!expr_list)
        return nullptr;

    auto syntax_result = TreeRewriter(context).analyze(expr_list, header.getNamesAndTypesList());
    auto expression_analyzer = ExpressionAnalyzer{expr_list, syntax_result, context};
    auto dag = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    auto actions = expression_analyzer.getActionsDAG(true, !save_unneeded_columns);
    dag = ActionsDAG::merge(std::move(*dag), std::move(*actions));

    return dag;
}

}

void performRequiredConversions(Block & block, const NamesAndTypesList & required_columns, ContextPtr context)
{
    ASTPtr conversion_expr_list = convertRequiredExpressions(block, required_columns);
    if (conversion_expr_list->children.empty())
        return;

    if (auto dag = createExpressions(block, conversion_expr_list, true, context))
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings::fromContext(context));
        expression->execute(block);
    }
}

ActionsDAGPtr evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns,
    bool null_as_default)
{
    if (!columns.hasDefaults())
        return nullptr;

    ASTPtr expr_list = defaultRequiredExpressions(header, required_columns, columns, null_as_default);
    return createExpressions(header, expr_list, save_unneeded_columns, context);
}

static bool arrayHasNoElementsRead(const IColumn & column)
{
    const auto * column_array = typeid_cast<const ColumnArray *>(&column);

    if (!column_array)
        return false;

    size_t size = column_array->size();
    if (!size)
        return false;

    size_t data_size = column_array->getData().size();
    if (data_size)
        return false;

    size_t last_offset = column_array->getOffsets()[size - 1];
    return last_offset != 0;
}

void fillMissingColumns(
    Columns & res_columns,
    size_t num_rows,
    const NamesAndTypesList & requested_columns,
    StorageMetadataPtr metadata_snapshot)
{
    size_t num_columns = requested_columns.size();
    if (num_columns != res_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to fillMissingColumns. Expected {}, got {}",
            num_columns, res_columns.size());

    /// For a missing column of a nested data structure we must create not a column of empty
    /// arrays, but a column of arrays of correct length.

    /// First, collect offset columns for all arrays in the block.

    std::unordered_map<String, ColumnPtr> offset_columns;
    auto requested_column = requested_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++requested_column)
    {
        if (res_columns[i] == nullptr)
            continue;

        if (const auto * array = typeid_cast<const ColumnArray *>(res_columns[i].get()))
        {
            String offsets_name = Nested::extractTableName(requested_column->name);
            auto & offsets_column = offset_columns[offsets_name];

            /// If for some reason multiple offsets columns are present for the same nested data structure,
            /// choose the one that is not empty.
            if (!offsets_column || offsets_column->empty())
                offsets_column = array->getOffsetsPtr();
        }
    }

    /// insert default values only for columns without default expressions
    requested_column = requested_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++requested_column)
    {
        const auto & [name, type] = *requested_column;

        if (res_columns[i] && arrayHasNoElementsRead(*res_columns[i]))
            res_columns[i] = nullptr;

        if (res_columns[i] == nullptr)
        {
            if (metadata_snapshot && metadata_snapshot->getColumns().hasDefault(name))
                continue;

            String offsets_name = Nested::extractTableName(name);
            auto offset_it = offset_columns.find(offsets_name);
            const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
            if (offset_it != offset_columns.end() && array_type)
            {
                const auto & nested_type = array_type->getNestedType();
                ColumnPtr offsets_column = offset_it->second;
                size_t nested_rows = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                ColumnPtr nested_column =
                    nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                res_columns[i] = ColumnArray::create(nested_column, offsets_column);
            }
            else
            {
                /// We must turn a constant column into a full column because the interpreter could infer
                /// that it is constant everywhere but in some blocks (from other parts) it can be a full column.
                res_columns[i] = type->createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();
            }
        }
    }
}

}
