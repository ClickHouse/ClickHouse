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
#include <DataTypes/ObjectUtils.h>
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
        convert_null_to_default = null_as_default && isNullableOrLowCardinalityNullable(block.findByName(required_column_name)->type) && !isNullableOrLowCardinalityNullable(required_column_type);

    if ((is_column_in_query && !convert_null_to_default) || added_columns.contains(required_column_name))
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
        auto required_type = std::make_shared<ASTLiteral>(columns.get(required_column_name).type->getName());

        auto expr = makeASTFunction("_CAST", column_default_expr, required_type);

        if (is_column_in_query && convert_null_to_default)
        {
            expr = makeASTFunction("ifNull", std::make_shared<ASTIdentifier>(required_column_name), std::move(expr));
            /// ifNull does not respect LowCardinality.
            /// It may be fixed later or re-implemented properly for identical types.
            expr = makeASTFunction("_CAST", std::move(expr), required_type);
        }
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
        ASTPtr expr = std::make_shared<ASTLiteral>(default_value);
        if (is_column_in_query && convert_null_to_default)
        {
            /// We should CAST default value to required type, otherwise the result of ifNull function can be different type.
            auto cast_expr = makeASTFunction("_CAST", std::move(expr), std::make_shared<ASTLiteral>(columns.get(required_column_name).type->getName()));
            expr = makeASTFunction("ifNull", std::make_shared<ASTIdentifier>(required_column_name), std::move(cast_expr));
        }
        default_expr_list_accum->children.emplace_back(setAlias(expr, required_column_name));
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

bool needConvertAnyNullToDefault(const Block & header, const NamesAndTypesList & required_columns, const ColumnsDescription & columns)
{
    for (const auto & required_column : required_columns)
    {
        if (columns.has(required_column.name) && isNullableOrLowCardinalityNullable(header.findByName(required_column.name)->type) && !isNullableOrLowCardinalityNullable(required_column.type))
            return true;
    }
    return false;
}

ActionsDAGPtr evaluateMissingDefaults(
    const Block & header,
    const NamesAndTypesList & required_columns,
    const ColumnsDescription & columns,
    ContextPtr context,
    bool save_unneeded_columns,
    bool null_as_default)
{
    if (!columns.hasDefaults() && (!null_as_default || !needConvertAnyNullToDefault(header, required_columns, columns)))
        return nullptr;

    ASTPtr expr_list = defaultRequiredExpressions(header, required_columns, columns, null_as_default);
    return createExpressions(header, expr_list, save_unneeded_columns, context);
}

static std::unordered_map<String, ColumnPtr> collectOffsetsColumns(
    const NamesAndTypesList & available_columns, const Columns & res_columns)
{
    std::unordered_map<String, ColumnPtr> offsets_columns;

    auto available_column = available_columns.begin();
    for (size_t i = 0; i < available_columns.size(); ++i, ++available_column)
    {
        if (res_columns[i] == nullptr || isColumnConst(*res_columns[i]))
            continue;

        auto serialization = IDataType::getSerialization(*available_column);
        serialization->enumerateStreams([&](const auto & subpath)
        {
            if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                return;

            auto stream_name = ISerialization::getFileNameForStream(*available_column, subpath);
            const auto & current_offsets_column = subpath.back().data.column;

            /// If for some reason multiple offsets columns are present
            /// for the same nested data structure, choose the one that is not empty.
            if (current_offsets_column && !current_offsets_column->empty())
            {
                auto & offsets_column = offsets_columns[stream_name];
                if (!offsets_column)
                    offsets_column = current_offsets_column;

            #ifndef NDEBUG
                const auto & offsets_data = assert_cast<const ColumnUInt64 &>(*offsets_column).getData();
                const auto & current_offsets_data = assert_cast<const ColumnUInt64 &>(*current_offsets_column).getData();

                if (offsets_data != current_offsets_data)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Found non-equal columns with offsets (sizes: {} and {}) for stream {}",
                        offsets_data.size(), current_offsets_data.size(), stream_name);
            #endif
            }
        }, available_column->type, res_columns[i]);
    }

    return offsets_columns;
}

void fillMissingColumns(
    Columns & res_columns,
    size_t num_rows,
    const NamesAndTypesList & requested_columns,
    const NamesAndTypesList & available_columns,
    const NameSet & partially_read_columns,
    StorageMetadataPtr metadata_snapshot)
{
    size_t num_columns = requested_columns.size();
    if (num_columns != res_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to fillMissingColumns. Expected {}, got {}",
            num_columns, res_columns.size());

    /// For a missing column of a nested data structure
    /// we must create not a column of empty arrays,
    /// but a column of arrays of correct length.

    /// First, collect offset columns for all arrays in the block.
    auto offsets_columns = collectOffsetsColumns(available_columns, res_columns);

    /// Insert default values only for columns without default expressions.
    auto requested_column = requested_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++requested_column)
    {
        const auto & [name, type] = *requested_column;

        if (res_columns[i] && partially_read_columns.contains(name))
            res_columns[i] = nullptr;

        if (res_columns[i])
            continue;

        if (metadata_snapshot && metadata_snapshot->getColumns().hasDefault(name))
            continue;

        std::vector<ColumnPtr> current_offsets;
        size_t num_dimensions = 0;

        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        if (array_type && !offsets_columns.empty())
        {
            num_dimensions = getNumberOfDimensions(*array_type);
            current_offsets.resize(num_dimensions);

            auto serialization = IDataType::getSerialization(*requested_column);
            serialization->enumerateStreams([&](const auto & subpath)
            {
                if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                    return;

                size_t level = ISerialization::getArrayLevel(subpath);
                /// It can happen if element of Array is Map.
                if (level >= num_dimensions)
                    return;

                auto stream_name = ISerialization::getFileNameForStream(*requested_column, subpath);
                auto it = offsets_columns.find(stream_name);
                if (it != offsets_columns.end())
                    current_offsets[level] = it->second;
            });

            for (size_t j = 0; j < num_dimensions; ++j)
            {
                if (!current_offsets[j])
                {
                    current_offsets.resize(j);
                    break;
                }
            }
        }

        if (!current_offsets.empty())
        {
            size_t num_empty_dimensions = num_dimensions - current_offsets.size();
            auto scalar_type = createArrayOfType(getBaseTypeOfArray(type), num_empty_dimensions);

            size_t data_size = assert_cast<const ColumnUInt64 &>(*current_offsets.back()).getData().back();
            res_columns[i] = scalar_type->createColumnConstWithDefaultValue(data_size)->convertToFullColumnIfConst();

            for (auto it = current_offsets.rbegin(); it != current_offsets.rend(); ++it)
                res_columns[i] = ColumnArray::create(res_columns[i], *it);
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
