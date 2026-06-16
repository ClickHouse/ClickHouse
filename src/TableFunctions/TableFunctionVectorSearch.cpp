#include "config.h"

#if USE_USEARCH

#include <Storages/StorageMergeTreeVectorSearch.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_search_topk_table_functions;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

class TableFunctionVectorSearch : public ITableFunction
{
public:
    static constexpr auto name = "vectorSearch";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        return "";
    }

    String source_database;
    String source_table;
    String source_index_name;
    VectorWithMemoryTracking<Float64> reference_vector;
    size_t limit_k = 0;
};

void TableFunctionVectorSearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_experimental_search_topk_table_functions])
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Set `allow_experimental_search_topk_table_functions = 1` to enable the `vectorSearch` table function.");
    }

    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;
    if (args.size() != 4 && args.size() != 5)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function vectorSearch requires 4 or 5 arguments: "
            "(database, table[, index_name], reference_vector, K). Got {}.",
            args.size());
    }

    auto database_arg = evaluateConstantExpressionForDatabaseName(args[0], context);
    auto table_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    source_database = checkAndGetLiteralArgument<String>(database_arg, "database");
    source_table = checkAndGetLiteralArgument<String>(table_arg, "table");

    const bool has_index_name = (args.size() == 5);
    size_t vector_arg_idx = has_index_name ? 3 : 2;

    if (has_index_name)
    {
        auto index_name_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        source_index_name = checkAndGetLiteralArgument<String>(index_name_arg, "index_name");

        if (source_index_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 3 of `{}` (index_name) must not be empty", getName());
    }

    auto [vector_field, vector_type] = evaluateConstantExpression(args[vector_arg_idx], context);
    auto [limit_field, limit_type] = evaluateConstantExpression(args[vector_arg_idx + 1], context);

    /// Parse reference vector from Array literal.
    if (vector_field.getType() != Field::Types::Array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} of `{}` (reference_vector) must be an Array", vector_arg_idx + 1, getName());

    const auto & arr = vector_field.safeGet<Array>();
    if (arr.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Reference vector must not be empty");

    reference_vector.reserve(arr.size());
    for (const auto & elem : arr)
        reference_vector.push_back(elem.safeGet<Float64>());

    limit_k = limit_field.safeGet<UInt64>();
    if (limit_k == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "K must be greater than 0");

    /// The same cap that makes the `ORDER BY ... LIMIT` rewrite refuse the
    /// vector index: large result sets can overflow memory in the index
    /// search path. That path falls back to an exact search; this one has
    /// no fallback, so fail explicitly.
    const UInt64 max_k = context->getSettingsRef()[Setting::max_limit_for_vector_search_queries];
    if (limit_k > max_k)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "K ({}) must not be greater than setting `max_limit_for_vector_search_queries` ({})",
            limit_k, max_k);
    }
}

ColumnsDescription TableFunctionVectorSearch::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    auto source_table_ptr = DatabaseCatalog::instance().getTable(StorageID{source_database, source_table}, context);
    auto metadata_snapshot = source_table_ptr->getInMemoryMetadataPtr(context, false);

    ColumnsDescription result = metadata_snapshot->getColumns();
    StorageMergeTreeScoredSearchBase::checkSourceColumnsForReservedNames(result, source_table_ptr->getStorageID());

    /// `_score` is the table function's headline output and appears in `SELECT *`.
    result.add(ColumnDescription("_score", std::make_shared<DataTypeFloat32>()));

    /// `_distance` is a backward-compatible alias for `_score` (the name the
    if (!result.has("_distance"))
    {
        ColumnDescription distance_column("_distance", std::make_shared<DataTypeFloat32>());
        distance_column.default_desc.kind = ColumnDefaultKind::Alias;
        distance_column.default_desc.expression = make_intrusive<ASTIdentifier>("_score");
        result.add(std::move(distance_column));
    }

    return result;
}

StoragePtr TableFunctionVectorSearch::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto source_table_ptr = DatabaseCatalog::instance().getTable(StorageID{source_database, source_table}, context);

    /// Engine guard: only MergeTree-family sources are supported. Distributed parents must be
    /// reached via `remote(...)` so the table function dispatches to a concrete shard.
    if (dynamic_cast<MergeTreeData *>(source_table_ptr.get()) == nullptr)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Function vectorSearch only supports MergeTree-family source tables; "
            "use remote(...) to dispatch to a Distributed table's shards.");
    }

    auto metadata_snapshot = source_table_ptr->getInMemoryMetadataPtr(context, false);
    const auto & indices = metadata_snapshot->getSecondaryIndices();

    String resolved_index_name;
    if (!source_index_name.empty())
    {
        /// 5-arg form: explicit index name. Must exist and must be a vector_similarity index.
        if (!indices.has(source_index_name))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No vector_similarity index named '{}' on table {}",
                source_index_name, source_table_ptr->getStorageID().getNameForLogs());
        }

        const auto & index_desc = indices.getByName(source_index_name);
        auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);

        if (!index_helper->isVectorSimilarityIndex())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Index '{}' on table {}.{} is not a vector_similarity index (got type '{}')",
                source_index_name, source_database, source_table, index_desc.type);
        }

        resolved_index_name = source_index_name;
    }
    else
    {
        /// 4-arg form: auto-resolve. Requires exactly one vector_similarity index.
        VectorWithMemoryTracking<String> vector_index_names;

        for (const auto & index_desc : indices)
        {
            auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
            if (index_helper->isVectorSimilarityIndex())
                vector_index_names.push_back(index_desc.name);
        }

        if (vector_index_names.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Table {} has no vector_similarity index; the 4-arg vectorSearch form requires exactly one",
                source_table_ptr->getStorageID().getNameForLogs());
        }

        if (vector_index_names.size() > 1)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Table {} has {} vector_similarity indexes ({}); "
                "the 4-arg form requires exactly one. Specify via the 5-arg form.",
                source_table_ptr->getStorageID().getNameForLogs(), vector_index_names.size(), fmt::join(vector_index_names, ", "));
        }

        resolved_index_name = vector_index_names.front();
    }

    auto vector_index = MergeTreeIndexFactory::instance().get(indices.getByName(resolved_index_name));
    auto columns = getActualTableStructure(context, is_insert_query);
    StorageID storage_id(getDatabaseName(), table_name);

    auto res = std::make_shared<StorageMergeTreeVectorSearch>(
        std::move(storage_id),
        std::move(source_table_ptr),
        std::move(vector_index),
        reference_vector,
        limit_k,
        std::move(columns),
        metadata_snapshot->virtuals);

    res->startup();
    return res;
}

void registerTableFunctionVectorSearch(TableFunctionFactory & factory);
void registerTableFunctionVectorSearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionVectorSearch>(
        {
            .description = "Searches the vector_similarity index of a MergeTree table. Returns the top K approximate nearest neighbors with their score (USearch's raw distance).",
            .examples = {{"vectorSearch", "SELECT id, _score FROM vectorSearch(currentDatabase(), my_table, [0.5, 0.5, 0.5], 5) ORDER BY _score", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = true}
    );
}

}

#endif
