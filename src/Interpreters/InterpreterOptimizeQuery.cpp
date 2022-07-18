#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/Common/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTExpressionList.h>

#include <Interpreters/processColumnTransformers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccess());

    getContext()->checkAccess(getRequiredAccess());

    auto table_id = getContext()->resolveStorageID(ast, Context::ResolveOrdinary);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto storage_snapshot = table->getStorageSnapshot(metadata_snapshot, getContext());

    // Empty list of names means we deduplicate by all columns, but user can explicitly state which columns to use.
    Names column_names;
    if (ast.deduplicate_by_columns)
    {
        // User requested custom set of columns for deduplication, possibly with Column Transformer expression.
        {
            // Expand asterisk, column transformers, etc into list of column names.
            const auto cols
                = processColumnTransformers(getContext()->getCurrentDatabase(), table, metadata_snapshot, ast.deduplicate_by_columns);
            for (const auto & col : cols->children)
                column_names.emplace_back(col->getColumnName());
        }

        storage_snapshot->check(column_names);
        Names required_columns;
        {
            required_columns = metadata_snapshot->getColumnsRequiredForSortingKey();
            const auto partitioning_cols = metadata_snapshot->getColumnsRequiredForPartitionKey();
            required_columns.reserve(required_columns.size() + partitioning_cols.size());
            required_columns.insert(required_columns.end(), partitioning_cols.begin(), partitioning_cols.end());
        }
        for (const auto & required_col : required_columns)
        {
            // Deduplication is performed only for adjacent rows in a block,
            // and all rows in block are in the sorting key order within a single partition,
            // hence deduplication always implicitly takes sorting keys and partition keys in account.
            // So we just explicitly state that limitation in order to avoid confusion.
            if (std::find(column_names.begin(), column_names.end(), required_col) == column_names.end())
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                        "DEDUPLICATE BY expression must include all columns used in table's"
                        " ORDER BY, PRIMARY KEY, or PARTITION BY but '{}' is missing."
                        " Expanded DEDUPLICATE BY columns expression: ['{}']",
                        required_col, fmt::join(column_names, "', '"));
        }
    }

    table->optimize(query_ptr, metadata_snapshot, ast.partition, ast.final, ast.deduplicate, column_names, getContext());

    return {};
}


AccessRightsElements InterpreterOptimizeQuery::getRequiredAccess() const
{
    const auto & optimize = query_ptr->as<const ASTOptimizeQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::OPTIMIZE, optimize.getDatabase(), optimize.getTable());
    return required_access;
}

}
