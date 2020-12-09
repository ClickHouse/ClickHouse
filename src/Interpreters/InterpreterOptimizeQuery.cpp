#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/AccessRightsElement.h>
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
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());

    context.checkAccess(getRequiredAccess());

    auto table_id = context.resolveStorageID(ast, Context::ResolveOrdinary);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    // Empty list of names means we deduplicate by all columns, but user can explicitly state which columns to use.
    Names column_names;
    if (ast.deduplicate_by_columns)
    {
        // User requested custom set of columns for deduplication, possibly with Column Transformer expression.
        {
            // Expand asterisk, column transformers, etc into list of column names.
            const auto cols = processColumnTransformers(context.getCurrentDatabase(), table, metadata_snapshot, ast.deduplicate_by_columns);
            for (const auto & col : cols->children)
                column_names.emplace_back(col->getColumnName());
        }

        metadata_snapshot->check(column_names, NamesAndTypesList{}, table_id);
        const auto & sorting_keys = metadata_snapshot->getColumnsRequiredForSortingKey();
        for (const auto & sorting_key : sorting_keys)
        {
            // Deduplication is performed only for adjacent rows in a block,
            // and all rows in block are in the sorting key order,
            // hence deduplication always implicitly takes sorting keys in account.
            // So we just explicitly state that limitation in order to avoid confusion.
            if (std::find(column_names.begin(), column_names.end(), sorting_key) == column_names.end())
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                        "DEDUPLICATE BY expression must include all columns used in table's ORDER BY or PRIMARY KEY,"
                        " but '{}' is missing."
                        " Expanded deduplicate columns expression: ['{}']",
                        sorting_key, fmt::join(column_names, "', '"));
        }
    }

    table->optimize(query_ptr, metadata_snapshot, ast.partition, ast.final, ast.deduplicate, column_names, context);

    return {};
}


AccessRightsElements InterpreterOptimizeQuery::getRequiredAccess() const
{
    const auto & optimize = query_ptr->as<const ASTOptimizeQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::OPTIMIZE, optimize.database, optimize.table);
    return required_access;
}

}
