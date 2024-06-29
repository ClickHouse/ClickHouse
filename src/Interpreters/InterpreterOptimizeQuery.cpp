#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/Common/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDistributed.h>

#include <Interpreters/processColumnTransformers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();
    auto table_id = getContext()->resolveStorageID(ast);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    bool do_rewrite_to_oncluster = false;
    const StorageDistributed * maybe_distributed = nullptr;
    if (table)
    {
        maybe_distributed = table->as<const StorageDistributed>();
        if (maybe_distributed && maybe_distributed->supportsOptimizeRewriteToOncluster())
        {
            do_rewrite_to_oncluster = true;
        }
    }

    if (!ast.cluster.empty())
    {
        // prevent from executing recursively
        if (do_rewrite_to_oncluster)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE ON CLUSTER is not allowed for "
                "distributed table when enable_ddl_optimize_rewrite_to_oncluster turned on");

        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess());

    // Rewrite optimizing to corresponding local table with on cluster
    if (do_rewrite_to_oncluster)
    {
        auto query_clone = query_ptr->clone();
        auto * optimize_ast_ptr = query_clone->as<ASTOptimizeQuery>();
        //change distributed table to remote table
        optimize_ast_ptr->setDatabase(maybe_distributed->getRemoteDatabaseName());
        optimize_ast_ptr->setTable(maybe_distributed->getRemoteTableName());
        //add on cluster
        optimize_ast_ptr->cluster = maybe_distributed->getCluster()->getName();
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_clone, getContext(), params);
    }

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

    if (auto * snapshot_data = dynamic_cast<MergeTreeData::SnapshotData *>(storage_snapshot->data.get()))
        snapshot_data->parts = {};

    table->optimize(query_ptr, metadata_snapshot, ast.partition, ast.final, ast.deduplicate, column_names, ast.cleanup, getContext());

    return {};
}


AccessRightsElements InterpreterOptimizeQuery::getRequiredAccess() const
{
    const auto & optimize = query_ptr->as<const ASTOptimizeQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::OPTIMIZE, optimize.getDatabase(), optimize.getTable());
    return required_access;
}

void registerInterpreterOptimizeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterOptimizeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterOptimizeQuery", create_fn);
}
}
