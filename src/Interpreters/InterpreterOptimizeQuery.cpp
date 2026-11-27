#include "config.h"

#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Planner/Utils.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#if USE_AVRO
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#endif

#include <Interpreters/processColumnTransformers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int THERE_IS_NO_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();

    if (!ast.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        if (ast.deduplicate)
        {
            params.additional_access_check = [captured_query_ptr = query_ptr, context = getContext()](const String & cluster_default_database, bool throw_if_unresolved)
            {
                /// Hosts that run the query as the initiating user check the table themselves.
                if (!throw_if_unresolved)
                    return;
                const auto & query = captured_query_ptr->as<const ASTOptimizeQuery &>();
                const StorageID table_id(query.getDatabase().empty() ? cluster_default_database : query.getDatabase(), query.getTable());
                /// The hosts compare the columns without the user, so `SELECT` is required on the whole table here.
                context->checkAccess(AccessType::SELECT, table_id.database_name, table_id.table_name);
                const auto table = DatabaseCatalog::instance().tryGetTable(table_id, context);
                if (!table)
                    throw Exception(ErrorCodes::UNKNOWN_TABLE,
                        "Table {} does not exist on the initiator. The other hosts do not run the query as the initiating user. So the "
                        "initiator must check the columns that DEDUPLICATE compares. Run the query on a host that has this table. "
                        "Alternatively, let every host check the query as the initiating user. For that, enable the server setting "
                        "distributed_ddl_use_initial_user_and_roles on every host, and set distributed_ddl_entry_format_version to at least {}",
                        table_id.getNameForLogs(), DDLLogEntry::INITIATOR_USER_VERSION);
                if (castStorage<StorageAlias>(table, DeferredTable::Load))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Table {} is an Alias. The other hosts do not run the query as the initiating user, so they would not check SELECT "
                        "on its target. Run OPTIMIZE ... ON CLUSTER ... DEDUPLICATE on the target table. Alternatively, enable the server "
                        "setting distributed_ddl_use_initial_user_and_roles on every host, and set distributed_ddl_entry_format_version to at least {}",
                        table_id.getNameForLogs(), DDLLogEntry::INITIATOR_USER_VERSION);
                if (auto view = castStorage<StorageMaterializedView>(table, DeferredTable::Load))
                {
                    auto view_metadata = view->getInMemoryMetadataPtr(context, false);
                    if (!view_metadata->sql_security_type && !view_metadata->select.select_table_id.empty())
                        context->checkAccess(AccessType::SELECT, view_metadata->select.select_table_id);
                }
            };
        }
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(getRequiredAccess());

    auto table_id = getContext()->resolveStorageID(ast);
    /// Resolve before reading the metadata, so the checks below and `optimizeDryRun` see the real
    /// structure rather than the columns-only one a lazily loaded table reports.
    StoragePtr table = resolveStorageProxyLoading(DatabaseCatalog::instance().getTable(table_id, getContext()));
    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    auto metadata_snapshot = table->getInMemoryMetadataPtr(getContext(), false);
    auto storage_snapshot = table->getStorageSnapshotWithoutData(metadata_snapshot, getContext());

    /// Handle OPTIMIZE TABLE ... MANIFEST for Iceberg tables
    if (ast.manifest)
    {
        if (ast.final || ast.partition || ast.deduplicate || ast.cleanup || ast.dry_run)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE MANIFEST is incompatible with FINAL, PARTITION, DEDUPLICATE, CLEANUP, and DRY RUN options");

#if USE_AVRO
        auto object_storage_table = castStorage<StorageObjectStorage>(table, DeferredTable::Skip);
        if (!object_storage_table)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OPTIMIZE MANIFEST is only supported for Iceberg tables");

        auto iceberg_metadata = std::dynamic_pointer_cast<IcebergMetadata>(object_storage_table->getExternalMetadata(getContext()));
        if (!iceberg_metadata)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OPTIMIZE MANIFEST is only supported for Iceberg tables");

        getContext()->onQueryExecutionStarted();
        iceberg_metadata->optimizeManifestFiles(metadata_snapshot, getContext(), object_storage_table->getCatalog(), table_id);
        return {};
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OPTIMIZE MANIFEST is only supported for Iceberg tables");
#endif
    }

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

        checkAccessRights(table, table_id, storage_snapshot, column_names, getContext());

        /// The errors below name the table's columns, so only for a user who may `DESCRIBE` the table.
        const auto * alias = table->as<StorageAlias>();
        const bool can_show_columns = getContext()->getAccess()->isGranted(AccessType::SHOW_COLUMNS, table_id.database_name, table_id.table_name)
            && (!alias || alias->isTargetTableGranted(getContext(), AccessType::SHOW_COLUMNS, {}));
        storage_snapshot->check(column_names, can_show_columns);
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
            {
                if (!can_show_columns)
                    throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                        "DEDUPLICATE BY expression must include all columns used in table's ORDER BY, PRIMARY KEY, or PARTITION BY");
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                        "DEDUPLICATE BY expression must include all columns used in table's"
                        " ORDER BY, PRIMARY KEY, or PARTITION BY but '{}' is missing."
                        " Expanded DEDUPLICATE BY columns expression: ['{}']",
                        required_col, fmt::join(column_names, "', '"));
            }
        }
    }
    else if (ast.deduplicate)
    {
        /// Without a list, deduplication compares all physical columns.
        checkAccessRights(table, table_id, storage_snapshot, metadata_snapshot->getColumns().getNamesOfPhysical(), getContext());
    }

    if (ast.dry_run)
    {
        auto * merge_tree_data = castStorage<MergeTreeData>(table, DeferredTable::Load).get();
        if (!merge_tree_data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE DRY RUN is only supported for MergeTree family tables");

        if (ast.final)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE DRY RUN is incompatible with FINAL");

        if (ast.partition)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE DRY RUN is incompatible with PARTITION");

        if (!ast.parts_list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE DRY RUN requires at least one part name");

        Names part_names;
        for (const auto & child : ast.parts_list->children)
        {
            const auto & literal = child->as<ASTLiteral &>();
            part_names.emplace_back(literal.value.safeGet<String>());
        }

        getContext()->onQueryExecutionStarted();
        merge_tree_data->optimizeDryRun(part_names, metadata_snapshot, ast.deduplicate, column_names, ast.cleanup, getContext());
        return {};
    }

    getContext()->onQueryExecutionStarted();
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

void registerInterpreterOptimizeQuery(InterpreterFactory & factory);
void registerInterpreterOptimizeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterOptimizeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterOptimizeQuery", create_fn);
}
}
