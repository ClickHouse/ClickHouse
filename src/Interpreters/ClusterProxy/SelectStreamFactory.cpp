#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/checkStackSize.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <DataTypes/ObjectUtils.h>

#include <Client/IConnections.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(
    ContextPtr context,
    const ASTPtr & query,
    const std::string & remote_database,
    const std::string & remote_table,
    ASTPtr table_function_ptr)
{
    auto modified_query_ast = query->clone();

    ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

    // Get rid of the settings clause so we don't send them to remote. Thus newly non-important
    // settings won't break any remote parser. It's also more reasonable since the query settings
    // are written into the query context and will be sent by the query pipeline.
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, {});

    if (table_function_ptr)
        select_query.addTableFunction(table_function_ptr);
    else
        select_query.replaceDatabaseAndTable(remote_database, remote_table);

    /// Restore long column names (cause our short names are ambiguous).
    /// TODO: aliased table functions & CREATE TABLE AS table function cases
    if (!table_function_ptr)
    {
        RestoreQualifiedNamesVisitor::Data data;
        data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query->as<ASTSelectQuery &>(), 0));
        data.remote_table.database = remote_database;
        data.remote_table.table = remote_table;
        RestoreQualifiedNamesVisitor(data).visit(modified_query_ast);
    }

    /// To make local JOIN works, default database should be added to table names.
    /// But only for JOIN section, since the following should work using default_database:
    /// - SELECT * FROM d WHERE value IN (SELECT l.value FROM l) ORDER BY value
    ///   (see 01487_distributed_in_not_default_db)
    AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase(),
        /* only_replace_current_database_function_= */false,
        /* only_replace_in_join_= */true);
    visitor.visit(modified_query_ast);

    return modified_query_ast;
}


SelectStreamFactory::SelectStreamFactory(
    const Block & header_,
    const ColumnsDescriptionByShardNum & objects_by_shard_,
    const StorageSnapshotPtr & storage_snapshot_,
    QueryProcessingStage::Enum processed_stage_)
    : header(header_),
    objects_by_shard(objects_by_shard_),
    storage_snapshot(storage_snapshot_),
    processed_stage(processed_stage_)
{
}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards,
    UInt32 shard_count)
{
    auto it = objects_by_shard.find(shard_info.shard_num);
    if (it != objects_by_shard.end())
        replaceMissedSubcolumnsByConstants(storage_snapshot->object_columns, it->second, query_ast);

    auto emplace_local_stream = [&]()
    {
        local_plans.emplace_back(createLocalPlan(
            query_ast, header, context, processed_stage, shard_info.shard_num, shard_count, /*replica_num=*/0, /*replica_count=*/0, /*coordinator=*/nullptr));
    };

    auto emplace_remote_stream = [&](bool lazy = false, time_t local_delay = 0)
    {
        remote_shards.emplace_back(Shard{
            .query = query_ast,
            .header = header,
            .shard_info = shard_info,
            .lazy = lazy,
            .local_delay = local_delay,
        });
    };

    const auto & settings = context->getSettingsRef();

    if (settings.prefer_localhost_replica && shard_info.isLocal())
    {
        StoragePtr main_table_storage;

        if (table_func_ptr)
        {
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_func_ptr, context);
            main_table_storage = table_function_ptr->execute(table_func_ptr, context, table_function_ptr->getName());
        }
        else
        {
            auto resolved_id = context->resolveStorageID(main_table);
            main_table_storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
        }


        if (!main_table_storage) /// Table is absent on a local server.
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            if (shard_info.hasRemoteConnections())
            {
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "There is no table {} on local replica of shard {}, will try remote replicas.",
                    main_table.getNameForLogs(), shard_info.shard_num);
                emplace_remote_stream();
            }
            else
                emplace_local_stream();  /// Let it fail the usual way.

            return;
        }

        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

        if (!replicated_storage)
        {
            /// Table is not replicated, use local server.
            emplace_local_stream();
            return;
        }

        UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

        if (!max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        UInt64 local_delay = replicated_storage->getAbsoluteDelay();

        if (local_delay < max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        /// If we reached this point, local replica is stale.
        ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"), "Local replica of shard {} is stale (delay: {}s.)", shard_info.shard_num, local_delay);

        if (!settings.fallback_to_stale_replicas_for_distributed_queries)
        {
            if (shard_info.hasRemoteConnections())
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            else
                throw Exception(ErrorCodes::ALL_REPLICAS_ARE_STALE, "Local replica of shard {} is stale (delay: "
                    "{}s.), but no other replica configured", shard_info.shard_num, toString(local_delay));
        }

        if (!shard_info.hasRemoteConnections())
        {
            /// There are no remote replicas but we are allowed to fall back to stale local replica.
            emplace_local_stream();
            return;
        }

        /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
        /// Do it lazily to avoid connecting in the main thread.
        emplace_remote_stream(true /* lazy */, local_delay);
    }
    else
        emplace_remote_stream();
}


}
}
