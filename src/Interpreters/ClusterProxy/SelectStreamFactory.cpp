#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/Utils.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <DataTypes/ObjectUtils.h>
#include <Client/IConnections.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
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
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool fallback_to_stale_replicas_for_distributed_queries;
    extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
    extern const SettingsBool prefer_localhost_replica;
}

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace FailPoints
{
    extern const char use_delayed_remote_source[];
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

    if (!context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
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
    UInt32 shard_count,
    bool parallel_replicas_enabled,
    AdditionalShardFilterGenerator shard_filter_generator)
{
    auto it = objects_by_shard.find(shard_info.shard_num);
    if (it != objects_by_shard.end())
        replaceMissedSubcolumnsByConstants(storage_snapshot->object_columns, it->second, query_ast);

    createForShardImpl(
        shard_info,
        query_ast,
        {},
        main_table,
        table_func_ptr,
        std::move(context),
        local_plans,
        remote_shards,
        shard_count,
        parallel_replicas_enabled,
        std::move(shard_filter_generator));
}

void SelectStreamFactory::createForShardImpl(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const QueryTreeNodePtr & query_tree,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards,
    UInt32 shard_count,
    bool parallel_replicas_enabled,
    AdditionalShardFilterGenerator shard_filter_generator,
    bool has_missing_objects)
{
    auto emplace_local_stream = [&]()
    {
        local_plans.emplace_back(createLocalPlan(
            query_ast, header, context, processed_stage, shard_info.shard_num, shard_count, has_missing_objects));
    };

    auto emplace_remote_stream = [&](bool lazy = false, time_t local_delay = 0)
    {
        Block shard_header;
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            shard_header = InterpreterSelectQueryAnalyzer::getSampleBlock(query_tree, context, SelectQueryOptions(processed_stage).analyze());
        else
            shard_header = header;

        remote_shards.emplace_back(Shard{
            .query = query_ast,
            .query_tree = query_tree,
            .main_table = main_table,
            .header = shard_header,
            .has_missing_objects = has_missing_objects,
            .shard_info = shard_info,
            .lazy = lazy,
            .local_delay = local_delay,
            .shard_filter_generator = std::move(shard_filter_generator),
        });
    };

    const auto & settings = context->getSettingsRef();

    fiu_do_on(FailPoints::use_delayed_remote_source,
    {
        emplace_remote_stream(/*lazy=*/true, /*local_delay=*/999999);
        return;
    });

    // prefer_localhost_replica is not effective in case of parallel replicas
    // (1) prefer_localhost_replica is about choosing one replica on a shard
    // (2) parallel replica coordinator has own logic to choose replicas to read from
    if (settings[Setting::prefer_localhost_replica] && shard_info.isLocal() && !parallel_replicas_enabled)
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
                LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"),
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

        const UInt64 max_allowed_delay = settings[Setting::max_replica_delay_for_distributed_queries];

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
        LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"), "Local replica of shard {} is stale (delay: {}s.)", shard_info.shard_num, local_delay);

        if (!settings[Setting::fallback_to_stale_replicas_for_distributed_queries])
        {
            if (shard_info.hasRemoteConnections())
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            throw Exception(
                ErrorCodes::ALL_REPLICAS_ARE_STALE,
                "Local replica of shard {} is stale (delay: "
                "{}s.), but no other replica configured",
                shard_info.shard_num,
                toString(local_delay));
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

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const QueryTreeNodePtr & query_tree,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards,
    UInt32 shard_count,
    bool parallel_replicas_enabled,
    AdditionalShardFilterGenerator shard_filter_generator)
{

    auto it = objects_by_shard.find(shard_info.shard_num);
    QueryTreeNodePtr modified_query = query_tree;

    bool has_missing_objects = false;
    if (it != objects_by_shard.end())
        has_missing_objects = replaceMissedSubcolumnsByConstants(storage_snapshot->object_columns, it->second, modified_query, context);

    auto query_ast = queryNodeToDistributedSelectQuery(modified_query);

    createForShardImpl(
        shard_info,
        query_ast,
        modified_query,
        main_table,
        table_func_ptr,
        std::move(context),
        local_plans,
        remote_shards,
        shard_count,
        parallel_replicas_enabled,
        std::move(shard_filter_generator),
        has_missing_objects);

}


}
}
