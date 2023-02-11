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
#include <Interpreters/RequiredSourceColumnsVisitor.h>
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
                throw Exception(
                    "Local replica of shard " + toString(shard_info.shard_num)
                    + " is stale (delay: " + toString(local_delay) + "s.), but no other replica configured",
                    ErrorCodes::ALL_REPLICAS_ARE_STALE);
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


void SelectStreamFactory::createForShardWithParallelReplicas(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const StorageID & main_table,
    ContextPtr context,
    UInt32 shard_count,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards)
{
    if (auto it = objects_by_shard.find(shard_info.shard_num); it != objects_by_shard.end())
        replaceMissedSubcolumnsByConstants(storage_snapshot->object_columns, it->second, query_ast);

    const auto & settings = context->getSettingsRef();

    auto is_local_replica_obsolete = [&]()
    {
        auto resolved_id = context->resolveStorageID(main_table);
        auto main_table_storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

        if (!replicated_storage)
            return false;

        UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

        if (!max_allowed_delay)
            return false;

        UInt64 local_delay = replicated_storage->getAbsoluteDelay();
        return local_delay >= max_allowed_delay;
    };

    size_t next_replica_number = 0;
    size_t all_replicas_count = shard_info.getRemoteNodeCount();

    auto coordinator = std::make_shared<ParallelReplicasReadingCoordinator>();

    if (settings.prefer_localhost_replica && shard_info.isLocal())
    {
        /// We don't need more than one local replica in parallel reading
        if (!is_local_replica_obsolete())
        {
            ++all_replicas_count;

            local_plans.emplace_back(createLocalPlan(
                query_ast, header, context, processed_stage, shard_info.shard_num, shard_count, next_replica_number, all_replicas_count, coordinator));

            ++next_replica_number;
        }
    }

    if (shard_info.hasRemoteConnections())
        remote_shards.emplace_back(Shard{
            .query = query_ast,
            .header = header,
            .shard_info = shard_info,
            .lazy = false,
            .local_delay = 0,
            .coordinator = coordinator,
        });
}

}
}
