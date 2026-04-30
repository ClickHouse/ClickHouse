#include <Storages/MergeTree/SelectiveReplication/OptimizeForwarder.h>
#include <Storages/MergeTree/SelectiveReplication/ForwardingUtils.h>

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/RemoteQueryCommon.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Client/ConnectionPool.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event SelectiveReplicationOptimizeForwardedPartitions;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ABORTED;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int TABLE_IS_READ_ONLY;
}

namespace Setting
{
    extern const SettingsUInt64 max_distributed_connections;
    extern const SettingsBool optimize_skip_merged_partitions;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsBool skip_unavailable_shards;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
}

namespace SelectiveReplication
{

OptimizeForwarder::OptimizeForwarder(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log(storage_.log.load())
{
}

UInt64 OptimizeForwarder::getMaxBytesToMergeAtMaxSpaceInPool(const MergeTreeSettingsPtr & settings)
{
    return (*settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool];
}

bool OptimizeForwarder::getAssignPartUuids(const MergeTreeSettingsPtr & settings)
{
    return (*settings)[MergeTreeSetting::assign_part_uuids];
}

bool OptimizeForwarder::getOptimizeSkipMergedPartitions(ContextPtr query_context)
{
    return query_context->getSettingsRef()[Setting::optimize_skip_merged_partitions];
}

UInt64 OptimizeForwarder::getReceiveTimeoutMs(ContextPtr query_context)
{
    return query_context->getSettingsRef()[Setting::receive_timeout].totalMilliseconds();
}

UInt64 OptimizeForwarder::getMaxDistributedConnections(ContextPtr query_context)
{
    return query_context->getSettingsRef()[Setting::max_distributed_connections];
}

bool OptimizeForwarder::getSkipUnavailableShards(ContextPtr query_context)
{
    return query_context->getSettingsRef()[Setting::skip_unavailable_shards];
}

bool OptimizeForwarder::forwardOptimizeToReplica(
    const String & target_replica,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    const ReplicatedMergeTreeAddress & address,
    ContextPtr query_context)
{
    try
    {
        ProfileEvents::increment(ProfileEvents::SelectiveReplicationOptimizeForwardedPartitions);
        auto remote_context = Context::createCopy(query_context);
        remote_context->setSetting("skip_unavailable_shards", false);
        remote_context->increaseDistributedDepth();

        auto remote_storage_id = StorageID(address.database, address.table);
        ASTPtr query = createOptimizeForRemoteTableQuery(
            remote_storage_id, partition_id, final, deduplicate, deduplicate_by_columns, cleanup);

        String query_str = query->formatWithSecretsOneLine();

        auto pool = ForwardingUtils::createReplicaPool(
            address,
            storage.getContext(),
            static_cast<unsigned>(remote_context->getSettingsRef()[Setting::max_distributed_connections]));

        /// Manually establish the connection before constructing RemoteQueryExecutor.
        /// This avoids a logical error in MultiplexedConnections when the connection
        /// pool returns an empty set because the target replica is unreachable.
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(remote_context->getSettingsRef());
        auto connection_entry = pool->get(timeouts, remote_context->getSettingsRef(), /*force_connected=*/true);

        std::vector<IConnectionPool::Entry> connections;
        connections.emplace_back(std::move(connection_entry));

        RemoteQueryExecutor executor(std::move(connections), query_str, nullptr, remote_context);
        executor.setLogger(log);
        executor.sendQuery();

        while (true) { Block block = executor.readBlock(); if (block.empty()) break; }

        LOG_DEBUG(log, "Selective replication: forwarded OPTIMIZE partition {} to replica {}", partition_id, target_replica);
        return true;
    }
    catch (const NetException & e)
    {
        LOG_WARNING(log, "Network error while forwarding OPTIMIZE to replica {}: {}", target_replica, e.message());
        return false;
    }
    catch (const Exception & e)
    {
        /// Remote replica can reject OPTIMIZE for various reasons:
        /// - ABORTED: log pulling is cancelled (SYSTEM STOP PULLING REPLICATION LOG)
        /// - CANNOT_ASSIGN_OPTIMIZE: merges are stopped (SYSTEM STOP MERGES) or nothing to merge
        /// - TABLE_IS_READ_ONLY: replica is in readonly mode
        /// All of these mean the remote replica cannot handle the OPTIMIZE right now,
        /// so we treat it as unreachable and try the next fallback candidate
        /// instead of propagating the error to the caller.
        if (e.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED
            || e.code() == ErrorCodes::ABORTED
            || e.code() == ErrorCodes::CANNOT_ASSIGN_OPTIMIZE
            || e.code() == ErrorCodes::TABLE_IS_READ_ONLY)
        {
            LOG_WARNING(log, "Cannot forward OPTIMIZE to replica {} ({}: {}), will try fallback",
                        target_replica, e.code(), e.message());
            return false;
        }
        throw;
    }
}

}
}
