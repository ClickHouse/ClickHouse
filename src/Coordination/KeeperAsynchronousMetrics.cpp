#include <Coordination/KeeperAsynchronousMetrics.h>

#include <Coordination/KeeperDispatcher.h>

#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>

namespace DB
{

void updateKeeperInformation(KeeperDispatcher & keeper_dispatcher, AsynchronousMetricValues & new_values)
{
#if USE_NURAFT
    size_t is_leader = 0;
    size_t is_follower = 0;
    size_t is_observer = 0;
    size_t is_standalone = 0;
    size_t znode_count = 0;
    size_t watch_count = 0;
    size_t ephemerals_count = 0;
    size_t approximate_data_size = 0;
    size_t key_arena_size = 0;
    size_t open_file_descriptor_count = 0;
    std::optional<size_t> max_file_descriptor_count = 0;
    size_t followers = 0;
    size_t synced_followers = 0;
    size_t zxid = 0;
    size_t session_with_watches = 0;
    size_t paths_watched = 0;
    size_t is_exceeding_mem_soft_limit = 0;

    if (keeper_dispatcher.isServerActive())
    {
        auto keeper_info = keeper_dispatcher.getKeeper4LWInfo();
        is_standalone = static_cast<size_t>(keeper_info.is_standalone);
        is_leader = static_cast<size_t>(keeper_info.is_leader);
        is_observer = static_cast<size_t>(keeper_info.is_observer);
        is_follower = static_cast<size_t>(keeper_info.is_follower);
        is_exceeding_mem_soft_limit = static_cast<size_t>(keeper_info.is_exceeding_mem_soft_limit);

        const auto & state_machine = keeper_dispatcher.getStateMachine();
        const auto & storage_stats = state_machine.getStorageStats();
        zxid = storage_stats.last_zxid.load(std::memory_order_relaxed);
        znode_count = storage_stats.nodes_count.load(std::memory_order_relaxed);
        watch_count = storage_stats.total_watches_count.load(std::memory_order_relaxed);
        ephemerals_count = storage_stats.total_emphemeral_nodes_count.load(std::memory_order_relaxed);
        approximate_data_size = storage_stats.approximate_data_size.load(std::memory_order_relaxed);
        key_arena_size = 0;
        session_with_watches = storage_stats.sessions_with_watches_count.load(std::memory_order_relaxed);
        paths_watched = storage_stats.watched_paths_count.load(std::memory_order_relaxed);

#    if defined(__linux__) || defined(__APPLE__)
        open_file_descriptor_count = getCurrentProcessFDCount();
        max_file_descriptor_count = getMaxFileDescriptorCount();
#    endif

        if (keeper_info.is_leader)
        {
            followers = keeper_info.follower_count;
            synced_followers = keeper_info.synced_follower_count;
        }
    }

    new_values["KeeperIsLeader"] = { is_leader, "1 if ClickHouse Keeper is a leader, 0 otherwise." };
    new_values["KeeperIsFollower"] = { is_follower, "1 if ClickHouse Keeper is a follower, 0 otherwise." };
    new_values["KeeperIsObserver"] = { is_observer, "1 if ClickHouse Keeper is an observer, 0 otherwise." };
    new_values["KeeperIsStandalone"] = { is_standalone, "1 if ClickHouse Keeper is in a standalone mode, 0 otherwise." };
    new_values["KeeperIsExceedingMemorySoftLimitHit"] = { is_exceeding_mem_soft_limit, "1 if ClickHouse Keeper is exceeding the memory soft limit, 0 otherwise." };

    new_values["KeeperZnodeCount"] = { znode_count, "The number of nodes (data entries) in ClickHouse Keeper." };
    new_values["KeeperWatchCount"] = { watch_count, "The number of watches in ClickHouse Keeper." };
    new_values["KeeperEphemeralsCount"] = { ephemerals_count, "The number of ephemeral nodes in ClickHouse Keeper." };

    new_values["KeeperApproximateDataSize"] = { approximate_data_size, "The approximate data size of ClickHouse Keeper, in bytes." };
    new_values["KeeperKeyArenaSize"] = { key_arena_size, "The size in bytes of the memory arena for keys in ClickHouse Keeper." };
    /// TODO: value was incorrectly set to 0 previously for local snapshots
    /// it needs to be fixed and it needs to be atomic to avoid deadlock
    ///new_values["KeeperLatestSnapshotSize"] = { latest_snapshot_size, "The uncompressed size in bytes of the latest snapshot created by ClickHouse Keeper." };

    new_values["KeeperOpenFileDescriptorCount"] = { open_file_descriptor_count, "The number of open file descriptors in ClickHouse Keeper." };
    if (max_file_descriptor_count.has_value())
        new_values["KeeperMaxFileDescriptorCount"] = { *max_file_descriptor_count, "The maximum number of open file descriptors in ClickHouse Keeper." };
    else
        new_values["KeeperMaxFileDescriptorCount"] = { -1, "The maximum number of open file descriptors in ClickHouse Keeper." };

    new_values["KeeperFollowers"] = { followers, "The number of followers of ClickHouse Keeper." };
    new_values["KeeperSyncedFollowers"] = { synced_followers, "The number of followers of ClickHouse Keeper who are also in-sync." };
    new_values["KeeperZxid"] = { zxid, "The current transaction id number (zxid) in ClickHouse Keeper." };
    new_values["KeeperSessionWithWatches"] = { session_with_watches, "The number of client sessions of ClickHouse Keeper having watches." };
    new_values["KeeperPathsWatched"] = { paths_watched, "The number of different paths watched by the clients of ClickHouse Keeper." };

    auto keeper_log_info = keeper_dispatcher.getKeeperLogInfo();

    new_values["KeeperLastLogIdx"] = { keeper_log_info.last_log_idx, "Index of the last log stored in ClickHouse Keeper." };
    new_values["KeeperLastLogTerm"] = { keeper_log_info.last_log_term, "Raft term of the last log stored in ClickHouse Keeper." };

    new_values["KeeperLastCommittedLogIdx"] = { keeper_log_info.last_committed_log_idx, "Index of the last committed log in ClickHouse Keeper." };
    new_values["KeeperTargetCommitLogIdx"] = { keeper_log_info.target_committed_log_idx, "Index until which logs can be committed in ClickHouse Keeper." };
    new_values["KeeperLastSnapshotIdx"] = { keeper_log_info.last_snapshot_idx, "Index of the last log present in the last created snapshot." };

    new_values["KeeperLatestLogsCacheEntries"] = {keeper_log_info.latest_logs_cache_entries, "Number of entries stored in the in-memory cache for latest logs"};
    new_values["KeeperLatestLogsCacheSize"] = {keeper_log_info.latest_logs_cache_size, "Total size of in-memory cache for latest logs"};

    new_values["KeeperCommitLogsCacheEntries"] = {keeper_log_info.commit_logs_cache_entries, "Number of entries stored in the in-memory cache for next logs to be committed"};
    new_values["KeeperCommitLogsCacheSize"] = {keeper_log_info.commit_logs_cache_size, "Total size of in-memory cache for next logs to be committed"};

    auto & keeper_connection_stats = keeper_dispatcher.getKeeperConnectionStats();

    new_values["KeeperMinLatency"] = { keeper_connection_stats.getMinLatency(), "Minimal request latency of ClickHouse Keeper." };
    new_values["KeeperMaxLatency"] = { keeper_connection_stats.getMaxLatency(), "Maximum request latency of ClickHouse Keeper." };
    new_values["KeeperAvgLatency"] = { keeper_connection_stats.getAvgLatency(), "Average request latency of ClickHouse Keeper." };
    new_values["KeeperPacketsReceived"] = { keeper_connection_stats.getPacketsReceived(), "Number of packets received by ClickHouse Keeper." };
    new_values["KeeperPacketsSent"] = { keeper_connection_stats.getPacketsSent(), "Number of packets sent by ClickHouse Keeper." };
#endif
}

KeeperAsynchronousMetrics::KeeperAsynchronousMetrics(
    ContextPtr context_,
    unsigned update_period_seconds,
    const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
    bool update_jemalloc_epoch_,
    bool update_rss_)
    : AsynchronousMetrics(update_period_seconds, protocol_server_metrics_func_, update_jemalloc_epoch_, update_rss_)
    , context(std::move(context_))
{
}

KeeperAsynchronousMetrics::~KeeperAsynchronousMetrics()
{
    /// NOTE: stop() from base class is not enough, since this leads to leak on vptr
    stop();
}

void KeeperAsynchronousMetrics::updateImpl(TimePoint /*update_time*/, TimePoint /*current_time*/, bool /*force_update*/, bool /*first_run*/, AsynchronousMetricValues & new_values)
{
#if USE_NURAFT
    {
        auto keeper_dispatcher = context->tryGetKeeperDispatcher();
        if (keeper_dispatcher)
            updateKeeperInformation(*keeper_dispatcher, new_values);
    }
#endif
}

}
