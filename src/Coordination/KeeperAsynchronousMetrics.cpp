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
    size_t latest_snapshot_size = 0;
    size_t open_file_descriptor_count = 0;
    size_t max_file_descriptor_count = 0;
    size_t followers = 0;
    size_t synced_followers = 0;
    size_t zxid = 0;
    size_t session_with_watches = 0;
    size_t paths_watched = 0;
    //size_t snapshot_dir_size = 0;
    //size_t log_dir_size = 0;

    if (keeper_dispatcher.isServerActive())
    {
        auto keeper_info = keeper_dispatcher.getKeeper4LWInfo();
        is_standalone = static_cast<size_t>(keeper_info.is_standalone);
        is_leader = static_cast<size_t>(keeper_info.is_leader);
        is_observer = static_cast<size_t>(keeper_info.is_observer);
        is_follower = static_cast<size_t>(keeper_info.is_follower);

        zxid = keeper_info.last_zxid;
        const auto & state_machine = keeper_dispatcher.getStateMachine();
        znode_count = state_machine.getNodesCount();
        watch_count = state_machine.getTotalWatchesCount();
        ephemerals_count = state_machine.getTotalEphemeralNodesCount();
        approximate_data_size = state_machine.getApproximateDataSize();
        key_arena_size = state_machine.getKeyArenaSize();
        latest_snapshot_size = state_machine.getLatestSnapshotBufSize();
        session_with_watches = state_machine.getSessionsWithWatchesCount();
        paths_watched = state_machine.getWatchedPathsCount();
        //snapshot_dir_size = keeper_dispatcher.getSnapDirSize();
        //log_dir_size = keeper_dispatcher.getLogDirSize();

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

    new_values["KeeperZnodeCount"] = { znode_count, "The number of nodes (data entries) in ClickHouse Keeper." };
    new_values["KeeperWatchCount"] = { watch_count, "The number of watches in ClickHouse Keeper." };
    new_values["KeeperEphemeralsCount"] = { ephemerals_count, "The number of ephemeral nodes in ClickHouse Keeper." };

    new_values["KeeperApproximateDataSize"] = { approximate_data_size, "The approximate data size of ClickHouse Keeper, in bytes." };
    new_values["KeeperKeyArenaSize"] = { key_arena_size, "The size in bytes of the memory arena for keys in ClickHouse Keeper." };
    new_values["KeeperLatestSnapshotSize"] = { latest_snapshot_size, "The uncompressed size in bytes of the latest snapshot created by ClickHouse Keeper." };

    new_values["KeeperOpenFileDescriptorCount"] = { open_file_descriptor_count, "The number of open file descriptors in ClickHouse Keeper." };
    new_values["KeeperMaxFileDescriptorCount"] = { max_file_descriptor_count, "The maximum number of open file descriptors in ClickHouse Keeper." };

    new_values["KeeperFollowers"] = { followers, "The number of followers of ClickHouse Keeper." };
    new_values["KeeperSyncedFollowers"] = { synced_followers, "The number of followers of ClickHouse Keeper who are also in-sync." };
    new_values["KeeperZxid"] = { zxid, "The current transaction id number (zxid) in ClickHouse Keeper." };
    new_values["KeeperSessionWithWatches"] = { session_with_watches, "The number of client sessions of ClickHouse Keeper having watches." };
    new_values["KeeperPathsWatched"] = { paths_watched, "The number of different paths watched by the clients of ClickHouse Keeper." };
    //new_values["KeeperSnapshotDirSize"] = { snapshot_dir_size, "The size of the snapshots directory of ClickHouse Keeper, in bytes." };
    //new_values["KeeperLogDirSize"] = { log_dir_size, "The size of the logs directory of ClickHouse Keeper, in bytes." };

    auto keeper_log_info = keeper_dispatcher.getKeeperLogInfo();

    new_values["KeeperLastLogIdx"] = { keeper_log_info.last_log_idx, "Index of the last log stored in ClickHouse Keeper." };
    new_values["KeeperLastLogTerm"] = { keeper_log_info.last_log_term, "Raft term of the last log stored in ClickHouse Keeper." };

    new_values["KeeperLastCommittedLogIdx"] = { keeper_log_info.last_committed_log_idx, "Index of the last committed log in ClickHouse Keeper." };
    new_values["KeeperTargetCommitLogIdx"] = { keeper_log_info.target_committed_log_idx, "Index until which logs can be committed in ClickHouse Keeper." };
    new_values["KeeperLastSnapshotIdx"] = { keeper_log_info.last_snapshot_idx, "Index of the last log present in the last created snapshot." };

    auto & keeper_connection_stats = keeper_dispatcher.getKeeperConnectionStats();

    new_values["KeeperMinLatency"] = { keeper_connection_stats.getMinLatency(), "Minimal request latency of ClickHouse Keeper." };
    new_values["KeeperMaxLatency"] = { keeper_connection_stats.getMaxLatency(), "Maximum request latency of ClickHouse Keeper." };
    new_values["KeeperAvgLatency"] = { keeper_connection_stats.getAvgLatency(), "Average request latency of ClickHouse Keeper." };
    new_values["KeeperPacketsReceived"] = { keeper_connection_stats.getPacketsReceived(), "Number of packets received by ClickHouse Keeper." };
    new_values["KeeperPacketsSent"] = { keeper_connection_stats.getPacketsSent(), "Number of packets sent by ClickHouse Keeper." };
#endif
}

KeeperAsynchronousMetrics::KeeperAsynchronousMetrics(
    ContextPtr context_, int update_period_seconds, const ProtocolServerMetricsFunc & protocol_server_metrics_func_)
    : AsynchronousMetrics(update_period_seconds, protocol_server_metrics_func_), context(std::move(context_))
{
}

void KeeperAsynchronousMetrics::updateImpl(AsynchronousMetricValues & new_values, TimePoint /*update_time*/, TimePoint /*current_time*/)
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
