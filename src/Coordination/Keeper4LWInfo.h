#pragma once

#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Keeper server related information for different 4lw commands
struct Keeper4LWInfo
{
    bool is_leader;
    bool is_observer;
    bool is_follower;
    bool is_standalone;

    bool has_leader;
    bool is_exceeding_mem_soft_limit;

    uint64_t alive_connections_count;
    uint64_t outstanding_requests_count;

    uint64_t follower_count;
    uint64_t synced_follower_count;

    String getRole() const
    {
        if (is_standalone)
            return "standalone";
        if (is_leader)
            return "leader";
        if (is_observer)
            return "observer";
        if (is_follower)
            return "follower";

        throw Exception(ErrorCodes::LOGICAL_ERROR, "RAFT server has undefined state, it's a bug");
    }
};

/// Keeper log information for 4lw commands
struct KeeperLogInfo
{
    /// My first log index in log store.
    uint64_t first_log_idx{0};

    /// My first log term.
    uint64_t first_log_term{0};

    /// My last log index in log store.
    uint64_t last_log_idx{0};

    /// My last log term.
    uint64_t last_log_term{0};

    /// My last committed log index in state machine.
    uint64_t last_committed_log_idx;

    /// Leader's committed log index from my perspective.
    uint64_t leader_committed_log_idx;

    /// Target log index should be committed to.
    uint64_t target_committed_log_idx;

    /// The largest committed log index in last snapshot.
    uint64_t last_snapshot_idx;

    uint64_t latest_logs_cache_entries;
    uint64_t latest_logs_cache_size;

    uint64_t commit_logs_cache_entries;
    uint64_t commit_logs_cache_size;
};

}
