#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/randomSeed.h>
#include <Common/Stopwatch.h>
#include <Common/ActionBlocker.h>
#include <Core/BackgroundSchedulePool.h>
#include <thread>

#include <map>
#include <unordered_map>
#include <pcg_random.hpp>


namespace DB
{

class StorageReplicatedMergeTree;


/** Removes obsolete data from a table of type ReplicatedMergeTree.
  */
class ReplicatedMergeTreeCleanupThread
{
public:
    explicit ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void stop() { task->deactivate(); }

    void wakeupEarlierIfNeeded();

    ActionLock getCleanupLock() { return cleanup_blocker.cancel(); }

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    LoggerPtr log;
    BackgroundSchedulePool::TaskHolder task;
    pcg64 rng{randomSeed()};

    UInt64 sleep_ms;

    std::atomic<UInt64> prev_cleanup_timestamp_ms = 0;
    std::atomic<bool> is_running = false;

    AtomicStopwatch wakeup_check_timer;

    ActionBlocker cleanup_blocker;

    void run();

    /// Returns a number this is directly proportional to the number of cleaned up blocks
    Float32 iterate();

    /// Remove old records from ZooKeeper. Returns the number of removed logs
    size_t clearOldLogs();

    /// The replica is marked as "lost" if it is inactive and its log pointer
    ///  is far behind and we are not going to keep logs for it.
    /// Lost replicas will use different strategy for repair.
    void markLostReplicas(const std::unordered_map<String, UInt32> & host_versions_lost_replicas,
                          const std::unordered_map<String, String> & log_pointers_candidate_lost_replicas,
                          size_t replicas_count, const zkutil::ZooKeeperPtr & zookeeper);

    using NodeCTimeAndVersionCache = std::map<String, std::pair<Int64, Int32>>;
    /// Remove old block hashes from ZooKeeper. This is done by the leader replica. Returns the number of removed blocks
    size_t clearOldBlocks(const String & blocks_dir_name, UInt64 window_seconds, UInt64 window_size, NodeCTimeAndVersionCache & cached_block_stats);

    /// Remove old mutations that are done from ZooKeeper. This is done by the leader replica. Returns the number of removed mutations
    size_t clearOldMutations();

    NodeCTimeAndVersionCache cached_block_stats_for_sync_inserts;
    NodeCTimeAndVersionCache cached_block_stats_for_async_inserts;

    struct NodeWithStat;
    /// Returns list of blocks (with their stat) sorted by ctime in descending order.
    void getBlocksSortedByTime(const String & blocks_dir_name, zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks, NodeCTimeAndVersionCache & cached_block_stats);

    /// TODO Removing old quorum/failed_parts
};


}
