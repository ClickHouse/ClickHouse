#pragma once

#include <common/types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>
#include <Common/randomSeed.h>
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
    ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void stop() { task->deactivate(); }

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;
    pcg64 rng{randomSeed()};

    void run();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// The replica is marked as "lost" if it is inactive and its log pointer
    ///  is far behind and we are not going to keep logs for it.
    /// Lost replicas will use different strategy for repair.
    void markLostReplicas(const std::unordered_map<String, UInt32> & host_versions_lost_replicas,
                          const std::unordered_map<String, String> & log_pointers_candidate_lost_replicas,
                          size_t replicas_count, const zkutil::ZooKeeperPtr & zookeeper);

    /// Remove old block hashes from ZooKeeper. This is done by the leader replica.
    void clearOldBlocks();

    /// Remove old mutations that are done from ZooKeeper. This is done by the leader replica.
    void clearOldMutations();

    using NodeCTimeAndVersionCache = std::map<String, std::pair<Int64, Int32>>;
    NodeCTimeAndVersionCache cached_block_stats;

    struct NodeWithStat;
    /// Returns list of blocks (with their stat) sorted by ctime in descending order.
    void getBlocksSortedByTime(zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks);

    /// TODO Removing old quorum/failed_parts
};


}
