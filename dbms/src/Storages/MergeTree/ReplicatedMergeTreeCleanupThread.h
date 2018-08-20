#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <thread>
#include <map>
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

    void start()
    {
        task->activate();
        task->schedule();
    }

    void wakeup() { task->schedule(); }

    void stop() { task->deactivate(); }

private:
    StorageReplicatedMergeTree & storage;
    String log_name;
    Logger * log;
    BackgroundSchedulePool::TaskHolder task;
    pcg64 rng;

    void run();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// Remove old block hashes from ZooKeeper. This is done by the leader replica.
    void clearOldBlocks();

    /// Remove old mutations that are done from ZooKeeper. This is done by the leader replica.
    void clearOldMutations();

    using NodeCTimeCache = std::map<String, Int64>;
    NodeCTimeCache cached_block_stats;

    struct NodeWithStat;
    /// Returns list of blocks (with their stat) sorted by ctime in descending order.
    void getBlocksSortedByTime(zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks);

    /// TODO Removing old quorum/failed_parts
};


}
