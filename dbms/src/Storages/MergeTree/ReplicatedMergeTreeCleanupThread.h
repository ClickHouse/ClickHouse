#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>
#include <Common/BackgroundSchedulePool.h>
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

    ~ReplicatedMergeTreeCleanupThread();

    void schedule() { task_handle->schedule(); }

private:
    StorageReplicatedMergeTree & storage;
    Logger * log;
    BackgroundSchedulePool::TaskHandle task_handle;
    pcg64 rng;

    void run();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// Remove old block hashes from ZooKeeper. This is done by the leader replica.
    void clearOldBlocks();

    using NodeCTimeCache = std::map<String, Int64>;
    NodeCTimeCache cached_block_stats;

    struct NodeWithStat;
    /// Returns list of blocks (with their stat) sorted by ctime in descending order.
    void getBlocksSortedByTime(zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks);

    /// TODO Removing old quorum/failed_parts
    /// TODO Removing old nonincrement_block_numbers
};


}
