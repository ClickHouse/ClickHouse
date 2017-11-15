#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/Types.h>
#include <common/logger_useful.h>
#include <thread>
#include <map>


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

private:
    StorageReplicatedMergeTree & storage;
    Logger * log;
    std::thread thread;

    void run();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// Remove old block hashes from ZooKeeper. This makes a leading replica.
    void clearOldBlocks();

    class NodesStatCache;
    struct NodeWithStat;
    std::unique_ptr<NodesStatCache> cached_block_stats;

    /// Returns list of blocks (with their stat) sorted by ctime in descending order
    void getBlocksSortedByTime(std::shared_ptr<zkutil::ZooKeeper> & zookeeper, std::vector<NodeWithStat> & timed_blocks);

    /// TODO Removing old quorum/failed_parts
    /// TODO Removing old nonincrement_block_numbers
};


}
