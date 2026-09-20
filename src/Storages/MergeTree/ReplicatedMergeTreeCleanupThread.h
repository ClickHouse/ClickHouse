#pragma once

#include <Storages/MergeTree/IMergeTreeCleanupThread.h>
#include <Common/Logger_fwd.h>

namespace zkutil
{

class ZooKeeper;
using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;

}

namespace DB
{

class StorageReplicatedMergeTree;

class ReplicatedMergeTreeCleanupThread : public IMergeTreeCleanupThread
{
public:
    explicit ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_);

    struct NodeCacheEntry
    {
        Int64 ctime = 0;
        Int64 czxid = 0;
        Int32 version = 0;
    };
    using NodeCTimeAndVersionCache = std::map<String, NodeCacheEntry>;
    /// Remove old block hashes from ZooKeeper. This is done by the leader replica. Returns the number of removed blocks
    static size_t clearOldBlocks(
        const String & zookeeper_path,
        const String & blocks_dir_name,
        zkutil::ZooKeeper & zookeeper,
        UInt64 window_seconds,
        UInt64 window_size,
        NodeCTimeAndVersionCache & cached_block_stats,
        LoggerPtr log_);

private:
    StorageReplicatedMergeTree & storage;

    /// Returns a number this is directly proportional to the number of cleaned up blocks
    Float32 iterate() override;

    /// Remove old records from ZooKeeper. Returns the number of removed logs
    size_t clearOldLogs();

    /// The replica is marked as "lost" if it is inactive and its log pointer
    ///  is far behind and we are not going to keep logs for it.
    /// Lost replicas will use different strategy for repair.
    void markLostReplicas(
        const std::unordered_map<String, UInt32> & host_versions_lost_replicas,
        const std::unordered_map<String, String> & log_pointers_candidate_lost_replicas,
        size_t replicas_count,
        const zkutil::ZooKeeperPtr & zookeeper);

    /// Remove old mutations that are done from ZooKeeper. This is done by the leader replica. Returns the number of removed mutations
    size_t clearOldMutations();

    NodeCTimeAndVersionCache cached_stats_for_insert_deduplication_hashes;
    NodeCTimeAndVersionCache cached_block_stats_for_sync_inserts;
    NodeCTimeAndVersionCache cached_block_stats_for_async_inserts;

    struct NodeWithStat;
    /// Returns list of blocks (with their stat) sorted by ctime in descending order.
    static void getBlocksSortedByTime(
        const String & zookeeper_path,
        const String & blocks_dir_name,
        zkutil::ZooKeeper & zookeeper,
        std::vector<NodeWithStat> & timed_blocks,
        NodeCTimeAndVersionCache & cached_block_stats,
        LoggerPtr log_);

    /// TODO Removing old quorum/failed_parts
};

}
