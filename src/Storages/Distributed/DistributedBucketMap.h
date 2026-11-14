#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <base/defines.h>

#include <mutex>


namespace DB
{

/// Assignment of the buckets of a `Distributed` table to the shards of its cluster.
///
/// A row goes to the bucket `sharding_key % buckets`, and then to the shard the bucket is assigned to,
/// instead of the slot computed from the shard weights. The number of buckets is fixed, so moving
/// a bucket to another shard does not change the shard of the rows of the other buckets.
///
/// The map is stored in a single ZooKeeper node as text:
///
///     format version: 1
///     buckets: 256
///     0 127 1
///     128 255 2
///
/// Every line after the header assigns an inclusive range of buckets to a shard number (`shard_num`
/// in `system.clusters`). The ranges are listed in order and cover all buckets.
///
/// A line may end with `previous` and the shard numbers the range was assigned to before:
///
///     128 191 3 previous 2
///
/// Rows are inserted only into the shard of the range, but the rows inserted before stay on the previous shards,
/// so the queries that skip shards by the sharding key read the previous shards too.
struct DistributedBucketMap
{
    struct Range
    {
        UInt64 first = 0;
        UInt64 last = 0;
        UInt32 shard_num = 0;
        std::vector<UInt32> previous_shard_nums;
    };

    static constexpr UInt64 MAX_BUCKETS = 65536;

    UInt64 buckets = 0;
    std::vector<Range> ranges;

    /// Version of the ZooKeeper node the map was read from, -1 for a map that was not read from ZooKeeper.
    Int32 version = -1;

    /// The index of the shard of every bucket, in the same form as `Cluster::getSlotToShard`.
    Cluster::SlotToShard slot_to_shard;
    UInt32 max_shard_num = 0;

    /// Whether any range has previous shards.
    bool has_previous_shards = false;
    /// The index in `ranges` of the range of every bucket, in the same form as `slot_to_shard`.
    /// It is built only if `has_previous_shards` is set.
    Cluster::SlotToShard slot_to_range;

    /// Splits `buckets` into ranges of almost equal size, one for each of `num_shards` shards.
    static DistributedBucketMap createEven(UInt64 buckets, size_t num_shards);

    static DistributedBucketMap parse(const String & data);
    String toString() const;

private:
    void validateAndBuildSlots();
};

using DistributedBucketMapPtr = std::shared_ptr<const DistributedBucketMap>;


/// Keeps the latest version of the bucket map of a `Distributed` table by watching its ZooKeeper node.
class DistributedBucketMapWatcher : WithContext
{
public:
    /// `zookeeper_path_` may start with the name of an auxiliary ZooKeeper, as the path of `ReplicatedMergeTree`.
    DistributedBucketMapWatcher(ContextPtr context_, const StorageID & storage_id, const String & zookeeper_path_);

    /// Creates the map with `buckets` buckets split evenly between `num_shards` shards if the node does not exist.
    /// If the node exists, checks that its map has `buckets` buckets.
    void createIfNotExists(UInt64 buckets, size_t num_shards);

    void startup();
    void shutdown();

    /// Returns the latest loaded map. If the map has not been loaded yet, loads it from ZooKeeper and throws on failure.
    DistributedBucketMapPtr get();

    struct Status
    {
        DistributedBucketMapPtr map;
        std::exception_ptr last_exception;
    };
    Status getStatus() const;

    const String & getFullZooKeeperPath() const { return full_zookeeper_path; }

private:
    zkutil::ZooKeeperPtr getZooKeeper() const;

    /// Reads the map, sets a watch on its node and stores the map.
    DistributedBucketMapPtr load();

    /// The body of `task`.
    void run();

    const String full_zookeeper_path;
    const String zookeeper_name;
    const String zookeeper_path;
    const LoggerPtr log;

    /// Serializes the calls of `load`, so a later call always stores a map read later.
    std::mutex load_mutex;

    mutable std::mutex mutex;
    DistributedBucketMapPtr map TSA_GUARDED_BY(mutex);
    std::exception_ptr last_exception TSA_GUARDED_BY(mutex);

    /// Declared last: it is destroyed first, and its destructor waits for `run`, which uses the members above.
    BackgroundSchedulePoolTaskHolder task;
};

}
