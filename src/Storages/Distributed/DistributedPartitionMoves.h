#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <base/defines.h>

#include <mutex>
#include <unordered_map>


namespace DB
{

/// Partitions of the tables under a `Distributed` table that are being moved from one shard to another.
///
/// While a partition is moved, both the source and the destination shard have a copy of it. A query must read only
/// one copy, or it reads the rows twice, so every move says which copy is read, and queries skip the other one.
/// The moves are stored in a single ZooKeeper node as text:
///
///     format version: 1
///     202401 1 3 source
///     202402 2 3 destination
///
/// Every line after the header is a move: the partition ID, the source and the destination shard numbers
/// (`shard_num` in `system.clusters`), and the copy that queries read, `source` or `destination`.
/// A partition can be moved by one move at a time.
struct DistributedPartitionMoves
{
    enum class ReadFrom : uint8_t
    {
        Source,
        Destination,
    };

    struct Move
    {
        String partition_id;
        UInt32 source_shard_num = 0;
        UInt32 destination_shard_num = 0;
        ReadFrom read_from = ReadFrom::Source;
    };

    static constexpr auto EMPTY = "format version: 1\n";

    std::vector<Move> moves;

    /// Version of the ZooKeeper node the moves were read from.
    Int32 version = -1;

    /// The partitions that queries skip, by shard number.
    std::unordered_map<UInt32, Strings> partitions_to_skip_by_shard;
    UInt32 max_shard_num = 0;

    static DistributedPartitionMoves parse(const String & data);
};

using DistributedPartitionMovesPtr = std::shared_ptr<const DistributedPartitionMoves>;


/// Keeps the latest version of the partition moves of a `Distributed` table by watching their ZooKeeper node.
class DistributedPartitionMovesWatcher : WithContext
{
public:
    /// `zookeeper_path_` may start with the name of an auxiliary ZooKeeper, as the path of `ReplicatedMergeTree`.
    DistributedPartitionMovesWatcher(ContextPtr context_, const StorageID & storage_id, const String & zookeeper_path_);

    /// Creates the node without moves if it does not exist.
    void createIfNotExists();

    void startup();
    void shutdown();

    /// Returns the latest loaded moves. If they have not been loaded yet, loads them from ZooKeeper and throws on failure.
    DistributedPartitionMovesPtr get();

    struct Status
    {
        DistributedPartitionMovesPtr moves;
        std::exception_ptr last_exception;
    };
    Status getStatus() const;

    const String & getFullZooKeeperPath() const { return full_zookeeper_path; }

private:
    zkutil::ZooKeeperPtr getZooKeeper() const;

    /// Reads the moves, sets a watch on their node and stores the moves.
    DistributedPartitionMovesPtr load();

    /// The body of `task`.
    void run();

    const String full_zookeeper_path;
    const String zookeeper_name;
    const String zookeeper_path;
    const LoggerPtr log;

    /// Serializes the calls of `load`, so a later call always stores moves read later.
    std::mutex load_mutex;

    mutable std::mutex mutex;
    DistributedPartitionMovesPtr moves TSA_GUARDED_BY(mutex);
    std::exception_ptr last_exception TSA_GUARDED_BY(mutex);

    /// Declared last: it is destroyed first, and its destructor waits for `run`, which uses the members above.
    BackgroundSchedulePoolTaskHolder task;
};

}
