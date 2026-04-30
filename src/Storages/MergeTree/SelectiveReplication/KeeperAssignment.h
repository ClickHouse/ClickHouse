#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <unordered_map>

#include <mutex>
#include <atomic>
#include <chrono>
#include <optional>

namespace DB
{

struct ReplicatedMergeTreeLogEntryData;
struct ReplicatedMergeTreeLogEntry;
using ReplicatedMergeTreeLogEntryPtr = std::shared_ptr<ReplicatedMergeTreeLogEntry>;

/// Keeper-based replica assignment for selective replication.
/// Assignments stored at `{zk_path}/selective/assignments/{partition_id}`.
/// New partitions allocated with least-loaded-first + CAS-create.
///
/// Read paths (SELECT, queue filter, merge picker) use a TTL-based cache.
/// Write paths (INSERT commit) use CAS version checks against ZK, independent of cache.
/// Cache is refreshed after `pullLogsToQueue` and on-demand when expired.
/// Only one thread refreshes at a time; others use the stale cache.
class KeeperReplicaAssignment
{
public:
    KeeperReplicaAssignment(const String & zookeeper_path_, UInt64 cache_ttl_seconds);

    /// Cached per-partition data: assigned replicas and ZK node version.
    struct CachedEntry
    {
        Strings replicas;
        int32_t version = -1;
    };

    /// Unified assignment read.
    ///
    /// @param zk            ZooKeeper connection. May be nullptr when force_refresh=false
    ///                      (pure cache mode); miss entries are simply absent from the result.
    /// @param partition_ids  Partitions to query. Empty = all partitions.
    /// @param force_refresh  true  → always read from ZK (zk required).
    ///                      false → cache first; ZK fallback on miss if zk is provided.
    ///
    /// Semantics by argument combination:
    ///   zk=nullptr, force=false → pure in-memory cache. Cold cache → empty map.
    ///   zk=valid,   force=false → cache first; expired or missing entries fetched from ZK.
    ///   zk=valid,   force=true  → always reads ZK and updates the cache.
    ///   zk=nullptr, force=true  → throws (programming error).
    ///
    /// When partition_ids is empty and force_refresh=false, an expired cache triggers
    /// a full ZK refresh (if zk is provided), subject to thundering-herd protection.
    /// When partition_ids is non-empty and force_refresh=false, individual entries
    /// present in cache are returned regardless of TTL (caller should have pre-warmed).
    ///
    /// Thread-safe. ZK reads happen outside cache_mutex.
    std::unordered_map<String, CachedEntry> getAssignments(
        const zkutil::ZooKeeperPtr & zk,
        const std::vector<String> & partition_ids = {},
        bool force_refresh = false);

    struct BatchAllocationResult
    {
        /// partition_id -> assigned replicas (newly allocated in this call)
        std::unordered_map<String, Strings> assignments;
        /// partition_id -> assigned replicas (already existed before this call)
        std::unordered_map<String, Strings> existing;
    };

    /// Allocate partitions using least-loaded-first + CAS-create.
    /// Uses batched multi-ops. Falls back to per-partition allocation on conflicts.
    BatchAllocationResult allocatePartitions(
        const zkutil::ZooKeeperPtr & zk,
        const std::vector<String> & partition_ids,
        const Strings & all_replicas,
        UInt64 replication_factor);

    void removePartition(const zkutil::ZooKeeperPtr & zk, const String & partition_id);

    /// Read the `/selective/counts` node.
    /// Returns replica_name -> partition_count. Returns empty map if node does not exist.
    std::unordered_map<String, size_t> readCounts(const zkutil::ZooKeeperPtr & zk) const;

    /// Initialize `/selective/counts` from the current assignments.
    /// Called once during table startup. Uses CAS-create so only the first replica to call wins.
    /// If the node already exists, this is a no-op.
    void initializeCounts(const zkutil::ZooKeeperPtr & zk);

    /// Parse counts node data: "format version: 1\nr1:10,r2:20,..."
    static std::unordered_map<String, size_t> parseCounts(const String & data);

    /// Serialize counts to node data.
    static String serializeCounts(const std::unordered_map<String, size_t> & counts);

    static String stripCloningSuffix(const String & replica_name);
    static bool hasCloningSuffix(const String & replica_name);

    /// Check whether `replica_name` appears in `assigned_replicas` (stripping `:cloning` suffix).
    static bool isReplicaAssigned(const Strings & assigned_replicas, const String & replica_name);

    struct ComputedAssignment
    {
        Strings replicas;      /// Sorted assigned replicas
        String serialized;     /// ZK-ready serialization (via serializeAssignment)
    };

    /// Pure computation: select the top-rf replicas for a partition using
    /// least-loaded-first ordering with hash-based rotation tie-breaking.
    /// Does NOT touch ZooKeeper or the cache.
    static ComputedAssignment computeAssignment(
        const String & partition_id,
        const Strings & all_replicas,
        const std::unordered_map<String, size_t> & partition_counts,
        const std::unordered_map<String, size_t> & replica_positions,
        UInt64 rf);

    /// Parse/serialize assignment data strings ("format version: 1\nreplica1,replica2,...").
    static Strings parseAssignment(const String & data);
    static String serializeAssignment(const Strings & replicas);

    /// Append ZK ops to create the `/selective` subtree (for atomic table creation in multi-op).
    static void appendCreateSelectiveOps(
        Coordination::Requests & ops,
        const String & zookeeper_path,
        UInt64 replication_factor);

    /// Create the `/selective` subtree asynchronously (for lazy node creation on existing tables).
    static void asyncCreateSelectiveNodes(
        const zkutil::ZooKeeperPtr & zookeeper,
        const String & zookeeper_path,
        UInt64 replication_factor,
        std::vector<zkutil::ZooKeeper::FutureCreate> & futures);

    /// Get the target replicas for merge strategy filtering.
    /// Returns assigned replicas for the entry's partition, or empty if
    /// selective replication is disabled or assignment is unknown.
    /// Used by ReplicatedMergeTreeMergeStrategyPicker::pickReplicaToExecuteMerge.
    static Strings getTargetReplicasForMerge(
        const std::shared_ptr<KeeperReplicaAssignment> & assignment,
        MergeTreeDataFormatVersion format_version,
        const ReplicatedMergeTreeLogEntryData & entry,
        UInt64 replication_factor,
        LoggerPtr log);

    /// Check if a replica should skip a log entry based on selective assignment.
    /// Returns true if the replica is NOT assigned (should skip).
    /// Core logic extracted from ReplicatedMergeTreeQueue::shouldSkipForSelectiveReplication.
    static bool shouldSkipEntry(
        const std::shared_ptr<KeeperReplicaAssignment> & assignment,
        const ReplicatedMergeTreeLogEntryData & entry,
        MergeTreeDataFormatVersion format_version,
        const String & replica_name,
        const zkutil::ZooKeeperPtr & zk,
        LoggerPtr log);

    /// Pre-populate assignment cache for all partitions in the given entries.
    /// Extracted from ReplicatedMergeTreeQueue::pullLogsToQueue.
    static void ensureCacheForEntries(
        const std::shared_ptr<KeeperReplicaAssignment> & assignment,
        const std::vector<ReplicatedMergeTreeLogEntryPtr> & entries,
        MergeTreeDataFormatVersion format_version,
        const zkutil::ZooKeeperPtr & zk);

    /// Filter source_replica_parts for cloneReplica: skip parts for unassigned partitions.
    /// Returns parts that should be added to get_part_set.
    static Strings filterPartsForClone(
        const std::unordered_map<String, CachedEntry> & selective_assignments,
        const Strings & source_replica_parts,
        MergeTreeDataFormatVersion format_version,
        const String & replica_name,
        LoggerPtr log);

    /// Ensure a partition assignment exists in ZK.
    /// Creates it via allocatePartitions if missing.
    /// Returns true if selective replication is active.
    bool ensurePartitionAssignment(
        const zkutil::ZooKeeperPtr & zk,
        const String & partition_id,
        const Strings & all_replicas,
        UInt64 replication_factor,
        LoggerPtr logger,
        const String & operation_name);

    /// Batch version of ensurePartitionAssignment.
    void ensurePartitionAssignments(
        const zkutil::ZooKeeperPtr & zk,
        const std::vector<String> & partition_ids,
        const Strings & all_replicas,
        UInt64 replication_factor,
        LoggerPtr logger,
        const String & operation_name);

    /// Validate that local replication_factor matches ZK config.
    /// Used during createReplicaAttempt and startup.
    static void validateReplicationFactorConsistency(
        const zkutil::ZooKeeperPtr & zk,
        const String & zookeeper_path,
        UInt64 local_rf,
        int32_t num_existing_replicas);

    /// Validate replication_factor on startup against ZK.
    static void validateReplicationFactorOnStartup(
        const zkutil::ZooKeeperPtr & zk,
        const String & zookeeper_path,
        UInt64 local_rf);

private:
    static constexpr int ASSIGNMENT_FORMAT_VERSION = 1;

    const std::chrono::seconds cache_ttl;

    struct CachedAssignment
    {
        std::unordered_map<String, CachedEntry> partitions;
        std::chrono::steady_clock::time_point last_refresh;
        bool empty() const { return partitions.empty(); }
    };

    String zookeeper_path;
    LoggerPtr log = getLogger("KeeperReplicaAssignment");

    mutable std::mutex cache_mutex;
    mutable CachedAssignment cached TSA_GUARDED_BY(cache_mutex);
    mutable std::atomic<bool> cache_refreshing{false};

    /// Read all assignments from ZK. Does NOT update cache.
    std::unordered_map<String, CachedEntry> readAllAssignmentsFromZK(const zkutil::ZooKeeperPtr & zk) const;

    /// Read specific partitions from ZK via batch tryGet. Does NOT update cache.
    std::unordered_map<String, CachedEntry> readPartitionsFromZK(
        const zkutil::ZooKeeperPtr & zk, const std::vector<String> & partition_ids) const;

    /// Allocate a single partition using least-loaded-first + CAS-create. Used by allocatePartitions as fallback.
    Strings allocateSinglePartition(
        const zkutil::ZooKeeperPtr & zk,
        const String & partition_id,
        const Strings & all_replicas,
        UInt64 replication_factor);

    std::unordered_map<String, size_t> readReplicaPartitionCounts(
        const zkutil::ZooKeeperPtr & zk) const;
};

}
