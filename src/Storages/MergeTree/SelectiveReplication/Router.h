#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

class QueryPlan;
struct SelectQueryInfo;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
struct Settings;
class StorageReplicatedMergeTree;

namespace SelectiveReplication
{

/// Routes SELECT queries to replicas assigned to each partition under
/// selective replication. Extracted from `StorageReplicatedMergeTree` to
/// keep routing logic focused and testable, following the
/// `MergeTreeDataSelectExecutor` pattern: holds a storage reference and
/// is declared as `friend class` in `StorageReplicatedMergeTree`.
class Router
{
public:
    explicit Router(StorageReplicatedMergeTree & storage_);

    /// Route a SELECT to assigned replicas based on partition assignment.
    /// Called from `read` when `replication_factor > 0` and
    /// `processed_stage == WithMergeableState` (depth=0), and from
    /// `reEnterSelectiveRoutingAtDepth` when forwarding the misplaced
    /// subset (depth>0).
    ///
    /// When `override_assignment_map` is non-null, it replaces the
    /// snapshot's assignment_map for this invocation.
    void readWithSelectiveRouting(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams,
        const std::unordered_map<String, Strings> * override_assignment_map = nullptr);

    /// Re-enter selective routing at `distributed_depth > 0`.
    /// Verifies each received partition against the local cache and
    /// re-routes misplaced partitions to their current owner.
    void reEnterSelectiveRoutingAtDepth(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    /// Unified entry point for `StorageReplicatedMergeTree::getQueryProcessingStage`.
    /// Handles the parallel-replicas guard, the depth==0 path (full assignment check),
    /// and the depth>0 path (misplaced-partition detection). Returns the stage to use
    /// or `nullopt` to fall through to `MergeTreeData::getQueryProcessingStage`.
    std::optional<QueryProcessingStage::Enum> resolveQueryProcessingStage(
        ContextPtr query_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const;

    /// Determine whether any partition routes to a remote replica (depth==0 only).
    /// Returns `WithMergeableState` if remote routing is needed,
    /// `nullopt` to fall through to the default stage.
    std::optional<QueryProcessingStage::Enum> resolveSelectiveReplicationStage(
        ContextPtr query_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const;

    /// Check whether any received partition IDs are assigned to a
    /// different replica (depth>0 fast-path test for `getQueryProcessingStage`).
    bool hasMisplacedReceivedPartitions(
        const SelectQueryInfo & query_info,
        ContextPtr local_context) const;

    /// Extract partition IDs referenced by the `_partition_id` predicate
    /// in the query filter.
    std::unordered_set<String> extractPartitionIdsFromQueryInfo(
        const SelectQueryInfo & query_info,
        ContextPtr local_context) const;

    /// Get sorted list of all replica names from ZK.
    Strings getAllReplicaNames() const;

    /// Get sorted list of active replica names from ZK.
    Strings getActiveReplicaNames(const zkutil::ZooKeeperPtr & zk) const;

    /// Batch-read replica host addresses from ZK.
    std::unordered_map<String, ReplicatedMergeTreeAddress> getReplicaAddresses(
        const Strings & replicas, const zkutil::ZooKeeperPtr & zk) const;

    /// Enrich a storage snapshot with selective replication assignment data.
    /// Fetches assignments from ZooKeeper and stores them in SnapshotData.
    /// Errors are logged but do not prevent snapshot creation.
    void enrichSnapshotWithAssignments(StorageSnapshotPtr & snapshot) const;

    /// Build CAS guard for REPLACE PARTITION: read assignment, check version, determine if locally assigned.
    struct ReplacePartitionCASGuard
    {
        bool this_replica_is_assigned = true;
        int32_t assignment_cas_version = -1;
    };

    ReplacePartitionCASGuard buildReplacePartitionCASGuard(
        const zkutil::ZooKeeperPtr & zk,
        const String & partition_id) const;

private:
    StorageReplicatedMergeTree & storage;
    LoggerPtr log;

    /// TTL-cached replica address map to reduce ZooKeeper round-trips
    /// during routing.
    struct CachedAddresses
    {
        std::unordered_map<String, ReplicatedMergeTreeAddress> addresses;
        std::chrono::steady_clock::time_point last_refresh;
    };

    mutable std::mutex address_cache_mutex;
    mutable CachedAddresses cached_addresses;
    static constexpr std::chrono::seconds ADDRESS_CACHE_TTL{30};

    /// Fetch replica addresses with TTL caching.
    std::unordered_map<String, ReplicatedMergeTreeAddress> getReplicaAddressesCached(
        const Strings & replicas) const;

    /// Build replica -> partition_ids routing map from the assignment data.
    std::unordered_map<String, Strings> buildPartitionRoutingMap(
        const std::unordered_map<String, Strings> & assignment_map,
        std::unordered_set<String> & local_partition_ids_set,
        const std::unordered_set<String> * pid_restriction = nullptr) const;

    /// Redistribute partitions from replicas with unavailable ZK addresses.
    void redistributeFailedReplicas(
        std::unordered_map<String, Strings> & partitions_by_replica,
        const std::unordered_map<String, ReplicatedMergeTreeAddress> & address_map,
        const std::unordered_map<String, Strings> & assignment_map,
        const std::unordered_set<String> & local_partition_ids_set,
        const Settings & settings) const;

    /// Detect replicas whose resolved address points back to the local
    /// host:port (self-loop). Extracted from duplicated logic in
    /// `hasMisplacedReceivedPartitions` and `reEnterSelectiveRoutingAtDepth`.
    std::unordered_set<String> detectSelfLoopReplicas(
        const std::unordered_set<String> & candidate_replicas) const;

    /// Find a sibling storage in the same process sharing our zookeeper_path
    /// but with the given replica_name (for single-process self-loop fallback).
    /// Returns a shared_ptr to keep the storage alive across usage.
    std::shared_ptr<StorageReplicatedMergeTree> findSiblingStorage(const String & target_replica_name) const;
};

}
}
