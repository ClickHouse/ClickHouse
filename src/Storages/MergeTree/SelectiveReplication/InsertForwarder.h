#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/Names.h>
#include <Client/ConnectionPool.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace SelectiveReplication
{

/// Forwards INSERT blocks to assigned replicas under selective replication.
/// Extracted from `ReplicatedMergeTreeSink` to keep forwarding logic focused,
/// following the `Router` / `OptimizeForwarder` pattern: holds a storage
/// reference and is declared as `friend class` in `StorageReplicatedMergeTree`.
class InsertForwarder
{
public:
    explicit InsertForwarder(StorageReplicatedMergeTree & storage_);

    struct ForwardingPlan
    {
        BlocksWithPartition local_blocks;
        std::vector<BlockWithPartition> remote_blocks_storage;
        std::unordered_map<String, std::vector<size_t>> blocks_by_replica;
        std::unordered_map<String, Strings> replicas_to_try_map;
        std::unordered_map<String, ReplicatedMergeTreeAddress> address_map;
        /// Full assignment map (partition_id -> replicas) used for quorum validation.
        std::unordered_map<String, Strings> assignments;
    };

    /// Phase 1: allocate partitions, separate local vs remote, build plan.
    ForwardingPlan planForwardByAssignment(
        BlocksWithPartition & blocks,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context);

    /// Execute forwarding: send remote blocks to assigned replicas.
    void executeForwarding(
        const ForwardingPlan & plan,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const String & log_prefix);

    /// Phase 2: re-route for assignment-race partitions.
    /// Takes a fresh ZK connection and partition IDs to re-allocate.
    ForwardingPlan planReForward(
        BlocksWithPartition & blocks,
        const zkutil::ZooKeeperPtr & zk,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context);

    /// Validate that insert_quorum does not exceed replication_factor.
    /// Throws TOO_FEW_LIVE_REPLICAS if violated.
    static void validateQuorumVsReplicationFactor(
        UInt64 quorum_size,
        UInt64 replication_factor);

    /// Validate that each local partition has enough assigned replicas for quorum.
    void validateQuorumForAssignments(
        const std::unordered_map<String, Strings> & assignments,
        const BlocksWithPartition & local_blocks,
        bool quorum_enabled,
        size_t quorum_size) const;

    /// Resolve the ZK node version for CAS check on the assignment node.
    /// Returns the version. Throws AssignmentChangedException if not assigned.
    int32_t getAssignmentCASVersion(
        const String & partition_id,
        const String & replica_name,
        const zkutil::ZooKeeperPtr & zk) const;

    /// Add a CAS check operation to an existing multi-op batch.
    /// Appends a makeCheckRequest for /selective/assignments/{partition_id}.
    void addAssignmentCASCheck(
        Coordination::Requests & ops,
        const MergeTreeData::MutableDataPartPtr & part,
        const String & replica_name,
        const zkutil::ZooKeeperPtr & zk) const;

    enum class CASMismatchAction
    {
        RETRY,          /// Still assigned, retry with fresh version
        REASSIGN,       /// No longer assigned, throw AssignmentChangedException
    };

    /// Handle ZBADVERSION on the selective assignment CAS check during commitPart.
    /// Returns the action the caller should take.
    CASMismatchAction handleCASVersionMismatch(
        const String & partition_id,
        const String & replica_name,
        LoggerPtr logger) const;

    enum class CASFailureResult
    {
        RETRY,      /// Caller should return CommitRetryContext::LOCK_AND_COMMIT
        REASSIGN,   /// Caller should throw AssignmentChangedException
    };

    /// Handles ZK ZBADVERSION on the selective/assignments/ path during commitPart.
    /// On RETRY: rolls back the transaction and renames the part back to temp.
    /// On REASSIGN: rolls back the transaction.
    /// Returns the action the caller should take.
    CASFailureResult handleCommitCASFailure(
        MergeTreeData::MutableDataPartPtr & part,
        MergeTreeData::Transaction & transaction,
        const String & initial_part_name,
        const String & temporary_part_relative_path,
        const String & replica_name,
        LoggerPtr logger);

    /// Phase 2: handle blocks whose local commit failed due to assignment race.
    /// Extracted from ReplicatedMergeTreeSink::handleAssignmentRace.
    struct AssignmentFailure
    {
        BlockWithPartition block_with_partition;
        String partition_id;
    };

    void handleAssignmentRace(
        std::vector<AssignmentFailure> & assignment_failures,
        const zkutil::ZooKeeperPtr & zk,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        std::function<MergeTreeTemporaryPartPtr(BlockWithPartition &)> write_temp_part,
        std::function<void(std::vector<DelayedPartInPartition> &, const ZooKeeperWithFaultInjectionPtr &, std::vector<AssignmentFailure> *)> finish_parts,
        const DeduplicationInfo::Ptr & deduplication_info,
        LoggerPtr logger);

private:
    StorageReplicatedMergeTree & storage;
    LoggerPtr log;

    /// Core plan builder shared by Phase 1 and Phase 2.
    ForwardingPlan buildForwardingPlan(
        BlocksWithPartition & blocks,
        const std::unordered_map<String, Strings> & assignments,
        const zkutil::ZooKeeperPtr & zk,
        const StorageMetadataPtr & metadata_snapshot);

    /// Check if `replica_name` is in the given assignment list.
    bool isAssignedToSelf(const Strings & assigned) const;

    /// Create connection pool for a remote replica.
    ConnectionPoolPtr createForwardingConnectionPool(
        const ReplicatedMergeTreeAddress & address,
        ContextPtr context) const;
};

}
}
