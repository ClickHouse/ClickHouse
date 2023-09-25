#pragma once

#include <Core/Types.h>
#include <vector>

namespace Coordination
{
struct Stat;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

enum ReplicatedMergeTreeClusterPartitionState
{
    /// FIXME: is naming OK?
    UP_TO_DATE,
    MIGRATING,
    CLONING,
    DROPPING,
};

/// Representation of /block_numbers/$partition in case of cluster
///
/// Example:
///
///     cluster partition format version: 1
///     state: UP_TO_DATE
///     all_replicas (2):
///     1
///     2
///     active_replicas (0):
///     source_replica:
///     new_replica:
///     drop_replica:
///
///
class ReplicatedMergeTreeClusterPartition
{
public:
    ReplicatedMergeTreeClusterPartition() = default;
    explicit ReplicatedMergeTreeClusterPartition(const String & partition_id_);
    ReplicatedMergeTreeClusterPartition(const String & partition_id_, const Strings & replicas);

    static ReplicatedMergeTreeClusterPartition read(ReadBuffer & in, const Coordination::Stat & stat, const String & partition_id);
    static ReplicatedMergeTreeClusterPartition fromString(const String & str, const Coordination::Stat & stat, const String & partition_id);

    void write(WriteBuffer & out) const;
    String toString() const;
    String toStringForLog() const;

    int getVersion() const { return version; }
    void incrementVersion() { ++version; }

    const Strings & getAllReplicas() const { return all_replicas; }
    const Strings & getActiveReplicas() const { return active_replicas; }
    const Strings & getActiveNonMigrationReplicas() const { return active_non_migration_replicas; }
    const Strings & getAllNonMigrationReplicas() const { return all_non_migration_replicas; }
    const String & getDropReplica() const { return drop_replica; }
    const String & getNewReplica() const { return new_replica; }
    const String & getSourceReplica() const { return source_replica; }

    /// FIXME: isUnderMigrationOrCloning()/isUpToDate()/... ?
    bool isUnderReSharding() const { return state != UP_TO_DATE; }

    bool hasReplica(const String & replica) const;
    void removeReplica(const String & replica);

    /// MIGRATING
    void replaceReplica(const String & src, const String & dest);
    /// CLONING
    void addReplica(const String & src, const String & dest);
    /// DROPPING
    void dropReplica(const String & replica);

    /// For migration/cloning
    void finish();
    void revert();

    const String & getPartitionId() const { return partition_id; }
    ReplicatedMergeTreeClusterPartitionState getState() const { return state; }

    Int64 getModificationTimeMs() const { return modification_time_ms; }

private:
    ReplicatedMergeTreeClusterPartition(
        const String & partition_id_,
        ReplicatedMergeTreeClusterPartitionState state_,
        const Strings & all_replicas_,
        const Strings & active_replicas_,
        const String & source_replica_,
        const String & new_replica_,
        const String & drop_replica_,
        const Coordination::Stat & stat);

    /// NOTE: This are constant values, however, if you will declare them with const CV, you cannot copy the object.
    String partition_id;

    ReplicatedMergeTreeClusterPartitionState state = UP_TO_DATE;

    /// List of all replicas -- should be used for filtered replication
    Strings all_replicas;

    /// List of active replicas -- only this replicas are allowed to be used for SELECT
    Strings active_replicas;

    /// List of replicas that are not participate in migration:
    ///
    ///     active_replicas - source_replica
    ///
    /// P.S. and of course does not include new_replica as well
    Strings active_non_migration_replicas;
    Strings all_non_migration_replicas;

    /// If replica is under migration:
    String source_replica;
    String new_replica;
    String drop_replica;

    int version = -1;
    Int64 modification_time_ms = 0;
};

using ReplicatedMergeTreeClusterPartitions = std::vector<ReplicatedMergeTreeClusterPartition>;

}
