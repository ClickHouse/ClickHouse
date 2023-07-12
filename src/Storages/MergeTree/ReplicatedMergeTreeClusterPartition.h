#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterReplica.h>
#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Representation of /block_numbers/$partition in case of cluster
class ReplicatedMergeTreeClusterPartition
{
public:
    using ResolveReplica = std::function<void(ReplicatedMergeTreeClusterReplica &)>;

    ReplicatedMergeTreeClusterPartition() = default;
    explicit ReplicatedMergeTreeClusterPartition(const String & partition_id_);
    ReplicatedMergeTreeClusterPartition(const String & partition_id_, const ReplicatedMergeTreeClusterReplicas & replicas_, int version_);

    static ReplicatedMergeTreeClusterPartition read(ReadBuffer & in, int version, const String & partition_id, const ResolveReplica & resolve_replica);
    static ReplicatedMergeTreeClusterPartition fromString(const String & str, int version, const String & partition_id, const ResolveReplica & resolve_replica);

    void write(WriteBuffer & out) const;
    String toString() const;
    String toStringForLog() const;

    int getVersion() const { return version; }

    const ReplicatedMergeTreeClusterReplicas & getReplicas() const { return replicas; }
    const Strings & getReplicasNames() const { return replicas_names; }
    const String & getPartitionId() const { return partition_id; }

private:
    /// NOTE: This are constant values, however, if you will declare them with const CV, you cannot copy the object.
    String partition_id;
    ReplicatedMergeTreeClusterReplicas replicas;
    Strings replicas_names;
    int version = -1;
};

using ReplicatedMergeTreeClusterPartitions = std::vector<ReplicatedMergeTreeClusterPartition>;

}
