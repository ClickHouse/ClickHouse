#pragma once

#include <optional>
#include <base/types.h>
#include <Core/Types_fwd.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>

namespace Poco
{
class Logger;
};

namespace DB
{

class ReplicatedMergeTreeCluster;
class StorageReplicatedMergeTree;

/// Responsible for finding new place for partition:
/// - during INSERT
/// - for rebalance
class ReplicatedMergeTreeClusterPartitionSelector
{
public:
    explicit ReplicatedMergeTreeClusterPartitionSelector(ReplicatedMergeTreeCluster & cluster_);

    std::optional<ReplicatedMergeTreeClusterPartition> select();

    /// @return new replicas on INSERT
    Strings allocatePartition();

private:
    ReplicatedMergeTreeCluster & cluster;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;
};

}
