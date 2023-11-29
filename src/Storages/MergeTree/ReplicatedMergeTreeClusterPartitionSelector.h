#pragma once

#include <optional>
#include <base/types.h>
#include <Core/Types_fwd.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include "Common/ZooKeeper/ZooKeeper.h"

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
    explicit ReplicatedMergeTreeClusterPartitionSelector(zkutil::ZooKeeperPtr zookeeper_, ReplicatedMergeTreeCluster & cluster_);

    std::optional<ReplicatedMergeTreeClusterPartition> select();

    /// @return new replicas on INSERT
    Strings allocatePartition();

private:
    ReplicatedMergeTreeCluster & cluster;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;
    zkutil::ZooKeeperPtr zookeeper;
};

}
