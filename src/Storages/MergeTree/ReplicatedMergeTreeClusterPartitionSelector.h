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

/// Responsible for finding replicas for partition:
/// - during INSERT
/// - for rebalance
///
/// Right now this selector is very dumb:
/// - prefers replicas with smaller partitions on them
/// - prefers partitions with less replicas
///
/// While the following information should be also taken into account:
/// - partition size
/// - free space on replica
/// - log_pointer
/// - is replica alive (is_lost != 1)
/// But this will be implemented later.
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
