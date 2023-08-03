#pragma once

#include <optional>
#include <base/types.h>
#include <Core/Types_fwd.h>

namespace Poco
{
class Logger;
};

namespace DB
{

struct ReplicatedClusterMigratePartition
{
    String partition;
    String source_replica;
    int partition_version = 0;

    explicit operator bool() const;
};

class ReplicatedMergeTreeCluster;
class StorageReplicatedMergeTree;

/// Responsible for finding new place for partition:
/// - during INSERT
/// - for rebalance
class ReplicatedMergeTreeClusterPartitionSelector
{
public:
    explicit ReplicatedMergeTreeClusterPartitionSelector(ReplicatedMergeTreeCluster & cluster_);

    /// @return partition migration task
    std::optional<ReplicatedClusterMigratePartition> select();

    /// @return new replicas on INSERT
    Strings allocatePartition();

private:
    ReplicatedMergeTreeCluster & cluster;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;
};

}
