#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Cluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <base/defines.h>
#include <boost/core/noncopyable.hpp>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

class StorageReplicatedMergeTree;

/// Implementation of the cluster mode for ReplicatedMergeTree.
///
/// Implementation notes:
/// - each partition could have different replica set (but not part, since in
///   this case it is tricky to apply some operations)
///
/// TODO(cluster):
/// - re sharding
/// - policy interface
/// - subscribe to cluster changes? (replicas + is_active)
/// - versioned map (in conjunction with delayed parts removal it will allow to
///   handle previous versions of the parts, i.e. serving queries using some
///   previous version of the map, w/o failing the query on rebalance/resharding)
class ReplicatedMergeTreeCluster : public boost::noncopyable
{
public:
    explicit ReplicatedMergeTreeCluster(StorageReplicatedMergeTree & storage_);

    void loadFromCoordinator();
    void loadPartitionFromCoordinator(const String & partition_id);

    ReplicatedMergeTreeClusterPartition getOrCreateClusterPartition(const String & partition_id);
    ReplicatedMergeTreeClusterPartition getClusterPartition(const String & partition_id) const;

    ReplicatedMergeTreeClusterPartitions getClusterPartitions() const;

private:
    StorageReplicatedMergeTree & storage;

    const fs::path zookeeper_path;
    const fs::path replica_path;
    const fs::path replica_name;

    Strings getActiveReplicas(const zkutil::ZooKeeperPtr & zookeeper) const;
    std::unordered_map<String, ReplicatedMergeTreeClusterPartition> partitions TSA_GUARDED_BY(mutex);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    void loadFromCoordinatorImpl(const zkutil::ZooKeeperPtr & zookeeper, const Strings & partition_ids);
    void resolveReplica(ReplicatedMergeTreeClusterReplica & replica);

    mutable std::mutex mutex;
};

}
