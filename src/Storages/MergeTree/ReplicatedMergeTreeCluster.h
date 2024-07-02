#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Cluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterReplica.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterBalancer.h>
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
/// - policy interface
/// - subscribe to cluster changes? (replicas + is_active)
/// - versioned map (in conjunction with delayed parts removal it will allow to
///   handle previous versions of the parts, i.e. serving queries using some
///   previous version of the map, w/o failing the query on rebalance/resharding)
class ReplicatedMergeTreeCluster : public boost::noncopyable
{
public:
    explicit ReplicatedMergeTreeCluster(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreeCluster();

    void addCreateOps(Coordination::Requests & ops) const;
    void addCreateReplicaOps(Coordination::Requests & ops) const;
    void addRemoveReplicaOps(const zkutil::ZooKeeperPtr & zookeeper, Coordination::Requests & ops);
    void addDropOps(Coordination::Requests & ops) const;

    bool isReplicaActive() const;
    void dropReplica(ContextPtr local_context);

    void initialize(const zkutil::ZooKeeperPtr & zookeeper);
    void startDistributor();

    void shutdown();
    void sync();
    void loadFromCoordinator(const zkutil::ZooKeeperPtr & zookeeper);
    void loadPartitionFromCoordinator(const String & partition_id);

    ReplicatedMergeTreeClusterPartition getOrCreateClusterPartition(const String & partition_id);
    ReplicatedMergeTreeClusterPartition getClusterPartition(const String & partition_id) const;

    ReplicatedMergeTreeClusterPartitions getClusterPartitions() const;
    void updateClusterPartition(const ReplicatedMergeTreeClusterPartition & new_partition);

    ReplicatedMergeTreeClusterReplicas getClusterReplicas() const;

private:
    friend class ReplicatedMergeTreeClusterBalancer;
    friend class ReplicatedMergeTreeClusterPartitionSelector;
    friend class ReplicatedMergeTreeClusterPartitionSelectorImpl;

    StorageReplicatedMergeTree & storage;
    ReplicatedMergeTreeClusterBalancer balancer;
    LoggerPtr log;

    const fs::path zookeeper_path;
    const fs::path cluster_path;
    const fs::path replica_path;
    const fs::path replica_name;

    Strings getActiveReplicas(const zkutil::ZooKeeperPtr & zookeeper) const;
    /// Store <partition_id, info>
    std::unordered_map<String, ReplicatedMergeTreeClusterPartition> partitions TSA_GUARDED_BY(partitions_mutex);

    ReplicatedMergeTreeClusterReplicas replicas TSA_GUARDED_BY(replicas_mutex);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    void loadFromCoordinatorImpl(const zkutil::ZooKeeperPtr & zookeeper, const Strings & partition_ids);
    ReplicatedMergeTreeClusterReplica resolveReplica(const String & name) const;

    void cloneReplicaWithReshardingIfNeeded(const zkutil::ZooKeeperPtr & zookeeper);
    void cloneReplicaWithResharding(const zkutil::ZooKeeperPtr & zookeeper);

    void updateReplicas(const Strings & names);

    mutable std::mutex partitions_mutex;
    mutable std::mutex replicas_mutex;

    bool is_replica_active = false;
};

}
