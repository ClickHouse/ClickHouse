#include <Storages/MergeTree/ReplicatedMergeTreeCluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <algorithm>
#include <filesystem>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

ReplicatedMergeTreeCluster::ReplicatedMergeTreeCluster(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , balancer(*this)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (Cluster)"))
    , zookeeper_path(storage.zookeeper_path)
    , cluster_path(zookeeper_path / "cluster")
    , replica_path(storage.replica_path)
    , replica_name(storage.replica_name)
{
}

ReplicatedMergeTreeCluster::~ReplicatedMergeTreeCluster()
{
    balancer.shutdown();
}

void ReplicatedMergeTreeCluster::addCreateOps(Coordination::Requests & ops)
{
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path / "cluster", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path / "cluster" / "balancer", "", zkutil::CreateMode::Persistent));
}

void ReplicatedMergeTreeCluster::addDropOps(const fs::path & zookeeper_path, Coordination::Requests & ops)
{
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path / "cluster" / "balancer", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path / "cluster", -1));
}

void ReplicatedMergeTreeCluster::loadFromCoordinatorImpl(const zkutil::ZooKeeperPtr & zookeeper, const Strings & partition_ids)
{
    /// FIXME: this lock should be held for the whole function, since without
    /// it some callers may get old version for some of partitions and brake
    /// some invariants of balancer.
    ///
    /// But this is an ugly hack, and we need to fix this in a better way
    /// (using multi-version, updating it only from one thread, ...)
    ///
    /// And actually holding this lock may not be enough sometimes I guess...
    std::lock_guard lock(partitions_mutex);

    Strings partition_ids_paths;
    partition_ids_paths.reserve(partition_ids.capacity());
    for (const auto & partition_id : partition_ids)
        partition_ids_paths.emplace_back(zookeeper_path / "block_numbers" / partition_id);

    auto partitions_data = zookeeper->get(partition_ids_paths);
    for (size_t i = 0; i < partitions_data.size(); ++i)
    {
        const auto & partition_id = fs::path(partition_ids_paths[i]).filename();
        const auto & partition_data = partitions_data[i];

        auto partition = ReplicatedMergeTreeClusterPartition::fromString(
            partition_data.data,
            partition_data.stat,
            partition_id);

        partitions[partition_id] = partition;
        updateReplicas(partition.getAllReplicas());

        LOG_TEST(log, "Loading partition from coordinator: {}", partition.toStringForLog());
    }
}

/// TODO: rename initialize() to cloneReplicaIfNeeded() and implement it
void ReplicatedMergeTreeCluster::initialize()
{
    /// NOTE(cluster): loads replicas not in parallel like it could
    loadFromCoordinator();
    cloneReplicaWithReshardingIfNeeded();
}

void ReplicatedMergeTreeCluster::startDistributor()
{
    balancer.wakeup();
}

void ReplicatedMergeTreeCluster::cloneReplicaWithReshardingIfNeeded()
{
    auto zookeeper = getZooKeeper();

    bool is_new_replica;
    int is_lost_version;
    if (!StorageReplicatedMergeTree::isReplicaLost(zookeeper, replica_path, is_new_replica, is_lost_version, /* create_is_lost= */ true))
        return;

    if (is_new_replica)
        LOG_INFO(log, "Will initialize re-sharding from other replicas");
    else
        LOG_WARNING(log, "Will initialize re-sharding from other replicas");

    Coordination::Stat source_is_lost_stat;
    String source_replica = storage.getReplicaToCloneFrom(zookeeper, source_is_lost_stat);
    fs::path source_path = zookeeper_path / "replicas" / source_replica;
    /// Is it safe without processing queue firstly? We should take care of this during re-sharding.
    storage.cloneMetadataIfNeeded(source_replica, source_path, zookeeper);

    String raw_log_pointer = zookeeper->get(source_path / "log_pointer");
    zookeeper->set(replica_path / "log_pointer", raw_log_pointer);

    /// Clear obsolete queue that we no longer need.
    zookeeper->removeChildren(replica_path / "queue");
    storage.queue.clear();

    /// Will do repair from the selected replica.
    cloneReplicaWithResharding(zookeeper);
    /// If repair fails to whatever reason, the exception is thrown, is_lost will remain "1" and the replica will be repaired later.

    /// If replica is repaired successfully, we remove is_lost flag.
    zookeeper->set(replica_path / "is_lost", "0");
}

void ReplicatedMergeTreeCluster::cloneReplicaWithResharding(const zkutil::ZooKeeperPtr &)
{
    /// TODO(cluster): implement the initial clone from replicas to make data
    /// available faster (right now it is omitted, because it will cause
    /// conflicts)
}

void ReplicatedMergeTreeCluster::sync()
{
    /// FIXME(cluster): this is a hack to sync cluster partitions map, we need to get rid of it
    loadFromCoordinator();
    balancer.waitSynced();
}

void ReplicatedMergeTreeCluster::loadFromCoordinator()
{
    auto zookeeper = getZooKeeper();
    Strings partition_ids = zookeeper->getChildren(zookeeper_path / "block_numbers");
    loadFromCoordinatorImpl(zookeeper, partition_ids);
}

void ReplicatedMergeTreeCluster::loadPartitionFromCoordinator(const String & partition_id)
{
    auto zookeeper = getZooKeeper();
    loadFromCoordinatorImpl(zookeeper, {partition_id});
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeCluster::getOrCreateClusterPartition(const String & partition_id)
{
    auto replicas_names = ReplicatedMergeTreeClusterPartitionSelector(*this).allocatePartition();
    LOG_TEST(log, "Candidate replicas for {}: {}", partition_id, fmt::join(replicas_names, ", "));

    ReplicatedMergeTreeClusterPartition partition(partition_id,
        /* all_replicas= */ replicas_names,
        /* active_replicas= */ replicas_names,
        /* source_replica_= */ {},
        /* new_replica_= */ {},
        /* stat= */ {});

    updateReplicas(replicas_names);

    /// Check in the coordinator
    {
        auto zookeeper = getZooKeeper();

        String partition_path = zookeeper_path / "block_numbers" / partition_id;
        auto code = zookeeper->tryCreate(partition_path, partition.toString(), zkutil::CreateMode::Persistent);

        /// Partition already added by some of replicas, update the in memory representation
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            /// FIXME(cluster): what if it got updated in the mean time?
            Coordination::Stat partition_stat;
            String partition_data = zookeeper->get(partition_path, &partition_stat);
            partition = ReplicatedMergeTreeClusterPartition::fromString(
                partition_data,
                partition_stat,
                partition_id);
        }
        else if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, partition_path);
    }

    /// Update in-memory representation
    {
        std::lock_guard lock(partitions_mutex);
        partitions.emplace(std::make_pair(partition_id, partition));
    }

    LOG_INFO(log, "Add new partition {}: {}", partition_id, partition.toStringForLog());
    return partition;
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeCluster::getClusterPartition(const String & partition_id) const
{
    std::lock_guard lock(partitions_mutex);

    const auto it = partitions.find(partition_id);
    if (it == partitions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} does not exists", partition_id);

    return it->second;
}

ReplicatedMergeTreeClusterPartitions ReplicatedMergeTreeCluster::getClusterPartitions() const
{
    std::lock_guard lock(partitions_mutex);

    ReplicatedMergeTreeClusterPartitions result;
    result.reserve(partitions.size());
    for (const auto & [_, partition] : partitions)
        result.emplace_back(partition);
    return result;
}

void ReplicatedMergeTreeCluster::updateClusterPartition(const ReplicatedMergeTreeClusterPartition & new_partition)
{
    std::lock_guard lock(partitions_mutex);
    partitions[new_partition.getPartitionId()] = new_partition;
}

Strings ReplicatedMergeTreeCluster::getActiveReplicasImpl(const zkutil::ZooKeeperPtr & zookeeper) const
{
    Strings active_replicas = zookeeper->getChildren(zookeeper_path / "replicas");
    std::erase_if(active_replicas, [this, &zookeeper](const auto & replica)
    {
        return zookeeper->exists(zookeeper_path / "replicas" / replica / "is_active") == false;
    });
    /// NOTE: Or just go through replicas and check is_lost?
    return active_replicas;
}

Strings ReplicatedMergeTreeCluster::getActiveReplicas() const
{
    return getActiveReplicasImpl(getZooKeeper());
}

zkutil::ZooKeeperPtr ReplicatedMergeTreeCluster::getZooKeeper() const
{
    return storage.getZooKeeper();
}

ReplicatedMergeTreeClusterReplicas ReplicatedMergeTreeCluster::getClusterReplicas() const
{
    std::lock_guard lock(replicas_mutex);
    return replicas;
}

void ReplicatedMergeTreeCluster::updateReplicas(const Strings & names)
{
    std::lock_guard lock(replicas_mutex);
    for (const auto & name : names)
    {
        if (replicas.contains(name))
            continue;

        replicas[name] = resolveReplica(name);
    }
}

ReplicatedMergeTreeClusterReplica ReplicatedMergeTreeCluster::resolveReplica(const String & name) const
{
    auto zookeeper = getZooKeeper();
    ReplicatedMergeTreeClusterReplica replica;
    replica.fromCoordinator(zookeeper, zookeeper_path, name);
    return replica;
}

}
