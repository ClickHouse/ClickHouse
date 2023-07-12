#include <Storages/MergeTree/ReplicatedMergeTreeCluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <algorithm>
#include <filesystem>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_LIVE_REPLICAS;
};

ReplicatedMergeTreeCluster::ReplicatedMergeTreeCluster(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , zookeeper_path(storage.zookeeper_path)
    , replica_path(storage.replica_path)
    , replica_name(storage.replica_name)
{
}

void ReplicatedMergeTreeCluster::loadFromCoordinatorImpl(const zkutil::ZooKeeperPtr & zookeeper, const Strings & partition_ids)
{
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
            partition_data.stat.version,
            partition_id,
            [this](auto & address) { resolveReplica(address); });

        {
            std::lock_guard lock(mutex);
            partitions[partition_id] = partition;
        }

        LOG_TEST(storage.log, "Loading partition {} from coordinator: {}", partition_id, partition.toStringForLog());
    }
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
    {
        std::lock_guard lock(mutex);
        auto it = partitions.find(partition_id);
        if (it != partitions.end())
            return it->second;
    }

    auto zookeeper = getZooKeeper();
    auto settings = storage.getSettings();

    auto replicas_names = getActiveReplicas(zookeeper);
    std::shuffle(replicas_names.begin(), replicas_names.end(), thread_local_rng);
    if (replicas_names.size() < settings->cluster_replication_factor)
    {
        throw Exception(ErrorCodes::TOO_FEW_LIVE_REPLICAS,
            "Not enough active replicas_names (there are only {}, required {}, active replicas: {})",
            replicas_names.size(), settings->cluster_replication_factor, fmt::join(replicas_names, ", "));
    }
    replicas_names.resize(settings->cluster_replication_factor);

    ReplicatedMergeTreeClusterReplicas replicas;
    replicas.reserve(replicas_names.size());
    for (const auto & candidate_replica_name : replicas_names)
    {
        auto & replica = replicas.emplace_back();
        replica.name = candidate_replica_name;
        resolveReplica(replica);
    }

    ReplicatedMergeTreeClusterPartition partition(partition_id, replicas, /* version= */ -1);
    /// Check in the coordinator
    {
        String partition_path = fs::path(storage.zookeeper_path) / "block_numbers" / partition_id;
        auto code = zookeeper->tryCreate(partition_path, partition.toString(), zkutil::CreateMode::Persistent);

        /// Partition already added by some of replicas, update the in memory representation
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            /// FIXME(cluster): what if it got updated in the mean time?
            Coordination::Stat partition_stat;
            String partition_data = zookeeper->get(partition_path, &partition_stat);
            partition = ReplicatedMergeTreeClusterPartition::fromString(
                partition_data,
                partition_stat.version,
                partition_id,
                [this](auto & address) { resolveReplica(address); });
        }
        else if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, partition_path);
    }

    /// Update in-memory representation
    {
        std::lock_guard lock(mutex);
        partitions.emplace(std::make_pair(partition_id, partition));
    }

    LOG_TEST(storage.log, "Add new partition {}: {}", partition_id, partition.toStringForLog());
    return partition;
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeCluster::getClusterPartition(const String & partition_id) const
{
    std::lock_guard lock(mutex);

    const auto it = partitions.find(partition_id);
    if (it == partitions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} does not exists", partition_id);

    return it->second;
}

ReplicatedMergeTreeClusterPartitions ReplicatedMergeTreeCluster::getClusterPartitions() const
{
    std::lock_guard lock(mutex);

    ReplicatedMergeTreeClusterPartitions result;
    result.reserve(partitions.size());
    for (const auto & [_, partition] : partitions)
        result.emplace_back(partition);
    return result;
}

Strings ReplicatedMergeTreeCluster::getActiveReplicas(const zkutil::ZooKeeperPtr & zookeeper) const
{
    Strings replicas = zookeeper->getChildren(zookeeper_path / "replicas");
    std::erase_if(replicas, [this, &zookeeper](const auto & replica)
    {
        return zookeeper->exists(zookeeper_path / "replicas" / replica / "is_active") == false;
    });
    return replicas;
}

zkutil::ZooKeeperPtr ReplicatedMergeTreeCluster::getZooKeeper() const
{
    return storage.getZooKeeper();
}

void ReplicatedMergeTreeCluster::resolveReplica(ReplicatedMergeTreeClusterReplica & replica)
{
    replica.fromString(getZooKeeper()->get(zookeeper_path / "replicas" / replica.name / "host"));
}

}
