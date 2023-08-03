#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartitionSelector.h>
#include <Storages/MergeTree/ReplicatedMergeTreeCluster.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_LIVE_REPLICAS;
};

ReplicatedClusterMigratePartition::operator bool() const
{
    chassert(partition.empty() || !source_replica.empty());
    return !partition.empty();
}

ReplicatedMergeTreeClusterPartitionSelector::ReplicatedMergeTreeClusterPartitionSelector(ReplicatedMergeTreeCluster & cluster_)
    : cluster(cluster_)
    , storage(cluster.storage)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (PartitionSelector)"))
{
}

std::optional<ReplicatedClusterMigratePartition> ReplicatedMergeTreeClusterPartitionSelector::select()
{
    cluster.loadFromCoordinator();

    const auto & partitions = cluster.getClusterPartitions();
    if (partitions.empty())
    {
        LOG_TEST(log, "No partitions");
        return std::nullopt;
    }

    const auto & replica_name = storage.getReplicaName();

    /// <replica_name, partitions>
    std::unordered_map<String, std::vector<String>> replicas_partitions;
    std::unordered_set<String> unique_partitions;
    for (const auto & partition : partitions)
    {
        const auto & replicas = partition.getActiveNonMigrationReplicas();
        for (const auto & replica : replicas)
        {
            const auto & partition_id = partition.getPartitionId();

            replicas_partitions[replica].emplace_back(partition_id);
            unique_partitions.emplace(partition_id);
        }
    }

    /// Take into account replicas without partitions
    {
        auto active_replicas = cluster.getActiveReplicas();
        for (const auto & active_replica : active_replicas)
            replicas_partitions[active_replica];
        /// Always adds ourself
        replicas_partitions[replica_name];
    }

    size_t min_partitions_per_replica = std::numeric_limits<size_t>::max();
    size_t max_partitions_per_replica = 0;
    for (const auto & [_, replica_partitions] : replicas_partitions)
    {
        min_partitions_per_replica = std::min(min_partitions_per_replica, replica_partitions.size());
        max_partitions_per_replica = std::max(max_partitions_per_replica, replica_partitions.size());
    }

    size_t replicas = replicas_partitions.size();
    UInt64 cluster_replication_factor = storage.getSettings()->cluster_replication_factor;
    if (replicas <= cluster_replication_factor)
    {
        LOG_TEST(log, "No spare replicas (replicas: {}, cluster_replication_factor: {})", replicas, cluster_replication_factor);
        return std::nullopt;
    }

    const auto & local_partitions = storage.getAllPartitionIds();
    size_t partitions_per_replica = unique_partitions.size() / (replicas / cluster_replication_factor);
    LOG_TEST(log, "Cluster (replicas: {}, partitions: {}, expected partitions per replica: {}, min {}/max {} partitions per replica), replica (cluster partitions: {}, local partitions: {})",
        replicas_partitions.size(), unique_partitions.size(), partitions_per_replica, min_partitions_per_replica, max_partitions_per_replica,
        replicas_partitions[replica_name].size(), local_partitions.size());

    if (replicas_partitions[replica_name].size() >= partitions_per_replica)
    {
        LOG_TEST(log, "Replica is in sync");
        return std::nullopt;
    }

    for (const auto & partition : partitions)
    {
        /// This is a pre-check, the real exclusive check is done via version in coordinator.
        if (partition.isUnderReSharding())
        {
            if (partition.getNewReplica() == replica_name)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Partition ({}) should be migrated to this replica. This is a bug, no partition selection should take place.",
                    partition.toStringForLog());

            LOG_TEST(log, "{} partition is already under re-sharding, skipping", partition.getPartitionId());
            continue;
        }

        const auto & replicas_names = partition.getAllReplicas();
        if (std::find(replicas_names.begin(), replicas_names.end(), replica_name) != replicas_names.end())
        {
            LOG_TEST(log, "Partition {} already exists on replica {}", partition.getPartitionId(), replica_name);
            continue;
        }

        /// Due to background DROP_RANGE it is possible that some partition may
        /// still exists (even if it replica is not included into the cluster
        /// partition information)
        if (local_partitions.contains(partition.getPartitionId()))
        {
            LOG_WARNING(log, "Partition {} still exists on replica {}", partition.getPartitionId(), replica_name);
            continue;
        }

        bool partition_has_unevenly_distributed_replica = false;
        for (const auto & partition_replica : replicas_names)
        {
            size_t replica_partitions = replicas_partitions[partition_replica].size();
            if (replica_partitions < partitions_per_replica)
            {
                LOG_TEST(log, "Partition {} has unevenly distributed replica {} ({} vs {})",
                    partition.getPartitionId(), partition_replica, replica_partitions, partitions_per_replica);
                partition_has_unevenly_distributed_replica = true;
                break;
            }
        }
        if (partition_has_unevenly_distributed_replica)
            continue;

        /// Get the replica with the maximum partitions in total on it
        /// TODO: take into account log_pointer as well?
        String source_replica;
        {
            auto partition_replicas = partition.getActiveReplicas();
            size_t max = 0;
            for (const auto & partition_replica : partition_replicas)
            {
                if (replicas_partitions[partition_replica].size() <= partitions_per_replica)
                    continue;
                if (replicas_partitions[partition_replica].size() > max)
                {
                    max = replicas_partitions[partition_replica].size();
                    source_replica = partition_replica;
                }
            }
        }
        if (source_replica.empty())
        {
            LOG_TRACE(log, "No candidate replica for partition {}", partition.toStringForLog());
            continue;
        }

        ReplicatedClusterMigratePartition target;
        target.source_replica = source_replica;
        target.partition = partition.getPartitionId();
        target.partition_version = partition.getVersion();

        LOG_INFO(log, "Partition {} is going to be moved from {} (with {} parts) to {} (with {} parts)",
            target.partition,
            target.source_replica, replicas_partitions[target.source_replica].size(),
            replica_name, replicas_partitions[replica_name].size());

        /// One by one
        return target;
    }

    LOG_TEST(log, "Nothing was selected");
    return std::nullopt;
}


Strings ReplicatedMergeTreeClusterPartitionSelector::allocatePartition()
{
    /// <replica_name, partitions>
    std::unordered_map<String, size_t> replicas_partitions;

    /// NOTE: it may require updating the cluster partitions map (loadFromCoordinator())
    for (const auto & partition : cluster.getClusterPartitions())
    {
        const auto & partition_replicas = partition.getAllReplicas();
        for (const auto & replica : partition_replicas)
            ++replicas_partitions[replica];
    }

    auto settings = storage.getSettings();

    auto replicas_names = cluster.getActiveReplicas();
    if (replicas_names.size() < settings->cluster_replication_factor)
    {
        throw Exception(ErrorCodes::TOO_FEW_LIVE_REPLICAS,
            "Not enough active replicas_names (there are only {}, required {}, active replicas: {})",
            replicas_names.size(), settings->cluster_replication_factor, fmt::join(replicas_names, ", "));
    }

    std::sort(replicas_names.begin(), replicas_names.end(), [&](const auto & lhs, const auto & rhs)
    {
        return replicas_partitions[lhs] < replicas_partitions[rhs];
    });
    replicas_names.resize(settings->cluster_replication_factor);

    return replicas_names;
}

};
