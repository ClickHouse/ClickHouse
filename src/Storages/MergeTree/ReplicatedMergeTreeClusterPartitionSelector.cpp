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

class ReplicatedMergeTreeClusterPartitionSelectorImpl
{
public:
    ReplicatedMergeTreeClusterPartitionSelectorImpl(ReplicatedMergeTreeCluster & cluster, const StorageReplicatedMergeTree & storage, Poco::Logger * log);

    std::optional<ReplicatedMergeTreeClusterPartition> select();
    std::optional<ReplicatedMergeTreeClusterPartition> selectPartitionToMigrateFromRemovedReplica();
    std::optional<ReplicatedMergeTreeClusterPartition> selectPartitionToMigrate();
    std::optional<ReplicatedMergeTreeClusterPartition> selectPartitionToClone();

private:
    ReplicatedMergeTreeCluster & cluster;
    const StorageReplicatedMergeTree & storage;
    Poco::Logger * log;
    const String replica_name;

    /// <replica_name, partitions>
    std::unordered_map<String, std::vector<String>> replicas_partitions;
    NameSet removing_replicas;
    ReplicatedMergeTreeClusterPartitions suitable_partitions;
    std::unordered_map<String, ReplicatedMergeTreeClusterPartition> partitions_map;
    size_t partitions_per_replica = 0;
    UInt64 cluster_replication_factor;

    void reset();
};
ReplicatedMergeTreeClusterPartitionSelectorImpl::ReplicatedMergeTreeClusterPartitionSelectorImpl(ReplicatedMergeTreeCluster & cluster_, const StorageReplicatedMergeTree & storage_, Poco::Logger * log_)
    : cluster(cluster_)
    , storage(storage_)
    , log(log_)
    , replica_name(storage.getReplicaName())
    , cluster_replication_factor(storage.getSettings()->cluster_replication_factor)
{
}
void ReplicatedMergeTreeClusterPartitionSelectorImpl::reset()
{
    replicas_partitions.clear();
    removing_replicas.clear();
    suitable_partitions.clear();
    partitions_map.clear();
    partitions_per_replica = 0;
}
std::optional<ReplicatedMergeTreeClusterPartition> ReplicatedMergeTreeClusterPartitionSelectorImpl::select()
{
    reset();
    cluster.loadFromCoordinator();

    suitable_partitions = cluster.getClusterPartitions();
    if (suitable_partitions.empty())
    {
        LOG_TEST(log, "No partitions");
        return std::nullopt;
    }

    std::unordered_set<String> unique_partitions;
    for (const auto & partition : suitable_partitions)
    {
        const auto & replicas = partition.getActiveNonMigrationReplicas();
        for (const auto & replica : replicas)
        {
            const auto & partition_id = partition.getPartitionId();

            replicas_partitions[replica].emplace_back(partition_id);
            unique_partitions.emplace(partition_id);
        }

        partitions_map[partition.getPartitionId()] = partition;
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

    for (const auto & [replica, _] : replicas_partitions)
    {
        if (cluster.getZooKeeper()->exists(cluster.cluster_path / "replicas" / replica / "removed"))
            removing_replicas.emplace(replica);
    }
    size_t alive_replicas = replicas_partitions.size() - removing_replicas.size();
    partitions_per_replica = static_cast<size_t>(unique_partitions.size() / (static_cast<double>(alive_replicas) / cluster_replication_factor));
    LOG_TEST(log, "Cluster (replicas: {}, of them removing: {}, partitions: {}, expected partitions per replica: {}, min {}/max {} partitions per replica), replica (cluster partitions: {}, local partitions: {})",
        replicas_partitions.size(), removing_replicas.size(), unique_partitions.size(), partitions_per_replica, min_partitions_per_replica, max_partitions_per_replica,
        replicas_partitions[replica_name].size(), storage.getActivePartsCount());

    if (removing_replicas.empty() && replicas_partitions[replica_name].size() >= partitions_per_replica)
    {
        LOG_TEST(log, "Replica is in sync");
        return std::nullopt;
    }

    std::erase_if(suitable_partitions, [this](const auto & partition)
    {
        if (partition.isUnderReSharding())
        {
            LOG_TEST(log, "{} partition is already under re-sharding, skipping", partition.getPartitionId());
            return true;
        }

        const auto & replicas_names = partition.getAllReplicas();
        if (std::find(replicas_names.begin(), replicas_names.end(), replica_name) != replicas_names.end())
        {
            LOG_TEST(log, "Partition {} already exists on replica {}", partition.getPartitionId(), replica_name);
            return true;
        }

        return false;
    });

    /// ORDER BY length(all_replicas)
    std::sort(suitable_partitions.begin(), suitable_partitions.end(), [](const auto & a, const auto & b)
    {
        return a.getActiveReplicas().size() < b.getActiveReplicas().size();
    });

    if (auto task = selectPartitionToMigrateFromRemovedReplica())
        return task;
    if (auto task = selectPartitionToMigrate())
        return task;
    /// TODO: add delay after which we should clone parts initially, since we
    /// may just add replica pair, and we can initiate clone, which later will
    /// be dropped/migrated.
    if (auto task = selectPartitionToClone())
        return task;
    return std::nullopt;
}
std::optional<ReplicatedMergeTreeClusterPartition> ReplicatedMergeTreeClusterPartitionSelectorImpl::selectPartitionToMigrateFromRemovedReplica()
{
    for (const auto & lost_replica : removing_replicas)
    {
        LOG_INFO(log, "Searching partitions for removed replica {}", lost_replica);

        const auto & lost_replica_partitions = replicas_partitions[lost_replica];
        for (const auto & partition_id : lost_replica_partitions)
        {
            const auto & partition_it = partitions_map.find(partition_id);
            if (partition_it == partitions_map.end())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot find partition {} for removing replica {}", partition_id, lost_replica);
            }
            const auto & partition = partition_it->second;

            auto new_partition = partition;
            if (new_partition.isUnderReSharding())
            {
                new_partition.revert();
                LOG_INFO(log, "Revert partition {} from lost replica ({} source parts, {} local parts)",
                    new_partition.toStringForLog(),
                    replicas_partitions[new_partition.getSourceReplica()].size(),
                    replicas_partitions[replica_name].size());
                return new_partition;
            }

            if (partition.hasReplica(replica_name))
                continue;

            new_partition.replaceReplica(lost_replica, replica_name);
            LOG_INFO(log, "Migrating partition {} from lost replica ({} source parts, {} local parts)",
                new_partition.toStringForLog(),
                replicas_partitions[new_partition.getSourceReplica()].size(),
                replicas_partitions[replica_name].size());
            return new_partition;
        }
    }

    return std::nullopt;
}

std::optional<ReplicatedMergeTreeClusterPartition> ReplicatedMergeTreeClusterPartitionSelectorImpl::selectPartitionToMigrate()
{
    for (const auto & partition : suitable_partitions)
    {
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

        auto new_partition = partition;
        new_partition.replaceReplica(source_replica, replica_name);

        LOG_INFO(log, "Migrating partition {} ({} source parts, {} local parts)",
            new_partition.toStringForLog(),
            replicas_partitions[new_partition.getSourceReplica()].size(),
            replicas_partitions[replica_name].size());

        return new_partition;
    }

    LOG_TEST(log, "Nothing to migrate");
    return std::nullopt;
}
std::optional<ReplicatedMergeTreeClusterPartition> ReplicatedMergeTreeClusterPartitionSelectorImpl::selectPartitionToClone()
{
    size_t this_replica_partitions = replicas_partitions[replica_name].size();
    if (this_replica_partitions >= partitions_per_replica)
    {
        LOG_TEST(log, "Replica is up to date");
        return std::nullopt;
    }

    for (const auto & partition : suitable_partitions)
    {
        if (partition.getActiveReplicas().size() >= cluster_replication_factor)
        {
            LOG_TEST(log, "Replication factor {} already satisfied for partition {} ", cluster_replication_factor, partition.getPartitionId());
            continue;
        }

        /// Get the replica with the maximum partitions in total on it
        /// TODO: take into account log_pointer as well?
        String source_replica;
        {
            auto partition_replicas = partition.getActiveReplicas();
            size_t max = 0;
            for (const auto & partition_replica : partition_replicas)
            {
                if (removing_replicas.contains(partition_replica))
                {
                    source_replica = partition_replica;
                    break;
                }

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

        auto new_partition = partition;
        new_partition.addReplica(source_replica, replica_name);

        LOG_INFO(log, "Cloning partition {} ({} source parts, {} local parts)",
            new_partition.toStringForLog(),
            replicas_partitions[new_partition.getSourceReplica()].size(),
            replicas_partitions[replica_name].size());

        /// One by one
        return new_partition;
    }

    LOG_TEST(log, "Nothing to clone");
    return std::nullopt;
}


ReplicatedMergeTreeClusterPartitionSelector::ReplicatedMergeTreeClusterPartitionSelector(ReplicatedMergeTreeCluster & cluster_)
    : cluster(cluster_)
    , storage(cluster.storage)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (ClusterPartitionSelector)"))
{
}

std::optional<ReplicatedMergeTreeClusterPartition> ReplicatedMergeTreeClusterPartitionSelector::select()
{
    ReplicatedMergeTreeClusterPartitionSelectorImpl impl(cluster, storage, log);
    return impl.select();
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
