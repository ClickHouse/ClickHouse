#include <Storages/MergeTree/ReplicatedMergeTreeCluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/Stopwatch.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
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
    , log(getLogger(storage.getStorageID().getFullTableName() + " (Cluster)"))
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

void ReplicatedMergeTreeCluster::addCreateOps(Coordination::Requests & ops) const
{
    ops.emplace_back(zkutil::makeCreateRequest(cluster_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(cluster_path / "balancer", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(cluster_path / "replicas", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(cluster_path / "replicas" / replica_name, "", zkutil::CreateMode::Persistent));
}

void ReplicatedMergeTreeCluster::addCreateReplicaOps(Coordination::Requests & ops) const
{
    ops.emplace_back(zkutil::makeCreateRequest(cluster_path / "replicas" / replica_name, "", zkutil::CreateMode::Persistent));
}

void ReplicatedMergeTreeCluster::addRemoveReplicaOps(const zkutil::ZooKeeperPtr & zookeeper, Coordination::Requests & ops)
{
    loadFromCoordinator(zookeeper);

    Coordination::Stat balancer_stat;
    fs::path balancer_path = cluster_path / "balancer";
    zookeeper->get(balancer_path, &balancer_stat);

    {
        std::lock_guard lock(partitions_mutex);
        for (auto & [_, partition] : partitions)
        {
            if (!partition.hasReplica(replica_name))
                continue;

            partition.removeReplica(replica_name);

            String partition_path = zookeeper_path / "block_numbers" / partition.getPartitionId();
            ops.emplace_back(zkutil::makeSetRequest(partition_path, partition.toString(), partition.getVersion()));
        }
    }
    LOG_TRACE(log, "Removing replica from the cluster. {} partitions affected", ops.size());

    if (!is_replica_active)
        ops.emplace_back(zkutil::makeRemoveRequest(cluster_path / "replicas" / replica_name / "removed", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(cluster_path / "replicas" / replica_name, -1));
    ops.emplace_back(zkutil::makeSetRequest(balancer_path, "", balancer_stat.version));
}

void ReplicatedMergeTreeCluster::addDropOps(Coordination::Requests & ops) const
{
    ops.emplace_back(zkutil::makeRemoveRequest(cluster_path / "replicas", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(cluster_path / "balancer", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(cluster_path, -1));
}

bool ReplicatedMergeTreeCluster::isReplicaActive() const
{
    return is_replica_active;
}

void ReplicatedMergeTreeCluster::dropReplica(ContextPtr)
{
    const auto zookeeper = getZooKeeper();
    balancer.shutdown();
    balancer.waitSynced(zookeeper, /* throw_if_stopped= */ false);
    is_replica_active = false;

    {
        const auto & active_replicas = getActiveReplicas(zookeeper).size();
        const auto cluster_replication_factor = storage.getSettings()->cluster_replication_factor;
        /// Current replica already removed => strict comparison
        if (active_replicas < cluster_replication_factor)
        {
            LOG_INFO(log, "Do not wait for partitions migration to other replicas in cluster, since there is only {} replicas (cluster_replication_factor: {})",
                active_replicas, cluster_replication_factor);
            return;
        }
        else
            LOG_INFO(log, "Waiting while all partitions will be migrated from this replica to other replicas in cluster");
    }

    LOG_INFO(log, "Marking replica as removed in the cluster");
    zookeeper->create(cluster_path / "replicas" / replica_name / "removed", "", zkutil::CreateMode::Persistent);

    /// Trigger cluster balancers to stole partitions from this replica.
    {
        Strings active_replicas = getActiveReplicas(zookeeper);
        std::erase(active_replicas, replica_name);

        ReplicatedMergeTreeLogEntryData sync_log_entry;

        sync_log_entry.type = ReplicatedMergeTreeLogEntryData::CLUSTER_SYNC;
        sync_log_entry.create_time = std::time(nullptr);
        sync_log_entry.source_replica = storage.replica_name;
        sync_log_entry.replicas = active_replicas;

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path / "log", "", -1));
        ops.emplace_back(zkutil::makeCreateRequest(
            zookeeper_path / "log" / "log-", sync_log_entry.toString(), zkutil::CreateMode::PersistentSequential));

        Coordination::Responses responses;
        Coordination::Error rc = zookeeper->tryMulti(ops, responses);
        zkutil::KeeperMultiException::check(rc, ops, responses);

        String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
        sync_log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);
        LOG_DEBUG(log, "Pushed CLUSTER_SYNC log entry: {}", sync_log_entry.znode_name);

        Stopwatch watch;
        storage.tryWaitForAllReplicasToProcessLogEntry(zookeeper_path, sync_log_entry, 0);
        LOG_INFO(log, "Waiting for CLUSTER_SYNC, took {} ms.", watch.elapsedMilliseconds());
    }

    Stopwatch watch;
    while (true)
    {
        loadFromCoordinator(zookeeper);
        std::lock_guard lock(partitions_mutex);
        size_t partitions_on_replica = std::count_if(partitions.begin(), partitions.end(), [this](const auto & pair)
        {
            return pair.second.hasReplica(replica_name);
        });
        if (!partitions_on_replica)
            break;
        LOG_DEBUG(LogFrequencyLimiter(log, 10), "Replica still has {} cluster partitions", partitions_on_replica);
        sleepForSeconds(1);
    }
    LOG_INFO(log, "Cluster partitions had been migrated out from this replica in {} ms.", watch.elapsedMilliseconds());
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

void ReplicatedMergeTreeCluster::initialize(const zkutil::ZooKeeperPtr & zookeeper)
{
    is_replica_active = !zookeeper->exists(cluster_path / "replicas" / replica_name / "removed");
    if (is_replica_active)
        LOG_INFO(log, "Initializing cluster for active replica.");
    else
        LOG_WARNING(log, "Initializing cluster for inactive replica.");

    /// NOTE(cluster): loads replicas not in parallel like it could
    loadFromCoordinator(zookeeper);

    if (is_replica_active)
        cloneReplicaWithReshardingIfNeeded(zookeeper);
}

void ReplicatedMergeTreeCluster::startDistributor()
{
    if (is_replica_active)
        balancer.wakeup();
}

void ReplicatedMergeTreeCluster::cloneReplicaWithReshardingIfNeeded(const zkutil::ZooKeeperPtr & zookeeper)
{
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

    cloneReplicaWithResharding(zookeeper);

    /// If replica is repaired successfully, we remove is_lost flag.
    zookeeper->set(replica_path / "is_lost", "0");
}

void ReplicatedMergeTreeCluster::cloneReplicaWithResharding(const zkutil::ZooKeeperPtr &)
{
    /// TODO(cluster): implement the initial clone from replicas to make data
    /// available faster (right now it is omitted, because it will cause
    /// conflicts)
}

void ReplicatedMergeTreeCluster::shutdown()
{
    balancer.shutdown();
}

void ReplicatedMergeTreeCluster::sync()
{
    const auto & zookeeper = getZooKeeper();
    balancer.waitSynced(zookeeper, /* throw_if_stopped= */ true);
    /// FIXME(cluster): this is a hack to sync cluster partitions map, we need to get rid of it
    loadFromCoordinator(zookeeper);
}

void ReplicatedMergeTreeCluster::loadFromCoordinator(const zkutil::ZooKeeperPtr & zookeeper)
{
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
    auto zookeeper = getZooKeeper();

    auto replicas_names = ReplicatedMergeTreeClusterPartitionSelector(zookeeper, *this).allocatePartition();
    LOG_TEST(log, "Candidate replicas for {}: {}", partition_id, fmt::join(replicas_names, ", "));

    ReplicatedMergeTreeClusterPartition partition(partition_id, replicas_names);
    updateReplicas(replicas_names);

    /// Check in the coordinator
    {
        String partition_path = zookeeper_path / "block_numbers" / partition_id;
        auto code = zookeeper->tryCreate(partition_path, partition.toString(), zkutil::CreateMode::Persistent);

        /// Partition already added by some of replicas, update the in memory representation
        if (code == Coordination::Error::ZNODEEXISTS)
        {
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

Strings ReplicatedMergeTreeCluster::getActiveReplicas(const zkutil::ZooKeeperPtr & zookeeper) const
{
    Strings active_replicas = zookeeper->getChildren(zookeeper_path / "replicas");
    std::erase_if(active_replicas, [this, &zookeeper](const auto & replica)
    {
        return zookeeper->exists(zookeeper_path / "replicas" / replica / "is_active") == false;
    });
    // std::erase_if(active_replicas, [this, &zookeeper](const auto & replica)
    // {
    //     return zookeeper->get(zookeeper_path / "replicas" / replica / "is_lost") == "1";
    // });
    /// NOTE: Or just go through replicas and check is_lost?
    return active_replicas;
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
