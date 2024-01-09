#pragma once

#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartitionSelector.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <atomic>
#include <optional>
#include <base/defines.h>

namespace DB
{

class ReplicatedMergeTreeCluster;
class StorageReplicatedMergeTree;

enum ReplicatedMergeTreeClusterBalancerStep
{
    /// Select partition to migrate/clone.
    /// For details see ReplicatedMergeTreeClusterPartitionSelector.
    BALANCER_SELECT_PARTITION,
    /// Regular migration for partition.
    BALANCER_MIGRATE_PARTITION,
    /// Usually if some existing replica had been gone.
    BALANCER_CLONE_PARTITION,
    /// Usually after SYSTEM DROP CLUSTER REPLICA
    BALANCER_DROP_PARTITION,
    /// Revert the CLONE_PARTITION/MIGRATE_PARTITION in case of error.
    BALANCER_REVERT,
    /// Everything is up to date.
    BALANCER_NOTHING_TODO,
};

class ReplicatedMergeTreeClusterBalancer
{
public:
    explicit ReplicatedMergeTreeClusterBalancer(ReplicatedMergeTreeCluster & cluster_);
    ~ReplicatedMergeTreeClusterBalancer();

    void wakeup();
    void shutdown();

    void waitSynced(const zkutil::ZooKeeperPtr & zookeeper, bool throw_if_stopped);

private:
    ReplicatedMergeTreeCluster & cluster;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;

    struct State
    {
        ReplicatedMergeTreeClusterBalancerStep step = BALANCER_SELECT_PARTITION;
        std::optional<ReplicatedMergeTreeClusterPartition> target;
    };
    State state;

    BackgroundSchedulePool::TaskHolder background_task;
    std::atomic_bool is_stopped = false;

    void restoreStateFromCoordinator();
    void run();
    void runStep(const zkutil::ZooKeeperPtr & zookeeper);

    void replicatePartition(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target, const std::list<ReplicatedMergeTreeLogEntryPtr> & entries);
    std::list<ReplicatedMergeTreeLogEntryPtr> clonePartition(const zkutil::ZooKeeperPtr & zookeeper, ReplicatedMergeTreeClusterPartition & target);

    void finish(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target);
    void revert(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target);

    void enqueueDropPartition(const zkutil::ZooKeeperPtr & zookeeper, const String & source_replica, const String & partition_id);
    void cleanupOldPartitions(const zkutil::ZooKeeperPtr & zookeeper, time_t ttl);
};

}
