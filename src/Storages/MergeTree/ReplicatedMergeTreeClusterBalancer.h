#pragma once

#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartitionSelector.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <atomic>
#include <optional>
#include <base/defines.h>

namespace DB
{

class ReplicatedMergeTreeCluster;
class StorageReplicatedMergeTree;

class ReplicatedMergeTreeClusterBalancer
{
public:
    explicit ReplicatedMergeTreeClusterBalancer(ReplicatedMergeTreeCluster & cluster_);
    ~ReplicatedMergeTreeClusterBalancer();

    void wakeup();
    void shutdown();

    void waitSynced();

private:
    ReplicatedMergeTreeCluster & cluster;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;

    enum Step
    {
        /// Select partition to migrate/clone.
        /// For details see ReplicatedMergeTreeClusterPartitionSelector.
        SELECT_PARTITION,
        /// Regular migration for partition.
        MIGRATE_PARTITION,
        /// Usually if some replica goes had been gone.
        CLONE_PARTITION,
        /// Revert the CLONE_PARTITION/MIGRATE_PARTITION in case of error.
        REVERT,
        /// Everything is up to date.
        NOTHING_TODO,
    };
    struct State
    {
        Step step = SELECT_PARTITION;
        std::optional<ReplicatedMergeTreeClusterPartition> target;
    };
    State state;

    BackgroundSchedulePool::TaskHolder background_task;
    std::atomic_bool is_stopped = false;

    void restoreStateFromCoordinator();
    void run();
    void runStep();

    std::optional<ReplicatedMergeTreeClusterPartition> selectPartition();

    void migrateOrClonePartitionWithClone(const ReplicatedMergeTreeClusterPartition & target);
    void clonePartition(const zkutil::ZooKeeperPtr & zookeeper, const String & partition, const String & source_replica);

    void finish(const ReplicatedMergeTreeClusterPartition & target);
    void revert(const ReplicatedMergeTreeClusterPartition & target);

    void enqueueDropPartition(const zkutil::ZooKeeperPtr & zookeeper, const String & source_replica, const String & partition_id);
    void cleanupOldPartitions();
};

}
