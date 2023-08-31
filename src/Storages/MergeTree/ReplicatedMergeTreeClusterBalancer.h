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
        SELECT_PARTITION,
        MIGRATE_PARTITION,
        REVERT_MIGRATE_PARTITION,
        NOTHING_TODO,
    };
    struct State
    {
        Step step = SELECT_PARTITION;
        std::optional<ReplicatedClusterMigratePartition> migrate_partition;
    };
    State state;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic_bool is_stopped = false;

    void restoreStateFromCoordinator();
    void run();
    void runStep();

    std::optional<ReplicatedClusterMigratePartition> selectPartition();
    void migratePartitionWithClone(const ReplicatedClusterMigratePartition & target);
    void finishPartitionMigration(const ReplicatedClusterMigratePartition & target);
    void clonePartition(const zkutil::ZooKeeperPtr & zookeeper, const String & partition, const String & source_replica);

    void revertMigratePartition(const ReplicatedClusterMigratePartition & target);

    void enqueueDropPartition(const zkutil::ZooKeeperPtr & zookeeper, const String & source_replica, const String & partition_id);
    void cleanupOldPartitions();
};

}
