#pragma once

#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class StorageReplicatedMergeTree;

// Attach table to the existing data.
// Initialize the table by creating all the necessary nodes and do the required checks.
// Initialization is repeated if an operation fails because of a ZK request or connection loss.
class ReplicatedMergeTreeAttachThread
{
public:
    explicit ReplicatedMergeTreeAttachThread(StorageReplicatedMergeTree & storage_);

    ~ReplicatedMergeTreeAttachThread();

    void start() { task->activateAndSchedule(); }

    void shutdown();

    void waitFirstTry() { first_try_done.wait(false); }

    void setSkipSanityChecks(bool skip_sanity_checks_);

    static void checkHasReplicaMetadataInZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const String & replica_path);

private:
    StorageReplicatedMergeTree & storage;
    BackgroundSchedulePool::TaskHolder task;

    std::string log_name;
    LoggerPtr log;

    std::atomic<bool> first_try_done{false};

    std::atomic<bool> shutdown_called{false};

    UInt64 retry_period;

    bool skip_sanity_checks{false};

    void run();
    void runImpl();

    void finalizeInitialization();
};

}
