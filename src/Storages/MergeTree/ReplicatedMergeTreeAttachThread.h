#pragma once

#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

class StorageReplicatedMergeTree;

class ReplicatedMergeTreeAttachThread
{
public:
    explicit ReplicatedMergeTreeAttachThread(StorageReplicatedMergeTree & storage_);

    ~ReplicatedMergeTreeAttachThread();

    void start() { task->activateAndSchedule(); }

    void shutdown();

    void waitFirstTry() { first_try_done.wait(false); }

    void setSkipSanityChecks(bool skip_sanity_checks_);

private:
    StorageReplicatedMergeTree & storage;
    BackgroundSchedulePool::TaskHolder task;

    std::string log_name;
    Poco::Logger * log;

    std::atomic<bool> first_try_done{false};

    std::atomic<bool> shutdown_called{false};

    zkutil::ZooKeeperPtr zookeeper;

    UInt64 retry_period;

    bool skip_sanity_checks{false};

    void run();
    void runImpl();

    void finalizeInitialization();
};

}
