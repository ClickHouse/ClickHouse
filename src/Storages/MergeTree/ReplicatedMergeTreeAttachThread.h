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

    bool first_try{true};
    std::atomic<bool> first_try_done{false};

    std::atomic<bool> shutdown_called{false};

    zkutil::ZooKeeperPtr zookeeper;

    UInt64 retry_period;

    bool skip_sanity_checks{false};

    void run();

    void tryReconnect();

    void resetCurrentZooKeeper();

    void finalizeInitialization();

    template <typename Function>
    decltype(auto) withRetries(Function && fn)
    {
        while (true)
        {
            try
            {
                return fn();
            }
            catch (const Coordination::Exception & e)
            {
                if (e.code == Coordination::Error::ZCONNECTIONLOSS || e.code == Coordination::Error::ZSESSIONEXPIRED)
                {
                    if (first_try)
                        resetCurrentZooKeeper();

                    LOG_TRACE(log, "Lost connection to ZooKeeper, will try to reconnect");
                    notifyIfFirstTry();
                    tryReconnect();
                }
                else if (e.code == Coordination::Error::ZOPERATIONTIMEOUT)
                    LOG_TRACE(log, "Operation timeout, will retry again");
                else
                    throw;

                if (shutdown_called)
                    throw Exception(ErrorCodes::ABORTED, "Shutdown called");
            }
        }
    }

    void notifyIfFirstTry();
};

}
