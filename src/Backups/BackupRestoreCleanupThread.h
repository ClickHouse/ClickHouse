#pragma once

#include <Poco/Event.h>
#include <Backups/BackupsWorker.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/Common.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/types.h>
#include <thread>
#include <atomic>


namespace DB
{

class BackupRestoreCleanupThread
{
public:
    explicit BackupRestoreCleanupThread(
        zkutil::GetZooKeeper get_zookeeper_,
        BackgroundSchedulePool & pool_,
        String root_zookeeper_path_,
        UInt64 check_period_ms_,
        UInt64 timeout_to_cleanup_ms_);

    void start() { task->activateAndSchedule(); }
    void shutdown();

private:
    zkutil::GetZooKeeper get_zookeeper;

    Poco::Logger * log;
    std::atomic<bool> need_stop {false};

    /// The random data we wrote into `/replicas/me/is_active`.
    String root_zookeeper_path;

    BackgroundSchedulePool::TaskHolder task;
    UInt64 check_period_ms;
    UInt64 timeout_to_cleanup_ms;

    void run();

    /// Restarts table if needed, returns false if it failed to restart replica.
    void runImpl();
};


}
