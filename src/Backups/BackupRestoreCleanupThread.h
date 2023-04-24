#pragma once

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
        UInt64 timeout_to_cleanup_ms_,
        UInt64 consecutive_failed_checks_to_be_stale_);

    void start() { task->activateAndSchedule(); }
    void shutdown();

private:
    zkutil::GetZooKeeper get_zookeeper;
    Poco::Logger * log;
    String root_zookeeper_path;
    UInt64 check_period_ms;
    UInt64 timeout_to_cleanup_ms;
    UInt64 consecutive_failed_checks_to_be_stale;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> need_stop {false};
    std::unordered_map<String, size_t> dead_counter;

    void run();
    void runImpl();
};


}
