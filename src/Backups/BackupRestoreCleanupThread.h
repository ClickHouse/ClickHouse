#pragma once

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/Common.h>
#include <Core/BackgroundSchedulePool.h>
#include <base/types.h>
#include <thread>
#include <atomic>


namespace DB
{


/// Background garbage collection job which cleans up stale nodes from [ZooKeeper]
/// created by backup or restore operation and didn't finish by whatever reason.
/// e.g. server crash or similar.
/// Has several parameters:
/// - path in [Zoo]Keeper (/clickhouse/backups)
/// - check_period_ms - how often does this task executes in background
/// - timeout_to_cleanup_ms - after how much time from the last possible activity we can consider
///     a backup or restore operation as stale or dead and remove corresponding nodes
/// - consecutive_failed_checks_to_be_stale - how many checks should be performed before deletion.
///
/// So, nodes from [Zoo]Keeper will be deleted iff a certain time is passed and we checked several times
/// whether this operation is active.
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
