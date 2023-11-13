#pragma once
#include "Poco/Logger.h"
#include "Common/Stopwatch.h"
#include "Core/BackgroundSchedulePool.h"
#include "base/types.h"

namespace zkutil
{
class ZooKeeperLock;
}

namespace DB
{
class DiskObjectStorageVFS;

// Despite the name, this thread handles not only garbage collection but also snapshot making and
// uploading it to corresponding object storage
class ObjectStorageVFSGCThread
{
public:
    ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context);

    void start() { task->activateAndSchedule(); }
    void wakeup() { task->schedule(); }
    void stop() { task->deactivate(); }

private:
    DiskObjectStorageVFS & storage;
    const String log_name;
    Poco::Logger const * const log;
    BackgroundSchedulePool::TaskHolder task;

    std::unique_ptr<zkutil::ZooKeeperLock> zookeeper_lock;

    UInt64 sleep_ms;

    std::atomic<UInt64> prev_cleanup_timestamp_ms = 0;
    AtomicStopwatch wakeup_check_timer;

    void run();
};
}
