#pragma once
#include "Poco/Logger.h"
#include "Common/Stopwatch.h"
#include "Core/BackgroundSchedulePool.h"
#include "Disks/ObjectStorages/VFSTransactionLog.h"
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
    ~ObjectStorageVFSGCThread();

    void start() { task->activateAndSchedule(); }
    void stop() { task->deactivate(); }

private:
    DiskObjectStorageVFS & storage;
    const String log_name;
    Poco::Logger const * const log;
    BackgroundSchedulePool::TaskHolder task;
    std::unique_ptr<zkutil::ZooKeeperLock> zookeeper_lock;
    UInt64 sleep_ms;

    void run();

    VFSSnapshot loadSnapshot(size_t end_logpointer);

    VFSSnapshot::ObsoleteObjects populateSnapshotWithLogEntries(VFSSnapshot & snapshot, size_t start_logpointer, size_t end_logpointer);
    void writeSnapshot(VFSSnapshot && snapshot, size_t end_logpointer);
    void removeObjectsFromObjectStorage(VFSSnapshot::ObsoleteObjects && items);
    void removeLogEntries(size_t start_logpointer, size_t end_logpointer);
};
}
