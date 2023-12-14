#pragma once
#include "Poco/Logger.h"
#include "Common/Stopwatch.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Core/BackgroundSchedulePool.h"
#include "Disks/ObjectStorages/VFSTransactionLog.h"
#include "base/types.h"

namespace DB
{
class DiskObjectStorageVFS;

// Despite the name, this thread handles not only garbage collection but also snapshot making and
// uploading it to corresponding object storage
// TODO myrrc we should think about dropping the snapshot for log (at some point we want to remove
// even the latest snapshot if we e.g. clean the bucket)
class ObjectStorageVFSGCThread
{
public:
    ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context);
    ~ObjectStorageVFSGCThread();

    inline void stop() { task->deactivate(); }

private:
    DiskObjectStorageVFS & storage;
    Poco::Logger * const log;
    BackgroundSchedulePool::TaskHolder task;
    zkutil::ZooKeeperLock zookeeper_lock;

    void run();

    // Given a pair of log pointers, load a snapshot before start_logpointer and apply [start_logpointer;
    // end_logpointer] updates to it
    VFSSnapshotWithObsoleteObjects getSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer);
    String writeSnapshot(VFSSnapshot && snapshot, const String & snapshot_name);
    void onBatchProcessed(size_t start_logpointer, size_t end_logpointer, const String & snapshot_remote_path);

    String getNode(size_t id) const;
};
}
