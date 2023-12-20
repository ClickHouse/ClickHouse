#pragma once
#include "Poco/Logger.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Core/BackgroundSchedulePool.h"
#include "VFSLogItem.h"
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

    void updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer);
    VFSLogItem getBatch(size_t start_logpointer, size_t end_logpointer) const;
    void removeBatch(size_t start_logpointer, size_t end_logpointer);

    String getNode(size_t id) const;
    StoredObject getSnapshotObject(size_t logpointer) const;
};
}
