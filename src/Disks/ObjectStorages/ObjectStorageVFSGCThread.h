#pragma once
#include "Poco/Logger.h"
#include "Core/BackgroundSchedulePool.h"
#include "VFSLogItem.h"
#include "base/types.h"

namespace DB
{
class DiskObjectStorageVFS;

class ObjectStorageVFSGCThread
{
public:
    ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool);
    inline void stop() { task->deactivate(); }

private:
    DiskObjectStorageVFS & storage;
    Poco::Logger * const log;
    BackgroundSchedulePoolTaskHolder task;
    const String lock_path;

    void run() const;
    void updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer) const;
    VFSLogItem getBatch(size_t start_logpointer, size_t end_logpointer) const;
    void removeBatch(size_t start_logpointer, size_t end_logpointer) const;
    String getNode(size_t id) const;
    StoredObject getSnapshotObject(size_t logpointer) const;
};
}
