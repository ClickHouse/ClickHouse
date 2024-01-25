#pragma once
#include "Poco/Logger.h"
#include "Core/BackgroundSchedulePool.h"
#include "VFSLogItem.h"
#include "VFSSettings.h"

namespace DB
{
class DiskObjectStorageVFS;

class ObjectStorageVFSGCThread
{
public:
    ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool);
    inline void stop() { task->deactivate(); }
    using Logpointer = size_t;

private:
    DiskObjectStorageVFS & storage;
    Poco::Logger * const log;
    BackgroundSchedulePoolTaskHolder task;
    std::shared_ptr<const VFSSettings> settings;

    void run();
    bool skipRun(size_t batch_size, Logpointer start, Logpointer end) const;
    void tryWriteSnapshotForZero() const;
    void updateSnapshotWithLogEntries(Logpointer start, Logpointer end) const;
    Logpointer reconcileLogWithSnapshot(Logpointer start, Logpointer end, Exception && e) const;
    VFSLogItem getBatch(Logpointer start, Logpointer end) const;
    void removeBatch(Logpointer start, Logpointer end) const;
    String getNode(Logpointer ptr) const;
    StoredObject getSnapshotObject(Logpointer ptr) const;
};
}
