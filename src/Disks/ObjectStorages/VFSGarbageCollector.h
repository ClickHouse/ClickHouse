#pragma once
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include "VFSLogItem.h"
#include "VFSSettings.h"

namespace DB
{
class DiskObjectStorageVFS;

class VFSGarbageCollector : private BackgroundSchedulePoolTaskHolder
{
public:
    VFSGarbageCollector(DiskObjectStorageVFS & disk_, BackgroundSchedulePool & pool);
    inline void stop() { (*this)->deactivate(); }
    using Logpointer = size_t;

private:
    struct OptimisticLock
    {
        String snapshot;
        int32_t version;
    };

    void run() const;
    OptimisticLock createLock() const;
    bool skipRun(size_t batch_size, Logpointer start) const;
    VFSLogItem getBatch(Logpointer start, Logpointer end) const;
    void updateSnapshotWithLogEntries(
        Logpointer start, Logpointer end, const StoredObject & old_snapshot, const StoredObject & new_snapshot) const;
    bool releaseLockAndRemoveEntries(OptimisticLock && lock, Logpointer start, Logpointer end) const;

    void createLockNodes(Coordination::ZooKeeperWithFaultInjection & zookeeper) const;
    String getNode(Logpointer ptr) const;

    DiskObjectStorageVFS & disk;
    LoggerPtr log;
    std::shared_ptr<const VFSSettings> settings;
};
}
