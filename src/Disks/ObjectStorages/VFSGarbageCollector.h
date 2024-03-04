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
    VFSGarbageCollector(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool);
    inline void stop() { (*this)->deactivate(); }
    using Logpointer = size_t;

private:
    struct LockNode
    {
        String snapshot;
        int32_t version;
    };

    LockNode getOptimisticLock() const;
    // Execute requests in transaction with checking if lock_node was modified
    bool releaseOptimisticLock(const LockNode & lock_node, Coordination::Requests && ops) const;
    String generateSnapshotName() const;

    DiskObjectStorageVFS & storage;
    LoggerPtr log;
    std::shared_ptr<const VFSSettings> settings;

    void run() const;
    bool skipRun(size_t batch_size, Logpointer start, Logpointer end) const;
    void updateSnapshotWithLogEntries(
        Logpointer start, Logpointer end, std::string_view old_snapshot_name, std::string_view new_snapshot_name) const;
    VFSLogItem getBatch(Logpointer start, Logpointer end) const;
    Coordination::Requests makeRemoveBatchRequests(Logpointer start, Logpointer end) const;
    String getNode(Logpointer ptr) const;
    void cleanSnapshots(const Strings & names) const;

    using FaultyKeeper = Coordination::ZooKeeperWithFaultInjection;
    void createLockNodes(FaultyKeeper & zookeeper) const;
};
}
