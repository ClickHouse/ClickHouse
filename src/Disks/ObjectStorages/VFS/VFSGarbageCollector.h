#pragma once
#include "AppendLog.h"
#include "VFSSnapshot.h"

#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/VFS/VFSSnapshot.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <fmt/chrono.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

class Context;

struct GarbageCollectorSettings
{
    String zk_gc_path;
    size_t gc_sleep_ms;
    double keeper_fault_injection_probability;
    UInt64 keeper_fault_injection_seed;
    size_t batch_size;
};

class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

class VFSGarbageCollector
{
public:
    VFSGarbageCollector(
        const String & gc_name_,
        ObjectStoragePtr object_storage_,
        VFSLogPtr wal_,
        BackgroundSchedulePool & pool,
        const GarbageCollectorSettings & settings_);
    
    ~VFSGarbageCollector();

    void shutdown();

private:
    void run();
    void updateSnapshot();


    String getZKSnapshotPath() const;
    // Get current shapshot object path from zookeeper.
    SnapshotMetadata getSnapshotMetadata() const;
    void updateShapshotMetadata(const SnapshotMetadata & new_snapshot, int32_t znode_required_version) const;
    void initGCState() const;
    ZooKeeperWithFaultInjectionPtr getZookeeper() const;


    String gc_name;
    ObjectStoragePtr object_storage;
    VFSSnapshotDataFromObjectStorage vfs_shapshot_data;
    VFSLogPtr wal;

    const GarbageCollectorSettings settings;
    LoggerPtr log;

    BackgroundSchedulePoolTaskHolder task_handle;
};
using VFSGarbageCollectorPtr = std::shared_ptr<VFSGarbageCollector>;
}
