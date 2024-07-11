#pragma once
#include "AppendLog.h"
#include "VFSSnapshot.h"

#include <Core/BackgroundSchedulePool.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

#include <vector>
#include <fmt/chrono.h>

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

// Structure for keeping metadata about shapshot.
struct SnapshotMetadata
{
    uint64_t metadata_version;
    String object_storage_key;
    uint64_t total_size;
    int32_t znode_version;
    bool is_initial_snaphot;

    SnapshotMetadata(
        uint64_t metadata_version_ = 0ull,
        const String & object_storage_key_ = "",
        uint64_t total_size_ = 0ull,
        int32_t znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snaphot(is_initial_)
    {
    }

    SnapshotMetadata(
        const String & object_storage_key_,
        uint64_t metadata_version_ = 0ull,
        uint64_t total_size_ = 0ull,
        int32_t znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snaphot(is_initial_)
    {
    }

    void update(const String & new_snapshot_key) { object_storage_key = new_snapshot_key; }

    String serialize() const { return fmt::format("{} {} {} ", metadata_version, object_storage_key, total_size); }

    static SnapshotMetadata deserialize(const String & str, int32_t znode_version)
    {
        SnapshotMetadata result;
        result.znode_version = znode_version;
        /// In case of initial snaphot, the content will be empty.
        if (str.empty())
        {
            result.is_initial_snaphot = true;
            return result;
        }
        ReadBufferFromString rb(str);

        readIntTextUnsafe(result.metadata_version, rb);
        checkChar(' ', rb);
        readStringUntilWhitespace(result.object_storage_key, rb);
        checkChar(' ', rb);
        readIntTextUnsafe(result.total_size, rb);
        result.is_initial_snaphot = false;
        return result;
    }
};

class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

class VFSGarbageCollector : private BackgroundSchedulePoolTaskHolder
{
public:
    VFSGarbageCollector(
        const String & gc_name_,
        ObjectStoragePtr object_storage_,
        WAL::AppendLog & alog,
        BackgroundSchedulePool & pool,
        const GarbageCollectorSettings & settings_);

private:
    void run() const;
    void updateSnapshot() const;


    String getZKSnapshotPath() const;
    // Get current shapshot object path from zookeeper.
    SnapshotMetadata getSnapshotMetadata() const;
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const;
    std::pair<std::unique_ptr<WriteBuffer>, StoredObject>
    getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata & snapshot_meta) const;
    void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) const;
    void updateShapshotMetadata(const SnapshotMetadata & new_snapshot, int32_t znode_required_version) const;
    void initGCState() const;
    ZooKeeperWithFaultInjectionPtr getZookeeper() const;


    String gc_name;
    ObjectStoragePtr object_storage;
    WAL::AppendLog & alog;

    const GarbageCollectorSettings settings;
    LoggerPtr log;
};
}
