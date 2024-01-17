#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"
#include "ObjectStorageVFSGCThread.h"
#include "VFSTraits.h"

namespace DB
{

// A wrapper for object storage which counts references to objects using a transaction log in Keeper.
// Disk operations don't remove data from object storage immediately, a garbage collector is responsible
// for that.
class DiskObjectStorageVFS : public DiskObjectStorage
{
public:
    // TODO myrrc should just "using DiskObjectStorage::DiskObjectStorage" and fill zookeeper
    // in startupImpl but in checkAccessImpl (called before startupImpl) a file is written therefore
    // we need to have zookeeper already
    DiskObjectStorageVFS(
        const String & name,
        const String & object_storage_root_path_,
        const String & log_name,
        MetadataStoragePtr metadata_storage_,
        ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        zkutil::ZooKeeperPtr zookeeper_,
        bool allow_gc);
    ~DiskObjectStorageVFS() override = default;

    void startupImpl(ContextPtr context) override;
    void shutdown() override;

    bool isObjectStorageVFS() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const;

    bool lock(std::string_view path, bool block) override;
    void unlock(std::string_view path) override;

private:
    friend class ObjectStorageVFSGCThread;
    std::optional<ObjectStorageVFSGCThread> garbage_collector;
    bool allow_gc;

    UInt64 gc_thread_sleep_ms;
    VFSTraits traits;

    zkutil::ZooKeeperPtr zookeeper;

    DiskTransactionPtr createObjectStorageTransaction() final;
    String lockPathToFullPath(std::string_view path) const;
};
}
