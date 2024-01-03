#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"
#include "ObjectStorageVFSGCThread.h"
#include "VFSTraits.h"

namespace DB
{
// A wrapper for object storage which counts references to objects using a transaction log in Keeper.
// Operations don't remove data from object storage immediately -- a garbage collector is responsible for that.
class DiskObjectStorageVFS final : public DiskObjectStorage
{
public:
    DiskObjectStorageVFS(
        const String & name,
        const String & object_storage_root_path_,
        const String & log_name,
        MetadataStoragePtr metadata_storage_,
        ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        bool enable_gc_);
    ~DiskObjectStorageVFS() override = default;

    void startupImpl(ContextPtr context) override;
    void shutdown() override;

    // TODO myrrc support changing disk settings in runtime e.g. GC interval

    bool isObjectStorageVFS() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const;

    bool lock(std::string_view path, bool block) override;
    void unlock(std::string_view path) override;

    bool shouldUploadMetadata(const String & lock_prefix);
    void uploadMetadata(const String & lock_prefix, const String & path);
    void downloadMetadata(const String & lock_prefix, const String & path);

private:
    friend struct RemoveRecursiveObjectStorageVFSOperation;
    friend struct RemoveManyObjectStorageVFSOperation;
    friend struct CopyFileObjectStorageVFSOperation;
    friend struct DiskObjectStorageVFSTransaction;
    friend class ObjectStorageVFSGCThread;

    std::optional<ObjectStorageVFSGCThread> garbage_collector;

    const bool enable_gc;
    const UInt64 gc_sleep_ms;
    const VFSTraits traits;

    zkutil::ZooKeeperPtr cached_zookeeper;
    zkutil::ZooKeeperPtr zookeeper();

    DiskTransactionPtr createObjectStorageTransaction() final;
    String lockPathToFullPath(std::string_view path) const;
};
}
