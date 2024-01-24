#pragma once
#include "Common/MultiVersion.h"
#include "Common/ZooKeeper/ZooKeeperWithFaultInjection.h"
#include "DiskObjectStorage.h"
#include "ObjectStorageVFSGCThread.h"
#include "VFSSettings.h"

namespace DB
{
// A wrapper for object storage which counts references to objects using a transaction log in Keeper.
// Operations don't remove data from object storage immediately -- a garbage collector is responsible for that.
class DiskObjectStorageVFS final : public DiskObjectStorage
{
public:
    DiskObjectStorageVFS(
        const String & name,
        const String & object_key_prefix_,
        MetadataStoragePtr metadata_storage_,
        ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        bool enable_gc_);
    ~DiskObjectStorageVFS() override = default;

    void startupImpl(ContextPtr context) override;
    void shutdown() override;
    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr, const String &, const DisksMap &) override;

    constexpr bool isObjectStorageVFS() const override { return true; }
    constexpr bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const override;

    bool lock(std::string_view path, bool block) override;
    void unlock(std::string_view path) override;

    bool tryDownloadMetadata(std::string_view remote_from, const String & to);
    void uploadMetadata(std::string_view remote_to, const String & from);

private:
    friend struct RemoveRecursiveObjectStorageVFSOperation;
    friend struct RemoveManyObjectStorageVFSOperation;
    friend struct CopyFileObjectStorageVFSOperation;
    friend struct DiskObjectStorageVFSTransaction;
    friend class ObjectStorageVFSGCThread;

    std::optional<ObjectStorageVFSGCThread> garbage_collector;

    const bool enable_gc; // In certain conditions we don't want a GC e.g. when running from clickhouse-disks
    const ObjectStorageType object_storage_type;
    const VFSTraits traits;
    MultiVersion<VFSSettings> settings;

    ZooKeeperWithFaultInjection::Ptr zookeeper();

    DiskTransactionPtr createObjectStorageTransaction() final;
    String lockPathToFullPath(std::string_view path) const;
    StoredObject getMetadataObject(std::string_view remote) const;
};
}
