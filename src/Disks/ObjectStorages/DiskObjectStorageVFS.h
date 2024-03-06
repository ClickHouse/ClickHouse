#pragma once
#include <Common/MultiVersion.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include "DiskObjectStorage.h"
#include "VFSGarbageCollector.h"
#include "VFSMetadataStorage.h"
#include "VFSSettings.h"
#include "VFSTransactionGroupStorage.h"

namespace DB
{
// A wrapper for object storage which counts references to objects using a transaction log in Keeper.
// Operations don't remove data from object storage immediately -- a garbage collector is responsible for that.
class DiskObjectStorageVFS final : public DiskObjectStorage, public VFSMetadataStorage, VFSTransactionGroupStorage
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

private:
    friend struct VFSTransaction;
    friend struct VFSTransactionGroup;
    friend struct VFSMigration;
    friend class VFSGarbageCollector;

    std::optional<VFSGarbageCollector> garbage_collector;
    const bool enable_gc; // We don't want a GC e.g. when running from clickhouse-disks
    const VFSNodes nodes;
    MultiVersion<VFSSettings> settings;

    ZooKeeperWithFaultInjection::Ptr zookeeper() const;
    DiskTransactionPtr createObjectStorageTransaction() final;
    DiskTransactionPtr createObjectStorageTransactionToAnotherDisk(DiskObjectStorage & to_disk) final;
};
}
