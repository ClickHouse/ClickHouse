#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"

namespace DB
{
// A wrapper for object storage (currently only s3 disk is supported) which counts references to objects
// using a transaction log in Keeper. Disk operations don't remove data from object storage,
// a separate entity (garbage collector) is responsible for that.
class DiskObjectStorageVFS : public DiskObjectStorage
{
public:
    // TODO myrrc should just "using DiskObjectStorage::DiskObjectStorage" and override startupImpl to fill
    // zookeeper but in checkAccessImpl (called before startupImpl) a file is written therefore we need to have
    // zookeeper already
    DiskObjectStorageVFS(
        const String & name,
        const String & object_storage_root_path_,
        const String & log_name,
        MetadataStoragePtr metadata_storage_,
        ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        zkutil::ZooKeeperPtr zookeeper_);

    bool isObjectStorageVFS() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const;

private:
    DiskTransactionPtr createObjectStorageTransaction() final;
    zkutil::ZooKeeperPtr zookeeper;
};
}
