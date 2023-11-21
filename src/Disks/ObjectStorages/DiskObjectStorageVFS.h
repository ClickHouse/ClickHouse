#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"

namespace DB
{
class ObjectStorageVFSGCThread;

// A wrapper for object storage (currently only s3 disk is supported) which counts references to objects
// using a transaction log in Keeper. Disk operations don't remove data from object storage,
// a separate entity (garbage collector) is responsible for that.
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
        zkutil::ZooKeeperPtr zookeeper_);

    ~DiskObjectStorageVFS() override;

    void startupImpl(ContextPtr context) override;
    void shutdown() override;

    bool isObjectStorageVFS() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const;

    bool lock(std::string_view path, bool block) override;
    void unlock(std::string_view path) override;

private:
    // TODO myrrc not sure we should couple object storage and garbage collector this way
    // This has a downside that e.g. clickhouse-disks usage spins up a GC each time we issue
    // a command which we surely don't want to do
    friend class ObjectStorageVFSGCThread;
    std::unique_ptr<ObjectStorageVFSGCThread> gc_thread;

    zkutil::ZooKeeperPtr zookeeper;

    DiskTransactionPtr createObjectStorageTransaction() final;

    std::unique_ptr<ReadBufferFromFileBase> readObject(const StoredObject& object);
    void removeObjects(StoredObjects && objects);
};
}
