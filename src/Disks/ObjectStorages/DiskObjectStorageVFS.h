#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"
#include "ObjectStorageVFSGCThread.h"

namespace DB
{
// A wrapper for object storage (currently only s3 disk is supported) which counts references to objects
// using a transaction log in Keeper. Disk operations don't remove data from object storage,
// a separate entity (garbage collector) is responsible for that.
class DiskObjectStorageVFS : public DiskObjectStorage
{
public:
    using DiskObjectStorage::DiskObjectStorage;
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
    // TODO myrrc not sure we should couple object storage and garbage collector this way
    // This has a downside that e.g. clickhouse-disks usage spins up a GC each time we issue
    // a command which we surely don't want to do
    friend class ObjectStorageVFSGCThread;
    std::unique_ptr<ObjectStorageVFSGCThread> gc_thread;

    zkutil::ZooKeeperPtr zookeeper;

    DiskTransactionPtr createObjectStorageTransaction() final;

    std::unique_ptr<ReadBufferFromFileBase> readObject(const StoredObject & object);
    void removeObjects(StoredObjects && objects);

    void preAccessCheck(ContextPtr context) override;
};
}
