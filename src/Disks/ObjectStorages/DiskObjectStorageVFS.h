#pragma once
#include "Common/ZooKeeper/ZooKeeper.h"
#include "DiskObjectStorage.h"
#include "ObjectStorageVFSGCThread.h"
#include "VFSTraits.h"

namespace DB
{

// class VFSTraits
// {
// public:
//     String VFS_BASE_NODE = "/vfs_log";
//     String VFS_LOCKS_NODE = VFS_BASE_NODE + "/locks";
//     String VFS_LOG_BASE_NODE = VFS_BASE_NODE + "/ops";
//     String VFS_LOG_ITEM = VFS_LOG_BASE_NODE + "/log-";
// };


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
    ~DiskObjectStorageVFS() override = default;

    void startupImpl(ContextPtr context) override;
    void shutdown() override;

    bool isObjectStorageVFS() const override { return true; }
    bool supportZeroCopyReplication() const override { return false; }
    DiskObjectStoragePtr createDiskObjectStorage() override;
    String getStructure() const;

    bool lock(std::string_view path, bool block) override;
    void unlock(std::string_view path) override;

    // Used only while moving parts from a local disk to VFS. Performs deduplication -- if target file
    // is present on disk, copies its metadata to local filesystem
    void copyFileReverse(
        const String & from_file_path,
        IDisk & from_disk,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override;

    VFSTraits traits;

private:
    // TODO myrrc not sure we should couple object storage and garbage collector this way
    // This has a downside that e.g. clickhouse-disks usage spins up a GC each time we issue
    // a command which we surely don't want to do.
    // Also having a GC per disk currently means we have multiple threads competing for log entry cleanup
    // as log is global (thus all of threads per replica except for one just waste resources).
    friend class ObjectStorageVFSGCThread;
    std::unique_ptr<ObjectStorageVFSGCThread> gc_thread;

    zkutil::ZooKeeperPtr zookeeper;

    DiskTransactionPtr createObjectStorageTransaction() final;

    String lockPathToFullPath(std::string_view path);

};
}
