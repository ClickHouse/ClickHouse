#include "DiskObjectStorageVFSTransaction.h"
#include "Disks/ObjectStorages/DiskObjectStorageTransactionOperation.h"
#include "Disks/ObjectStorages/VFSTransactionLog.h"

namespace DB
{
// TODO myrrc not actually Zookeeper-related
struct KeeperOperation : public IDiskObjectStorageOperation
{
    std::function<void()> cb;

    KeeperOperation( //NOLINT
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        decltype(cb) && cb_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_), cb(std::move(cb_))
    {
    }

    void execute(MetadataTransactionPtr) override { cb(); }
    void undo() override { }
    void finalize() override { }
    String getInfoForLog() const override { return "KeeperOperation"; }
};

DiskObjectStorageVFSTransaction::DiskObjectStorageVFSTransaction( //NOLINT
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    zkutil::ZooKeeperPtr zookeeper_)
    : DiskObjectStorageTransaction(object_storage_, metadata_storage_, nullptr), zookeeper(zookeeper_)
    , log(&Poco::Logger::get("DiskObjectStorageVFS"))
{
}

void DiskObjectStorageVFSTransaction::replaceFile(const String & from_path, const String & to_path)
{
    LOG_TRACE(log, "replaceFile");
    DiskObjectStorageTransaction::replaceFile(from_path, to_path);
    if (!metadata_storage.exists(to_path))
        return;
    // Remote file at from_path isn't changed, we just move it
    addStoredObjectsOp(VFSTransactionLogItem::Type::Unlink, metadata_storage.getStorageObjects(to_path));
}

void DiskObjectStorageVFSTransaction::removeFileIfExists(const String & path)
{
    LOG_TRACE(log, "removeFileIfExists");
    removeSharedFileIfExists(path, true);
}

void DiskObjectStorageVFSTransaction::removeSharedFile(const String & path, bool)
{
    LOG_TRACE(log, "removeSharedFile");
    DiskObjectStorageTransaction::removeSharedFile(path, /*keep_shared_data=*/true);
    addStoredObjectsOp(VFSTransactionLogItem::Type::Unlink, metadata_storage.getStorageObjects(path));
}

void DiskObjectStorageVFSTransaction::removeSharedFileIfExists(const String & path, bool)
{
    LOG_TRACE(log, "removeSharedFileIfExists");
    DiskObjectStorageTransaction::removeSharedFileIfExists(path, /*keep_shared_data=*/true);
    addStoredObjectsOp(VFSTransactionLogItem::Type::Unlink, metadata_storage.getStorageObjects(path));
}

struct RemoveRecursiveObjectStorageVFSOperation final : RemoveRecursiveObjectStorageOperation
{
    zkutil::ZooKeeperPtr zookeeper;

    RemoveRecursiveObjectStorageVFSOperation( // NOLINT
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        zkutil::ZooKeeperPtr zookeeper_)
        : RemoveRecursiveObjectStorageOperation(
            object_storage_,
            metadata_storage_,
            path_,
            /*keep_all_batch_data=*/true,
            /*remove_metadata_only*/ {})
        , zookeeper(std::move(zookeeper_))
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveRecursiveObjectStorageOperation::execute(tx);
        Coordination::Requests ops;
        for (const auto & [_, objects_to_remove] : objects_to_remove_by_path)
            getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type::Unlink, objects_to_remove.objects, ops);
        zookeeper->multi(ops);
    }
};

void DiskObjectStorageVFSTransaction::removeSharedRecursive(const String & path, bool, const NameSet &)
{
    LOG_TRACE(log, "removeSharedRecursive");
    operations_to_execute.emplace_back(std::make_unique<RemoveRecursiveObjectStorageVFSOperation>( //NOLINT
        object_storage,
        metadata_storage,
        path,
        zookeeper));
}

void DiskObjectStorageVFSTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool, const NameSet &)
{
    LOG_TRACE(log, "removeSharedFiles");
    // TODO myrrc should have something better than that, but not critical as for now
    for (const auto & file : files)
        removeSharedFileIfExists(file.path, false);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageVFSTransaction::writeFile( /// NOLINT
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings,
    bool autocommit)
{
    LOG_TRACE(log, "writeFile(autocommit={})", autocommit);
    StoredObjects currently_existing_blobs = metadata_storage.exists(path) ? metadata_storage.getStorageObjects(path) : StoredObjects{};
    StoredObject blob;

    auto buffer = writeFileOps(path, buf_size, mode, settings, autocommit, blob);

    // TODO myrrc this is not committed when autocommit=on
    // And this possibly should be grouped in a single Keeper transaction instead of three
    const StoredObjects objects = {{std::move(blob)}};
    addStoredObjectsOp(VFSTransactionLogItem::Type::CreateInode, objects);
    addStoredObjectsOp(VFSTransactionLogItem::Type::Link, objects);

    if (!currently_existing_blobs.empty())
        addStoredObjectsOp(VFSTransactionLogItem::Type::Unlink, currently_existing_blobs);

    return buffer;
}

void DiskObjectStorageVFSTransaction::writeFileUsingBlobWritingFunction(
    const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    LOG_TRACE(log, "writeFileWithFunction");
    StoredObjects currently_existing_blobs = metadata_storage.exists(path) ? metadata_storage.getStorageObjects(path) : StoredObjects{};
    StoredObject blob;

    writeFileUsingBlobWritingFunctionOps(path, mode, std::move(write_blob_function), blob);

    // TODO myrrc this possibly should be grouped in a single Keeper transaction instead of two

    const StoredObjects objects = {{std::move(blob)}};
    addStoredObjectsOp(VFSTransactionLogItem::Type::CreateInode, objects);
    addStoredObjectsOp(VFSTransactionLogItem::Type::Link, objects);

    if (!currently_existing_blobs.empty())
        addStoredObjectsOp(VFSTransactionLogItem::Type::Unlink, currently_existing_blobs);
}

void DiskObjectStorageVFSTransaction::createHardLink(const String & src_path, const String & dst_path)
{
    LOG_TRACE(log, "createHardLink");
    DiskObjectStorageTransaction::createHardLink(src_path, dst_path);
    addStoredObjectsOp(VFSTransactionLogItem::Type::Link, metadata_storage.getStorageObjects(src_path));
}

// Unfortunately, knowledge of object storage blob path doesn't go beyond
// this structure, so we need to write to Zookeeper inside of execute().
// Another option is to add another operation that would deserialize metadata file at to_path,
// get remote path and write to Zookeeper, but the former seems less ugly to me
struct CopyFileObjectStorageVFSOperation final : CopyFileObjectStorageOperation
{
    zkutil::ZooKeeperPtr zookeeper;

    CopyFileObjectStorageVFSOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const String & from_path_,
        const String & to_path_,
        zkutil::ZooKeeperPtr zookeeper_)
        : CopyFileObjectStorageOperation( //NOLINT
            object_storage_,
            metadata_storage_,
            read_settings_,
            write_settings_,
            from_path_,
            to_path_)
        , zookeeper(std::move(zookeeper_))
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        CopyFileObjectStorageOperation::execute(tx);
        Coordination::Requests ops;
        getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type::CreateInode, created_objects, ops);
        getStoredObjectsVFSLogOps(VFSTransactionLogItem::Type::Link, created_objects, ops);
        zookeeper->multi(ops);
    }
};

void DiskObjectStorageVFSTransaction::copyFile( //NOLINT
    const String & from_file_path,
    const String & to_file_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    LOG_TRACE(log, "copyFile");
    operations_to_execute.emplace_back(std::make_unique<CopyFileObjectStorageVFSOperation>( //NOLINT
        object_storage,
        metadata_storage,
        read_settings,
        write_settings,
        from_file_path,
        to_file_path,
        zookeeper));
}

void DiskObjectStorageVFSTransaction::addStoredObjectsOp(VFSTransactionLogItem::Type type, const StoredObjects & objects)
{
    LOG_TRACE(log, "Pushing {} {}", type, fmt::join(objects, ", "));
    operations_to_execute.emplace_back(std::make_unique<KeeperOperation>(
        object_storage,
        metadata_storage,
        [zk = this->zookeeper, type, objects, log = this->log]
        {
            LOG_TRACE(log, "Executing {} {}", type, fmt::join(objects, ", "));
            Coordination::Requests ops;
            getStoredObjectsVFSLogOps(type, objects, ops);
            zk->multi(ops);
        }));
}
}
