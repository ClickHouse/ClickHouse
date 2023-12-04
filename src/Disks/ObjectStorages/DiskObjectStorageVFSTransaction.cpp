#include "DiskObjectStorageVFSTransaction.h"
#include "Disks/IO/WriteBufferWithFinalizeCallback.h"
#include "Disks/ObjectStorages/DiskObjectStorageTransactionOperation.h"
#include "Disks/ObjectStorages/VFSTransactionLog.h"
#include "VFSTraits.h"
#include "base/FnTraits.h"

namespace DB
{
using enum VFSTransactionLogItem::Type;

DiskObjectStorageVFSTransaction::DiskObjectStorageVFSTransaction(
  IObjectStorage & object_storage_,
  IMetadataStorage & metadata_storage_,
  zkutil::ZooKeeperPtr zookeeper_,
  const VFSTraits & traits_)
    : DiskObjectStorageTransaction(object_storage_, metadata_storage_, nullptr)
    , zookeeper(std::move(zookeeper_))
    , log(&Poco::Logger::get("DiskObjectStorageVFS"))
    , traits(traits_)
{
}

void DiskObjectStorageVFSTransaction::replaceFile(const String & from_path, const String & to_path)
{
    DiskObjectStorageTransaction::replaceFile(from_path, to_path);
    // Remote file at from_path isn't changed, we just move it
    addStoredObjectsOp(Unlink, metadata_storage.exists(to_path) ? metadata_storage.getStorageObjects(to_path) : StoredObjects{});
}

void DiskObjectStorageVFSTransaction::removeFileIfExists(const String & path)
{
    removeSharedFileIfExists(path, true);
}

void DiskObjectStorageVFSTransaction::removeSharedFile(const String & path, bool)
{
    DiskObjectStorageTransaction::removeSharedFile(path, /*keep_shared_data=*/true);
    addStoredObjectsOp(Unlink, metadata_storage.getStorageObjects(path));
}

void DiskObjectStorageVFSTransaction::removeSharedFileIfExists(const String & path, bool)
{
    if (!metadata_storage.exists(path)) return;
    DiskObjectStorageTransaction::removeSharedFileIfExists(path, /*keep_shared_data=*/true);
    addStoredObjectsOp(Unlink, metadata_storage.getStorageObjects(path));
}

struct RemoveRecursiveObjectStorageVFSOperation final : RemoveRecursiveObjectStorageOperation
{
    zkutil::ZooKeeperPtr zookeeper;
    VFSTraits traits;

    RemoveRecursiveObjectStorageVFSOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const String & path_,
        zkutil::ZooKeeperPtr zookeeper_,
        VFSTraits traits_)
        : RemoveRecursiveObjectStorageOperation(
            object_storage_,
            metadata_storage_,
            path_,
            /*keep_all_batch_data=*/true,
            /*remove_metadata_only*/ {})
        , zookeeper(std::move(zookeeper_))
        , traits(traits_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveRecursiveObjectStorageOperation::execute(tx);
        Coordination::Requests requests;
        for (const auto & [_, objects_to_remove] : objects_to_remove_by_path)
            getStoredObjectsVFSLogOps(Unlink, objects_to_remove.objects, requests, traits);
        zookeeper->multi(requests);
    }
};

void DiskObjectStorageVFSTransaction::removeSharedRecursive(const String & path, bool, const NameSet &)
{
    operations_to_execute.emplace_back(
        std::make_unique<RemoveRecursiveObjectStorageVFSOperation>(object_storage, metadata_storage, path, zookeeper, traits));
}

void DiskObjectStorageVFSTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool, const NameSet &)
{
    // TODO myrrc should have something better than that, but not critical as for now
    for (const auto & file : files)
        removeSharedFileIfExists(file.path, false);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageVFSTransaction::writeFile(
    const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings, bool autocommit)
{
    const bool is_metadata_file_for_vfs = path.ends_with(":vfs");
    const String & path_without_tag = is_metadata_file_for_vfs ? path.substr(0, path.size() - 4) : path;

    LOG_TRACE(log, "writeFile(is_metadata={})", is_metadata_file_for_vfs);

    StoredObjects currently_existing_blobs
        = metadata_storage.exists(path_without_tag) ? metadata_storage.getStorageObjects(path_without_tag) : StoredObjects{};
    StoredObject blob;

    // This is a metadata file we got from some replica, we need to load it on local metadata disk
    // and add a Link entry
    if (is_metadata_file_for_vfs)
        // TODO myrrc transaction per file is slow, should revisit whether it's grouped in a DataPart
        // transaction (see DataPartsExchange.cpp -- downloadPartTodisk -> beginTransaction)
        // TODO myrrc research whether there's any optimal way except for writing file and immediately
        // reading it back
        return std::make_unique<WriteBufferWithFinalizeCallback>(
            std::make_unique<WriteBufferFromFile>(path_without_tag, buf_size),
            [tx = shared_from_this(), path_without_tag](size_t)
            {
                tx->addStoredObjectsOp(Link, tx->metadata_storage.getStorageObjects(path_without_tag));
                tx->commit();
            },
            "");

    auto buffer = writeFileOps(path_without_tag, buf_size, mode, settings, autocommit, blob);

    // TODO myrrc this possibly should be grouped in a single Keeper transaction instead of three
    addStoredObjectsOp(CreateInode, {blob});
    addStoredObjectsOp(Link, {blob});
    addStoredObjectsOp(Unlink, std::move(currently_existing_blobs));

    return buffer;
}

void DiskObjectStorageVFSTransaction::writeFileUsingBlobWritingFunction(
    const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    // TODO myrrc right now this function isn't used in data parts exchange protocol but can we be sure
    // this won't change in the near future? Maybe add chassert(!path.ends_with(":vfs"))
    StoredObjects currently_existing_blobs = metadata_storage.exists(path) ? metadata_storage.getStorageObjects(path) : StoredObjects{};
    StoredObject blob;

    writeFileUsingBlobWritingFunctionOps(path, mode, std::move(write_blob_function), blob);

    // TODO myrrc this possibly should be grouped in a single Keeper transaction instead of three
    addStoredObjectsOp(CreateInode, {blob});
    addStoredObjectsOp(Link, {blob});
    addStoredObjectsOp(Unlink, std::move(currently_existing_blobs));
}

void DiskObjectStorageVFSTransaction::createHardLink(const String & src_path, const String & dst_path)
{
    DiskObjectStorageTransaction::createHardLink(src_path, dst_path);
    addStoredObjectsOp(Link, metadata_storage.getStorageObjects(src_path));
}

// Unfortunately, knowledge of object storage blob path doesn't go beyond
// this structure, so we need to write to Zookeeper inside of execute().
// Another option is to add another operation that would deserialize metadata file at to_path,
// get remote path and write to Zookeeper, but the former seems less ugly to me
struct CopyFileObjectStorageVFSOperation final : CopyFileObjectStorageOperation
{
    zkutil::ZooKeeperPtr zookeeper;
    VFSTraits traits;

    CopyFileObjectStorageVFSOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const String & from_path_,
        const String & to_path_,
        zkutil::ZooKeeperPtr zookeeper_,
        const VFSTraits & traits_)
        : CopyFileObjectStorageOperation(object_storage_, metadata_storage_, read_settings_, write_settings_, from_path_, to_path_)
        , zookeeper(std::move(zookeeper_))
        , traits(traits_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        CopyFileObjectStorageOperation::execute(tx);
        Coordination::Requests requests;
        getStoredObjectsVFSLogOps(CreateInode, created_objects, requests, traits);
        getStoredObjectsVFSLogOps(Link, created_objects, requests, traits);
        zookeeper->multi(requests);
    }
};

void DiskObjectStorageVFSTransaction::copyFile(
    const String & from_file_path, const String & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(std::make_unique<CopyFileObjectStorageVFSOperation>(
            object_storage, metadata_storage, read_settings, write_settings, from_file_path, to_file_path, zookeeper, traits));
}

void DiskObjectStorageVFSTransaction::addStoredObjectsOp(VFSTransactionLogItem::Type type, StoredObjects && objects)
{
    if (objects.empty()) [[unlikely]]
        return;
    LOG_TRACE(log, "Pushing {} {}", type, fmt::join(objects, ", "));

    auto callback = [zk = this->zookeeper, type, objects_captured = std::move(objects), log_captured = this->log, traits = this->traits]
    {
        LOG_TRACE(log_captured, "Executing {} {}", type, fmt::join(objects_captured, "\n"));
        Coordination::Requests requests;
        getStoredObjectsVFSLogOps(type, objects_captured, requests, traits);
        zk->multi(requests);
    };

    operations_to_execute.emplace_back(
        std::make_unique<CallbackOperation<decltype(callback)>>(object_storage, metadata_storage, std::move(callback)));
}
}
