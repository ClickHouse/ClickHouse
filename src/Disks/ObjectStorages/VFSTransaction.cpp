#include "VFSTransaction.h"
#include <Disks/IO/WriteBufferWithFinalizeCallback.h>
#include "DiskObjectStorageTransactionOperation.h"
#include "DiskObjectStorageVFS.h"
#include "VFSLogItem.h"
#include "VFSTransactionGroup.h"

namespace DB
{
constexpr VFSLogItem * leftIfNonNull(VFSLogItem * left, VFSLogItem * right)
{
    return left ? left : right;
}

VFSTransaction::VFSTransaction(DiskObjectStorageVFS & disk_)
    // nullptr as send_metadata is prohibited in VFS disk constructor
    : DiskObjectStorageTransaction(*disk_.object_storage, *disk_.metadata_storage, nullptr)
    , disk(disk_)
    , item(*leftIfNonNull(disk.get(), this))
{
}

const int pers_seq = zkutil::CreateMode::PersistentSequential;
void VFSTransaction::commit()
{
    DiskObjectStorageTransaction::commit();
    SCOPE_EXIT(item.clear());

    if (const Strings nodes = item.serialize(); nodes.empty())
        LOG_TRACE(disk.log, "VFSTransaction: nothing to commit");
    else if (nodes.size() == 1)
    {
        LOG_TRACE(disk.log, "VFSTransaction: executing {}", nodes[0]);
        disk.zookeeper()->create(disk.nodes.log_item, nodes[0], pers_seq);
    }
    else
    {
        LOG_TRACE(disk.log, "VFSTransaction: executing many {}", fmt::join(nodes, "\n"));

        Coordination::Requests req;
        req.reserve(nodes.size());
        for (const auto & node : nodes)
            req.emplace_back(zkutil::makeCreateRequest(disk.nodes.log_item, node, pers_seq));
        disk.zookeeper()->multi(req);
    }
}

void VFSTransaction::replaceFile(const String & from_path, const String & to_path)
{
    DiskObjectStorageTransaction::replaceFile(from_path, to_path);
    if (!metadata_storage.exists(to_path))
        return;
    // Remote file at from_path isn't changed, we just move it
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(to_path));
}

void VFSTransaction::removeFileIfExists(const String & path)
{
    removeSharedFileIfExists(path, true);
}

void VFSTransaction::removeSharedFile(const String & path, bool)
{
    DiskObjectStorageTransaction::removeSharedFile(path, /*keep_shared_data=*/true);
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(path));
}

void VFSTransaction::removeSharedFileIfExists(const String & path, bool)
{
    if (!metadata_storage.exists(path))
        return;
    DiskObjectStorageTransaction::removeSharedFileIfExists(path, /*keep_shared_data=*/true);
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(path));
}

struct RemoveRecursiveObjectStorageVFSOperation final : RemoveRecursiveObjectStorageOperation
{
    VFSLogItem & item;

    RemoveRecursiveObjectStorageVFSOperation(
        IObjectStorage & object_storage_, IMetadataStorage & metadata_storage_, const String & path_, VFSLogItem & item_)
        : RemoveRecursiveObjectStorageOperation(
            object_storage_,
            metadata_storage_,
            path_,
            /*keep_all_batch_data=*/true,
            {})
        , item(item_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveRecursiveObjectStorageOperation::execute(tx);
        for (const auto & [_, unlink_by_path] : objects_to_remove_by_path)
            for (const auto & obj : unlink_by_path.objects)
                item.remove(obj);
    }
};

void VFSTransaction::removeSharedRecursive(const String & path, bool, const NameSet &)
{
    operations_to_execute.emplace_back(
        std::make_unique<RemoveRecursiveObjectStorageVFSOperation>(*disk.object_storage, *disk.metadata_storage, path, item));
}

struct RemoveManyObjectStorageVFSOperation final : RemoveManyObjectStorageOperation
{
    VFSLogItem & item;

    RemoveManyObjectStorageVFSOperation(
        IObjectStorage & object_storage_, IMetadataStorage & metadata_storage_, const RemoveBatchRequest & request_, VFSLogItem & item_)
        : RemoveManyObjectStorageOperation(
            object_storage_,
            metadata_storage_,
            request_,
            /*keep_all_batch_data=*/false, // Different behaviour compared to RemoveObjectStorageOperation
            {})
        , item(item_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveManyObjectStorageOperation::execute(tx);
        for (const auto & [objects, _] : objects_to_remove)
            for (const auto & obj : objects)
                item.remove(obj);
    }

    void finalize() override { }
};

void VFSTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool, const NameSet &)
{
    operations_to_execute.emplace_back(
        std::make_unique<RemoveManyObjectStorageVFSOperation>(*disk.object_storage, *disk.metadata_storage, files, item));
}

// createFile creates an empty file. If writeFile is called, we'd
// account hardlinks, if it's not, no need to track it.
// If we create a hardlink to an empty file or copy it, there will be no associated metadata

std::unique_ptr<WriteBufferFromFileBase>
VFSTransaction::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings, bool autocommit)
{
    if (settings.vfs_is_metadata_file)
    {
        LOG_TRACE(disk.log, "writeFile(vfs_metadata=true)");
        chassert(autocommit);
        // TODO myrrc research whether there's any optimal way except for writing file and immediately
        // reading it back
        return std::make_unique<WriteBufferWithFinalizeCallback>(
            std::make_unique<WriteBufferFromFile>(path, buf_size),
            [tx = shared_from_this(), path](size_t)
            {
                tx->addStoredObjectsOp(tx->metadata_storage.getStorageObjects(path), {});
                tx->commit();
            },
            "");
    }

    StoredObjects currently_existing_blobs = metadata_storage.exists(path) ? metadata_storage.getStorageObjects(path) : StoredObjects{};
    StoredObject blob;

    auto buffer = writeFileOps(path, buf_size, mode, settings, autocommit, blob);
    addStoredObjectsOp({std::move(blob)}, std::move(currently_existing_blobs));
    return buffer;
}

void VFSTransaction::writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    // TODO myrrc right now this function isn't used in data parts exchange protocol but can we be sure
    // this won't change in the near future? Maybe add chassert(!path.ends_with(":vfs"))
    StoredObjects currently_existing_blobs = metadata_storage.exists(path) ? metadata_storage.getStorageObjects(path) : StoredObjects{};
    StoredObject blob;

    writeFileUsingBlobWritingFunctionOps(path, mode, std::move(write_blob_function), blob);
    addStoredObjectsOp({std::move(blob)}, std::move(currently_existing_blobs));
}

void VFSTransaction::createHardLink(const String & src_path, const String & dst_path)
{
    DiskObjectStorageTransaction::createHardLink(src_path, dst_path);
    addStoredObjectsOp(metadata_storage.getStorageObjects(src_path), {});
}

struct CopyFileObjectStorageVFSOperation final : CopyFileObjectStorageOperation
{
    VFSLogItem & item;

    CopyFileObjectStorageVFSOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        IObjectStorage & destination_object_storage_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const String & from_path_,
        const String & to_path_,
        VFSLogItem & item_)
        : CopyFileObjectStorageOperation(
            object_storage_, metadata_storage_, destination_object_storage_, read_settings_, write_settings_, from_path_, to_path_)
        , item(item_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        CopyFileObjectStorageOperation::execute(tx);
        for (const auto & obj : created_objects)
            item.add(obj);
    }
};

void VFSTransaction::copyFile(
    const String & from_file_path, const String & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(std::make_unique<CopyFileObjectStorageVFSOperation>(
        *disk.object_storage,
        *disk.metadata_storage,
        *disk.object_storage,
        read_settings,
        write_settings,
        from_file_path,
        to_file_path,
        item));
}

void VFSTransaction::addStoredObjectsOp(StoredObjects && link, StoredObjects && unlink)
{
    if (link.empty() && unlink.empty()) [[unlikely]]
        return;
    LOG_TRACE(disk.log, "Pushing:\nlink:{}\nunlink:{}", fmt::join(link, "\n"), fmt::join(unlink, "\n"));
    for (const auto & obj : link)
        item.add(obj);
    for (const auto & obj : unlink)
        item.remove(obj);
}

MultipleDisksVFSTransaction::MultipleDisksVFSTransaction(DiskObjectStorageVFS & disk_, IObjectStorage & destination_object_storage_)
    : VFSTransaction(disk_), destination_object_storage(destination_object_storage_)
{
}

void MultipleDisksVFSTransaction::copyFile(
    const String & from_file_path, const String & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(std::make_unique<CopyFileObjectStorageVFSOperation>(
        object_storage, metadata_storage, destination_object_storage, read_settings, write_settings, from_file_path, to_file_path, item));
}
}
