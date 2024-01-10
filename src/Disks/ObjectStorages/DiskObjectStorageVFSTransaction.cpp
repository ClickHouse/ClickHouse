#include "DiskObjectStorageVFSTransaction.h"
#include "DiskObjectStorageTransactionOperation.h"
#include "DiskObjectStorageVFS.h"
#include "Disks/IO/WriteBufferWithFinalizeCallback.h"
#include "VFSLogItem.h"

namespace DB
{
DiskObjectStorageVFSTransaction::DiskObjectStorageVFSTransaction(DiskObjectStorageVFS & disk_)
    // nullptr passed as we prohibit send_metadata in VFS disk constructor
    : DiskObjectStorageTransaction(*disk_.object_storage, *disk_.metadata_storage, nullptr), disk(disk_)
{
}

void DiskObjectStorageVFSTransaction::replaceFile(const String & from_path, const String & to_path)
{
    DiskObjectStorageTransaction::replaceFile(from_path, to_path);
    if (!metadata_storage.exists(to_path))
        return;
    // Remote file at from_path isn't changed, we just move it
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(to_path));
}

void DiskObjectStorageVFSTransaction::removeFileIfExists(const String & path)
{
    removeSharedFileIfExists(path, true);
}

void DiskObjectStorageVFSTransaction::removeSharedFile(const String & path, bool)
{
    DiskObjectStorageTransaction::removeSharedFile(path, /*keep_shared_data=*/true);
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(path));
}

void DiskObjectStorageVFSTransaction::removeSharedFileIfExists(const String & path, bool)
{
    if (!metadata_storage.exists(path))
        return;
    DiskObjectStorageTransaction::removeSharedFileIfExists(path, /*keep_shared_data=*/true);
    addStoredObjectsOp({}, metadata_storage.getStorageObjects(path));
}

struct RemoveRecursiveObjectStorageVFSOperation final : RemoveRecursiveObjectStorageOperation
{
    DiskObjectStorageVFS & disk;

    RemoveRecursiveObjectStorageVFSOperation(DiskObjectStorageVFS & disk_, const String & path_)
        : RemoveRecursiveObjectStorageOperation(
            *disk_.object_storage,
            *disk_.metadata_storage,
            path_,
            /*keep_all_batch_data=*/true,
            {})
        , disk(disk_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveRecursiveObjectStorageOperation::execute(tx);
        StoredObjects unlink;
        for (auto && [_, unlink_by_path] : objects_to_remove_by_path)
            std::ranges::move(unlink_by_path.objects, std::back_inserter(unlink));
        const String entry = VFSLogItem::getSerialised({}, std::move(unlink));
        LOG_TRACE(disk.log, "{}: Executing {}", getInfoForLog(), entry);
        disk.zookeeper()->create(disk.traits.log_item, entry, zkutil::CreateMode::PersistentSequential);
    }
};

void DiskObjectStorageVFSTransaction::removeSharedRecursive(const String & path, bool, const NameSet &)
{
    operations_to_execute.emplace_back(std::make_unique<RemoveRecursiveObjectStorageVFSOperation>(disk, path));
}

struct RemoveManyObjectStorageVFSOperation final : RemoveManyObjectStorageOperation
{
    DiskObjectStorageVFS & disk;

    RemoveManyObjectStorageVFSOperation(DiskObjectStorageVFS & disk_, const RemoveBatchRequest & request_)
        : RemoveManyObjectStorageOperation(
            *disk_.object_storage,
            *disk_.metadata_storage,
            request_,
            /*keep_all_batch_data=*/false, // Different behaviour compared to RemoveObjectStorageOperation
            {})
        , disk(disk_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        RemoveManyObjectStorageOperation::execute(tx);
        StoredObjects unlink;
        for (auto && [objects, _] : objects_to_remove)
            std::ranges::move(objects, std::back_inserter(unlink));
        const String entry = VFSLogItem::getSerialised({}, std::move(unlink));
        LOG_TRACE(disk.log, "{}: Executing {}", getInfoForLog(), entry);
        disk.zookeeper()->create(disk.traits.log_item, entry, zkutil::CreateMode::PersistentSequential);
    }

    void finalize() override { }
};

void DiskObjectStorageVFSTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool, const NameSet &)
{
    operations_to_execute.emplace_back(std::make_unique<RemoveManyObjectStorageVFSOperation>(disk, files));
}

// createFile creates an empty file. If writeFile is called, we'd
// account hardlinks, if it's not, no need to track it.
// If we create a hardlink to an empty file or copy it, there will be no associated metadata

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageVFSTransaction::writeFile(
    const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings, bool autocommit)
{
    const bool is_metadata_file_for_vfs = path.ends_with(":vfs");
    const String path_without_tag = is_metadata_file_for_vfs ? path.substr(0, path.size() - 4) : path;

    LOG_TRACE(disk.log, "writeFile(is_metadata={})", is_metadata_file_for_vfs);

    // This is a metadata file we got from some replica, we need to load it on local metadata disk
    if (is_metadata_file_for_vfs)
    {
        chassert(autocommit);
        // TODO myrrc research whether there's any optimal way except for writing file and immediately
        // reading it back
        return std::make_unique<WriteBufferWithFinalizeCallback>(
            std::make_unique<WriteBufferFromFile>(path_without_tag, buf_size),
            [tx = shared_from_this(), path_without_tag](size_t)
            {
                // TODO myrrc this queries file on s3.
                tx->addStoredObjectsOp(tx->metadata_storage.getStorageObjects(path_without_tag), {});
                tx->commit();
            },
            "");
    }

    StoredObjects currently_existing_blobs
        = metadata_storage.exists(path_without_tag) ? metadata_storage.getStorageObjects(path_without_tag) : StoredObjects{};
    StoredObject blob;

    auto buffer = writeFileOps(path_without_tag, buf_size, mode, settings, autocommit, blob);
    addStoredObjectsOp({std::move(blob)}, std::move(currently_existing_blobs));
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
    addStoredObjectsOp({std::move(blob)}, std::move(currently_existing_blobs));
}

void DiskObjectStorageVFSTransaction::createHardLink(const String & src_path, const String & dst_path)
{
    DiskObjectStorageTransaction::createHardLink(src_path, dst_path);
    addStoredObjectsOp(metadata_storage.getStorageObjects(src_path), {});
}

struct CopyFileObjectStorageVFSOperation final : CopyFileObjectStorageOperation
{
    DiskObjectStorageVFS & disk;

    CopyFileObjectStorageVFSOperation(
        DiskObjectStorageVFS & disk_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const String & from_path_,
        const String & to_path_)
        : CopyFileObjectStorageOperation(
            *disk_.object_storage, *disk_.metadata_storage, *disk_.object_storage, read_settings_, write_settings_, from_path_, to_path_)
        , disk(disk_)
    {
    }

    void execute(MetadataTransactionPtr tx) override
    {
        CopyFileObjectStorageOperation::execute(tx);
        const String entry = VFSLogItem::getSerialised(std::move(created_objects), {});
        LOG_TRACE(disk.log, "{}: Executing {}", getInfoForLog(), entry);
        disk.zookeeper()->create(disk.traits.log_item, entry, zkutil::CreateMode::PersistentSequential);
    }
};

void DiskObjectStorageVFSTransaction::copyFile(
    const String & from_file_path, const String & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    operations_to_execute.emplace_back(
        std::make_unique<CopyFileObjectStorageVFSOperation>(disk, read_settings, write_settings, from_file_path, to_file_path));
}

// TODO myrrc A better approach would be to execute writes to Keeper as a single transaction when
// all other transactions are committed. So instead of adding a transaction operation on each filesystem
// operation, we could store Keeper actions in the VFSTransaction class directly. However, this brings two
// major questions:
//
// What happens if Keeper dies after we finished all FS operations? Will they be retried in order so that
//  correct info would be propagated to VFS log?
//  Currently on each Keeper failure we just abort the operation by throwing, achieving "early exit".
// On metadata copying from replica to replica, how will we handle that? Currently there's a hacky
//  approach: when a metadata file is written, it's immediately read back, deserialized, and StoredObject
//  info is propagated to Keeper. Then transaction is committed immediately. This solution was developed
//  as writeFile autocommits by default, so writing to actual file happens when buffer is finalized, not
//  when the corresponding transaction commits.
void DiskObjectStorageVFSTransaction::addStoredObjectsOp(StoredObjects && link, StoredObjects && unlink)
{
    if (link.empty() && unlink.empty()) [[unlikely]]
        return;

    LOG_TRACE(disk.log, "Pushing:\nlink:{}\nunlink:{}", fmt::join(link, "\n"), fmt::join(unlink, "\n"));
    String entry = VFSLogItem::getSerialised(std::move(link), std::move(unlink));

    auto callback = [entry_captured = std::move(entry), &disk_captured = disk]
    {
        LOG_TRACE(disk_captured.log, "Executing {}", entry_captured);
        disk_captured.zookeeper()->create(disk_captured.traits.log_item, entry_captured, zkutil::CreateMode::PersistentSequential);
    };

    operations_to_execute.emplace_back(
        std::make_unique<CallbackOperation<decltype(callback)>>(object_storage, metadata_storage, std::move(callback)));
}
}
