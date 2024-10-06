#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/IDisk.h>
#include <Common/getRandomASCIIString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <optional>
#include <ranges>
#include <filesystem>
#include <utility>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::string getTempFileName(const std::string & dir)
{
    return fs::path(dir) / getRandomASCIIString(32);
}

/* OK */
SetLastModifiedOperation::SetLastModifiedOperation(const std::string & path_, Poco::Timestamp new_timestamp_, IDisk & disk_)
    : path(path_)
    , new_timestamp(new_timestamp_)
    , disk(disk_)
{
}

void SetLastModifiedOperation::execute(std::unique_lock<SharedMutex> &)
{
    old_timestamp = disk.getLastModified(path);
    disk.setLastModified(path, new_timestamp);
}

void SetLastModifiedOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.setLastModified(path, old_timestamp);
}

/* OK */
ChmodOperation::ChmodOperation(const std::string & path_, mode_t mode_, IDisk & disk_)
    : path(path_)
    , mode(mode_)
    , disk(disk_)
{
}

void ChmodOperation::execute(std::unique_lock<SharedMutex> &)
{
    old_mode = disk.stat(path).st_mode;
    disk.chmod(path, mode);
}

void ChmodOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.chmod(path, old_mode);
}


/* OK */
UnlinkFileOperation::UnlinkFileOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
{
}

void UnlinkFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    auto buf = disk.readFile(path, ReadSettings{}, std::nullopt, disk.getFileSize(path));
    readStringUntilEOF(prev_data, *buf);
    disk.removeFile(path);
}

void UnlinkFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    auto buf = disk.writeFile(path);
    writeString(prev_data, *buf);
    buf->finalize();
}


/* OK */
CreateDirectoryOperation::CreateDirectoryOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
{
}

void CreateDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    disk.createDirectory(path);
}

void CreateDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.removeDirectory(path);
}


/* OK */
CreateDirectoryRecursiveOperation::CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
{
}

void CreateDirectoryRecursiveOperation::execute(std::unique_lock<SharedMutex> &)
{
    namespace fs = std::filesystem;
    fs::path p(path);
    while (!disk.exists(p))
    {
        paths_created.push_back(p);
        if (!p.has_parent_path())
            break;
        p = p.parent_path();
    }
    for (const auto & path_to_create : paths_created | std::views::reverse)
        disk.createDirectory(path_to_create);
}

void CreateDirectoryRecursiveOperation::undo(std::unique_lock<SharedMutex> &)
{
    for (const auto & path_created : paths_created)
        disk.removeDirectory(path_created);
}


/* OK */
RemoveDirectoryOperation::RemoveDirectoryOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
{
}

void RemoveDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    disk.removeDirectory(path);
}

void RemoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.createDirectory(path);
}

/* OK */
// TODO: RemoveRecursive is not supported for VFS

RemoveRecursiveOperation::RemoveRecursiveOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
    , temp_path(getTempFileName(fs::path(path).parent_path()))
{
}

void RemoveRecursiveOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (disk.isFile(path))
        disk.moveFile(path, temp_path);
    else if (disk.isDirectory(path))
        disk.moveDirectory(path, temp_path);
}

void RemoveRecursiveOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (disk.isFile(temp_path))
        disk.moveFile(temp_path, path);
    else if (disk.isDirectory(temp_path))
        disk.moveDirectory(temp_path, path);
}

void RemoveRecursiveOperation::finalize()
{
    if (disk.exists(temp_path))
        disk.removeRecursive(temp_path);

    if (disk.exists(path))
        disk.removeRecursive(path);
}


CreateHardlinkOperation::CreateHardlinkOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_, const MetadataStorageFromDisk & metadata_storage_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
    , metadata_storage(metadata_storage_)
{
}

void CreateHardlinkOperation::execute(std::unique_lock<SharedMutex> & lock)
{
    auto metadata = metadata_storage.readMetadataUnlocked(path_from, lock);

    metadata->incrementRefCount();

    write_operation = std::make_unique<WriteFileOperation>(path_from, disk, metadata->serializeToString());

    write_operation->execute(lock);

    disk.createHardLink(path_from, path_to);
}

void CreateHardlinkOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (write_operation)
        write_operation->undo(lock);
    disk.removeFile(path_to);
}


/* OK */
VFSCreateHardlinkOperation::VFSCreateHardlinkOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_, const MetadataStorageFromDisk & metadata_storage_, VFSLog & vfs_log_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
    , metadata_storage(metadata_storage_)
    , vfs_log(vfs_log_)
{
}

void VFSCreateHardlinkOperation::execute(std::unique_lock<SharedMutex> & lock)
{
    metadata = metadata_storage.readMetadataUnlocked(path_from, lock);

    vfs_log.link(*metadata);

    create_hardlink_operation = std::make_unique<CreateHardlinkOperation>(path_from, path_to, disk, metadata_storage);
    create_hardlink_operation->execute(lock);
}

void VFSCreateHardlinkOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    create_hardlink_operation->undo(lock);
    vfs_log.unlink(*metadata);
}


/* OK */
MoveFileOperation::MoveFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
{
}

void MoveFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    disk.moveFile(path_from, path_to);
}

void MoveFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.moveFile(path_to, path_from);
}

/* OK */
MoveDirectoryOperation::MoveDirectoryOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
{
}

void MoveDirectoryOperation::execute(std::unique_lock<SharedMutex> &)
{
    disk.moveDirectory(path_from, path_to);
}

void MoveDirectoryOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.moveDirectory(path_to, path_from);
}

ReplaceFileOperation::ReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
    , temp_path_to(getTempFileName(fs::path(path_to).parent_path()))
{
}

void ReplaceFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (disk.exists(path_to))
        disk.moveFile(path_to, temp_path_to);

    disk.replaceFile(path_from, path_to);
}

void ReplaceFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    disk.moveFile(path_to, path_from);
    disk.moveFile(temp_path_to, path_to);
}

void ReplaceFileOperation::finalize()
{
    disk.removeFileIfExists(temp_path_to);
}

/* OK */
VFSReplaceFileOperation::VFSReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_, const MetadataStorageFromDisk & metadata_storage_, VFSLog & vfs_log_)
    : path_from(path_from_)
    , path_to(path_to_)
    , disk(disk_)
    , metadata_storage(metadata_storage_)
    , vfs_log(vfs_log_)
{
}

void VFSReplaceFileOperation::execute(std::unique_lock<SharedMutex> & lock)
{
    prev_metadata = metadata_storage.readMetadataUnlocked(path_from, lock);
    replace_file_operation = std::make_unique<ReplaceFileOperation>(path_from, path_to, disk);
    replace_file_operation->execute(lock);
}

void VFSReplaceFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    replace_file_operation->undo(lock);    
}

void VFSReplaceFileOperation::finalize()
{
    replace_file_operation->finalize();
    vfs_log.unlink(*prev_metadata);
}

WriteFileOperation::WriteFileOperation(const std::string & path_, IDisk & disk_, const std::string & data_)
    : path(path_)
    , disk(disk_)
    , data(data_)
{
}

void WriteFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (disk.exists(path))
    {
        existed = true;
        auto buf = disk.readFile(path);
        readStringUntilEOF(prev_data, *buf);
    }
    auto buf = disk.writeFile(path);
    writeString(data, *buf);
    buf->finalize();
}

void WriteFileOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (!existed)
    {
        disk.removeFileIfExists(path);
    }
    else
    {
        auto buf = disk.writeFile(path);
        writeString(prev_data, *buf);
    }
}

VFSWriteMetadataFileOperation::VFSWriteMetadataFileOperation(const std::string & path_, IDisk & disk_, const MetadataStorageFromDisk & metadata_storage_, const DiskObjectStorageMetadata & metadata_, VFSLog & vfs_log_)
    : path(path_)
    , disk(disk_)
    , metadata_storage(metadata_storage_)
    , vfs_log(vfs_log_)
    , metadata(metadata_)
    
{
}

void VFSWriteMetadataFileOperation::execute(std::unique_lock<SharedMutex> & lock)
{
    vfs_log.link(metadata);

    if (disk.exists(path))
    {
        existed = true;
        prev_metadata = metadata_storage.readMetadataUnlocked(path, lock);
    }
    write_operation = std::make_unique<WriteFileOperation>(path, disk, metadata.serializeToString());
    write_operation->execute(lock);
}

void VFSWriteMetadataFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (write_operation)
        write_operation->undo(lock);
    vfs_log.unlink(metadata);

}

void VFSWriteMetadataFileOperation::finalize()
{
    if (prev_metadata)
        vfs_log.unlink(*prev_metadata);
}

void AddBlobOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    DiskObjectStorageMetadataPtr metadata;
    if (metadata_storage.exists(path))
        metadata = metadata_storage.readMetadataUnlocked(path, metadata_lock);
    else
        metadata = std::make_unique<DiskObjectStorageMetadata>(disk.getPath(), path);

    metadata->addObject(object_key, size_in_bytes);

    write_operation = std::make_unique<WriteFileOperation>(path, disk, metadata->serializeToString());

    write_operation->execute(metadata_lock);
}

void AddBlobOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (write_operation)
        write_operation->undo(lock);
}


/* OK */
void VFSAddBlobOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    vfs_log.link(object_key.serialize(), path);
    add_blob_operation = std::make_unique<AddBlobOperation>(path, object_key, size_in_bytes, disk, metadata_storage);
    add_blob_operation->execute(metadata_lock);
}

void VFSAddBlobOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (add_blob_operation)
        add_blob_operation->undo(lock);
    
    vfs_log.unlink(object_key.serialize(), path);
}

void UnlinkMetadataFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    auto metadata = metadata_storage.readMetadataUnlocked(path, metadata_lock);
    uint32_t ref_count = metadata->getRefCount();
    if (ref_count != 0)
    {
        metadata->decrementRefCount();
        write_operation = std::make_unique<WriteFileOperation>(path, disk, metadata->serializeToString());
        write_operation->execute(metadata_lock);
    }
    outcome->num_hardlinks = ref_count;

    unlink_operation = std::make_unique<UnlinkFileOperation>(path, disk);
    unlink_operation->execute(metadata_lock);
}

void UnlinkMetadataFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    /// Operations MUST be reverted in the reversed order, so
    /// when we apply operation #1 (write) and operation #2 (unlink)
    /// we should revert #2 and only after it #1. Otherwise #1 will overwrite
    /// file with incorrect data.
    if (unlink_operation)
        unlink_operation->undo(lock);

    if (write_operation)
        write_operation->undo(lock);

    /// Update outcome to reflect the fact that we have restored the file.
    outcome->num_hardlinks++;
}


/* OK */
void VFSUnlinkMetadataFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    prev_metadata = metadata_storage.readMetadataUnlocked(path, metadata_lock);    
    unlink_operation = std::make_unique<UnlinkMetadataFileOperation>(path, disk, metadata_storage);
    unlink_operation->execute(metadata_lock);
    *outcome = *unlink_operation->outcome;
}

void VFSUnlinkMetadataFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    unlink_operation->undo(lock);
    *outcome = *unlink_operation->outcome;
}

void VFSUnlinkMetadataFileOperation::finalize()
{
    vfs_log.unlink(*prev_metadata);
}



/* OK */
void TruncateMetadataFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    if (metadata_storage.exists(path))
    {
        auto metadata = metadata_storage.readMetadataUnlocked(path, metadata_lock);
        while (metadata->getTotalSizeBytes() > target_size)
        {
            auto object_key_with_metadata = metadata->popLastObject();
            outcome->objects_to_remove.emplace_back(object_key_with_metadata.key.serialize(), path, object_key_with_metadata.metadata.size_bytes);
        }
        if (metadata->getTotalSizeBytes() != target_size)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} can't be truncated to size {}", path, target_size);
        }
        LOG_TEST(getLogger("TruncateMetadataFileOperation"), "Going to remove {} blobs.", outcome->objects_to_remove.size());

        write_operation = std::make_unique<WriteFileOperation>(path, disk, metadata->serializeToString());

        write_operation->execute(metadata_lock);
    }
}

void TruncateMetadataFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (write_operation)
        write_operation->undo(lock);
}


void VFSTruncateMetadataFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    truncate_operation->execute(metadata_lock);
}

void VFSTruncateMetadataFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    truncate_operation->undo(lock);
}

void VFSTruncateMetadataFileOperation::finalize()
{
    for (const auto & obj: outcome->objects_to_remove)
        vfs_log.unlink(obj.remote_path, path);
}

/* OK */
void SetReadonlyFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    auto metadata = metadata_storage.readMetadataUnlocked(path, metadata_lock);
    metadata->setReadOnly();
    write_operation = std::make_unique<WriteFileOperation>(path, disk, metadata->serializeToString());
    write_operation->execute(metadata_lock);
}

void SetReadonlyFileOperation::undo(std::unique_lock<SharedMutex> & lock)
{
    if (write_operation)
        write_operation->undo(lock);
}

}
