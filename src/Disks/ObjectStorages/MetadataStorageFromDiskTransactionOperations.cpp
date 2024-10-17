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

CreateDirectoryRecursiveOperation::CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
{
}

void CreateDirectoryRecursiveOperation::execute(std::unique_lock<SharedMutex> &)
{
    namespace fs = std::filesystem;
    fs::path p(path);
    while (!disk.existsFileOrDirectory(p))
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

RemoveRecursiveOperation::RemoveRecursiveOperation(const std::string & path_, IDisk & disk_)
    : path(path_)
    , disk(disk_)
    , temp_path(getTempFileName(fs::path(path).parent_path()))
{
}

void RemoveRecursiveOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (disk.existsFile(path))
        disk.moveFile(path, temp_path);
    else if (disk.existsDirectory(path))
        disk.moveDirectory(path, temp_path);
}

void RemoveRecursiveOperation::undo(std::unique_lock<SharedMutex> &)
{
    if (disk.existsFile(temp_path))
        disk.moveFile(temp_path, path);
    else if (disk.existsDirectory(temp_path))
        disk.moveDirectory(temp_path, path);
}

void RemoveRecursiveOperation::finalize()
{
    if (disk.existsFileOrDirectory(temp_path))
        disk.removeRecursive(temp_path);

    if (disk.existsFileOrDirectory(path))
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
    if (disk.existsFile(path_to))
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

WriteFileOperation::WriteFileOperation(const std::string & path_, IDisk & disk_, const std::string & data_)
    : path(path_)
    , disk(disk_)
    , data(data_)
{
}

void WriteFileOperation::execute(std::unique_lock<SharedMutex> &)
{
    if (auto buf = disk.readFileIfExists(path, ReadSettings{}))
    {
        existed = true;
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

void AddBlobOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    DiskObjectStorageMetadataPtr metadata;
    if (metadata_storage.existsFile(path))
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

void TruncateMetadataFileOperation::execute(std::unique_lock<SharedMutex> & metadata_lock)
{
    if (metadata_storage.existsFile(path))
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
