#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/IDisk.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Common/Exception.h>
#include <Common/ObjectStorageKey.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>

#include <base/defines.h>

#include <memory>
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
    extern const int FILE_DOESNT_EXIST;
    extern const int TOO_DEEP_RECURSION;
}

namespace
{

std::optional<DiskObjectStorageMetadata> tryReadMetadataFile(const std::string & compatible_key_prefix, const std::string & path, const IDisk & disk)
{
    if (!disk.existsFile(path))
        return std::nullopt;

    std::string data;
    auto buf = disk.readFile(path, ReadSettings{});
    readStringUntilEOF(data, *buf);

    DiskObjectStorageMetadata object_metadata(compatible_key_prefix, path);
    const bool is_metadata_file = object_metadata.tryDeserializeFromString(data);
    if (!is_metadata_file)
        return std::nullopt;

    return object_metadata;
}

}

SetLastModifiedOperation::SetLastModifiedOperation(std::string path_, Poco::Timestamp new_timestamp_, IDisk & disk_)
    : path(std::move(path_))
    , new_timestamp(new_timestamp_)
    , disk(disk_)
{
}

void SetLastModifiedOperation::execute()
{
    old_timestamp = disk.getLastModified(path);
    disk.setLastModified(path, new_timestamp);
}

void SetLastModifiedOperation::undo()
{
    disk.setLastModified(path, old_timestamp);
}

ChmodOperation::ChmodOperation(std::string path_, mode_t mode_, IDisk & disk_)
    : path(std::move(path_))
    , mode(mode_)
    , disk(disk_)
{
}

void ChmodOperation::execute()
{
    old_mode = disk.stat(path).st_mode;
    disk.chmod(path, mode);
}

void ChmodOperation::undo()
{
    disk.chmod(path, old_mode);
}

WriteFileOperation::WriteFileOperation(std::string path_, std::string data_, IDisk & disk_)
    : path(std::move(path_))
    , data(std::move(data_))
    , disk(disk_)
{
}

void WriteFileOperation::execute()
{
    if (auto buf = disk.readFileIfExists(path, ReadSettings{}))
    {
        std::string file_data;
        readStringUntilEOF(file_data, *buf);
        prev_data = file_data;
    }

    auto buf = disk.writeFile(path);
    writeString(data, *buf);
    buf->finalize();
}

void WriteFileOperation::undo()
{
    if (!prev_data.has_value())
    {
        disk.removeFileIfExists(path);
    }
    else
    {
        auto buf = disk.writeFile(path);
        writeString(prev_data.value(), *buf);
        buf->finalize();
    }
}

UnlinkFileOperation::UnlinkFileOperation(std::string path_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(std::move(path_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
    , outcome(std::make_shared<UnlinkMetadataFileOperationOutcome>())
{
}

void UnlinkFileOperation::tryUnlinkMetadataFile()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk);
    if (!object_metadata.has_value())
        return;

    uint32_t ref_count = object_metadata->ref_count;
    if (ref_count > 0)
    {
        object_metadata->ref_count -= 1;
        write_operation = std::make_unique<WriteFileOperation>(path, object_metadata->serializeToString(), disk);
        write_operation->execute();
    }

    outcome->num_hardlinks = ref_count;
}

UnlinkMetadataFileOperationOutcomePtr UnlinkFileOperation::getOutcome()
{
    return outcome;
}

void UnlinkFileOperation::execute()
{
    if (!disk.existsFile(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't unlink file {}", path);

    /// Let's update hardlink count written in serialized DiskObjectStorageMetadata before the move
    tryUnlinkMetadataFile();

    /// We need to move file to the random name for the possible undo and to save the fs hardlink count
    auto tmp_path = getRandomASCIIString(32);
    disk.moveFile(path, tmp_path);
    tmp_file_path = tmp_path;
}

void UnlinkFileOperation::undo()
{
    if (tmp_file_path.has_value())
        disk.moveFile(tmp_file_path.value(), path);

    if (write_operation)
        write_operation->undo();
}

void UnlinkFileOperation::finalize()
{
    disk.removeFile(tmp_file_path.value());
}

CreateDirectoryOperation::CreateDirectoryOperation(std::string path_, IDisk & disk_)
    : path(std::move(path_))
    , disk(disk_)
{
}

void CreateDirectoryOperation::execute()
{
    disk.createDirectory(path);
}

void CreateDirectoryOperation::undo()
{
    disk.removeDirectory(path);
}

CreateDirectoryRecursiveOperation::CreateDirectoryRecursiveOperation(std::string path_, IDisk & disk_)
    : path(std::move(path_))
    , disk(disk_)
{
}

void CreateDirectoryRecursiveOperation::execute()
{
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

void CreateDirectoryRecursiveOperation::undo()
{
    for (const auto & path_created : paths_created)
        disk.removeDirectoryIfExists(path_created);
}

RemoveDirectoryOperation::RemoveDirectoryOperation(std::string path_, IDisk & disk_)
    : path(std::move(path_))
    , disk(disk_)
{
}

void RemoveDirectoryOperation::execute()
{
    disk.removeDirectory(path);
    removed = true;
}

void RemoveDirectoryOperation::undo()
{
    if (removed)
        disk.createDirectory(path);
}

RemoveRecursiveOperation::RemoveRecursiveOperation(std::string path_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(path_)
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void RemoveRecursiveOperation::traverseFile(const std::string & leaf)
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, leaf, disk);
    if (!object_metadata.has_value())
        return;

    uint32_t ref_count = object_metadata->ref_count;
    if (ref_count > 0)
    {
        object_metadata->ref_count -= 1;
        write_operations.push_back(std::make_unique<WriteFileOperation>(leaf, object_metadata->serializeToString(), disk));
        write_operations.back()->execute();
    }
}

void RemoveRecursiveOperation::traverseDirectory(const std::string & mid_path)
{
    for (auto it = disk.iterateDirectory(mid_path); it->isValid(); it->next())
    {
        const std::string next_to_visit = it->path();
        const int64_t path_inode = disk.stat(next_to_visit).st_ino;
        const bool is_new_path = visited_inodes.emplace(path_inode).second;
        if (!is_new_path)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Found cyclic symlink path {}", next_to_visit);

        if (disk.existsFile(next_to_visit))
            traverseFile(next_to_visit);
        else
            traverseDirectory(next_to_visit);
    }
}

void RemoveRecursiveOperation::execute()
{
    if (disk.existsFile(path))
    {
        traverseFile(path);

        auto path_to = getRandomASCIIString(32);
        disk.moveFile(path, path_to);
        temp_file_path = std::move(path_to);
    }
    else if (disk.existsDirectory(path))
    {
        traverseDirectory(path);

        auto path_to = getRandomASCIIString(32);
        disk.moveDirectory(path, path_to);
        temp_directory_path = std::move(path_to);
    }
}

void RemoveRecursiveOperation::undo()
{
    if (temp_file_path.has_value())
        disk.moveFile(temp_file_path.value(), path);
    else if (temp_directory_path.has_value())
        disk.moveDirectory(temp_directory_path.value(), path);

    for (auto & write_op : write_operations)
        write_op->undo();
}

void RemoveRecursiveOperation::finalize()
{
    if (temp_file_path.has_value())
        disk.removeFile(temp_file_path.value());
    else if (temp_directory_path.has_value())
        disk.removeRecursive(temp_directory_path.value());
}

CreateHardlinkOperation::CreateHardlinkOperation(std::string path_from_, std::string path_to_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void CreateHardlinkOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path_from, disk);
    if (!object_metadata.has_value())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't create hardlink for file {}", path_from);

    object_metadata->ref_count += 1;
    write_operation = std::make_unique<WriteFileOperation>(path_from, object_metadata->serializeToString(), disk);
    write_operation->execute();

    disk.createHardLink(path_from, path_to);
}

void CreateHardlinkOperation::undo()
{
    if (write_operation)
        write_operation->undo();

    disk.removeFileIfExists(path_to);
}

MoveFileOperation::MoveFileOperation(std::string path_from_, std::string path_to_, IDisk & disk_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , disk(disk_)
{
}

void MoveFileOperation::execute()
{
    disk.moveFile(path_from, path_to);
}

void MoveFileOperation::undo()
{
    disk.moveFile(path_to, path_from);
}

MoveDirectoryOperation::MoveDirectoryOperation(std::string path_from_, std::string path_to_, IDisk & disk_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , disk(disk_)
{
}

void MoveDirectoryOperation::execute()
{
    disk.moveDirectory(path_from, path_to);
}

void MoveDirectoryOperation::undo()
{
    disk.moveDirectory(path_to, path_from);
}

ReplaceFileOperation::ReplaceFileOperation(std::string path_from_, std::string path_to_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path_from(path_from_)
    , path_to(path_to_)
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void ReplaceFileOperation::execute()
{
    if (disk.existsFile(path_to))
    {
        unlink_operation = std::make_unique<UnlinkFileOperation>(path_to, compatible_key_prefix, disk);
        unlink_operation->execute();
    }

    disk.moveFile(path_from, path_to);
    moved = true;
}

void ReplaceFileOperation::undo()
{
    if (moved)
        disk.moveFile(path_to, path_from);

    if (unlink_operation)
        unlink_operation->undo();
}

void ReplaceFileOperation::finalize()
{
    if (unlink_operation)
        unlink_operation->finalize();
}

WriteInlineDataOperation::WriteInlineDataOperation(std::string path_, std::string inline_data_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(std::move(path_))
    , inline_data(std::move(inline_data_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void WriteInlineDataOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk).value_or(DiskObjectStorageMetadata(disk.getPath(), path));
    object_metadata.inline_data = inline_data;

    write_operation = std::make_unique<WriteFileOperation>(path, object_metadata.serializeToString(), disk);
    write_operation->execute();
}

void WriteInlineDataOperation::undo()
{
    if (write_operation)
        write_operation->undo();
}

RewriteFileOperation::RewriteFileOperation(std::string path_, StoredObjects objects_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(path_)
    , objects(std::move(objects_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void RewriteFileOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk).value_or(DiskObjectStorageMetadata(disk.getPath(), path));
    object_metadata.inline_data.clear();
    object_metadata.objects = objects;

    write_operation = std::make_unique<WriteFileOperation>(path, object_metadata.serializeToString(), disk);
    write_operation->execute();
}

void RewriteFileOperation::undo()
{
    if (write_operation)
        write_operation->undo();
}

AddBlobOperation::AddBlobOperation(std::string path_, StoredObject object_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(path_)
    , object(std::move(object_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void AddBlobOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk).value_or(DiskObjectStorageMetadata(disk.getPath(), path));
    object_metadata.objects.push_back(object);

    write_operation = std::make_unique<WriteFileOperation>(path, object_metadata.serializeToString(), disk);
    write_operation->execute();
}

void AddBlobOperation::undo()
{
    if (write_operation)
        write_operation->undo();
}

SetReadonlyFileOperation::SetReadonlyFileOperation(std::string path_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(std::move(path_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
{
}

void SetReadonlyFileOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk);
    if (!object_metadata.has_value())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't update readonly flag for file: {}", path);

    object_metadata->read_only = true;

    write_operation = std::make_unique<WriteFileOperation>(path, object_metadata->serializeToString(), disk);
    write_operation->execute();
}

void SetReadonlyFileOperation::undo()
{
    if (write_operation)
        write_operation->undo();
}

TruncateMetadataFileOperation::TruncateMetadataFileOperation(std::string path_, size_t target_size_, const std::string & compatible_key_prefix_, IDisk & disk_)
    : path(std::move(path_))
    , target_size(std::move(target_size_))
    , compatible_key_prefix(compatible_key_prefix_)
    , disk(disk_)
    , outcome(std::make_shared<TruncateFileOperationOutcome>())
{
}

TruncateFileOperationOutcomePtr TruncateMetadataFileOperation::getOutcome()
{
    return outcome;
}

void TruncateMetadataFileOperation::execute()
{
    auto object_metadata = tryReadMetadataFile(compatible_key_prefix, path, disk);
    if (!object_metadata.has_value())
    {
        if (target_size > 0)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist, can't truncate", path);
        else
            return;
    }

    size_t current_size = getTotalSize(object_metadata->objects);
    while (current_size > target_size)
    {
        StoredObject next_to_remove = std::move(object_metadata->objects.back());
        object_metadata->objects.pop_back();

        current_size -= next_to_remove.bytes_size;
        outcome->objects_to_remove.push_back(std::move(next_to_remove));
    }

    if (current_size != target_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} can't be truncated to size {}", path, target_size);

    write_operation = std::make_unique<WriteFileOperation>(path, object_metadata->serializeToString(), disk);
    write_operation->execute();
}

void TruncateMetadataFileOperation::undo()
{
    if (write_operation)
        write_operation->undo();
}

}
