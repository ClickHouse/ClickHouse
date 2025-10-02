#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/StoredObject.h>

#include <unordered_set>

namespace DB
{

class MetadataStorageFromDisk;
class MetadataStorageFromDiskTransaction;
class IDisk;

/**
 * Implementations for transactional operations with metadata used by MetadataStorageFromDisk.
 */

struct SetLastModifiedOperation final : public IMetadataOperation
{
    SetLastModifiedOperation(std::string path_, Poco::Timestamp new_timestamp_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const Poco::Timestamp new_timestamp;
    IDisk & disk;

    Poco::Timestamp old_timestamp;
};

struct ChmodOperation final : public IMetadataOperation
{
    ChmodOperation(std::string path_, mode_t mode_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const mode_t mode;
    IDisk & disk;

    mode_t old_mode;
};

struct WriteFileOperation final : public IMetadataOperation
{
    WriteFileOperation(std::string path_, std::string data_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const std::string data;
    IDisk & disk;

    std::optional<std::string> prev_data;
};

struct UnlinkFileOperation final : public IMetadataOperation
{
    UnlinkFileOperation(std::string path_, const std::string & compatible_key_prefix, IDisk & disk_);

    void tryUnlinkMetadataFile();
    UnlinkMetadataFileOperationOutcomePtr getOutcome();

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    UnlinkMetadataFileOperationOutcomePtr outcome;

    std::optional<std::string> tmp_file_path;
    std::unique_ptr<WriteFileOperation> write_operation;
};

struct CreateDirectoryOperation final : public IMetadataOperation
{
    CreateDirectoryOperation(std::string path_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    IDisk & disk;
};

struct CreateDirectoryRecursiveOperation final : public IMetadataOperation
{
    CreateDirectoryRecursiveOperation(std::string path_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    IDisk & disk;

    std::vector<std::string> paths_created;
};

struct RemoveDirectoryOperation final : public IMetadataOperation
{
    RemoveDirectoryOperation(std::string path_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    IDisk & disk;

    bool removed = false;
};

struct RemoveRecursiveOperation final : public IMetadataOperation
{
    RemoveRecursiveOperation(std::string path_, const std::string & compatible_key_prefix, IDisk & disk_);

    void traverseFile(const std::string & leaf);
    void traverseDirectory(const std::string & mid_path);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::optional<std::string> temp_file_path;
    std::optional<std::string> temp_directory_path;
    std::unordered_set<int64_t> visited_inodes;
    std::vector<std::unique_ptr<WriteFileOperation>> write_operations;
};

struct CreateHardlinkOperation final : public IMetadataOperation
{
    CreateHardlinkOperation(std::string path_from_, std::string path_to_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path_from;
    const std::string path_to;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct MoveFileOperation final : public IMetadataOperation
{
    MoveFileOperation(std::string path_from_, std::string path_to_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path_from;
    const std::string path_to;
    IDisk & disk;
};

struct MoveDirectoryOperation final : public IMetadataOperation
{
    MoveDirectoryOperation(std::string path_from_, std::string path_to_, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path_from;
    const std::string path_to;
    IDisk & disk;
};

struct ReplaceFileOperation final : public IMetadataOperation
{
    ReplaceFileOperation(std::string path_from_, std::string path_to_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path_from;
    const std::string path_to;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<UnlinkFileOperation> unlink_operation;
    bool moved = false;
};

struct WriteInlineDataOperation final : public IMetadataOperation
{
    WriteInlineDataOperation(std::string path_, std::string inline_data_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const String inline_data;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct RewriteFileOperation final : public IMetadataOperation
{
    RewriteFileOperation(std::string path_, StoredObjects objects_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const StoredObjects objects;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct AddBlobOperation final : public IMetadataOperation
{
    AddBlobOperation(std::string path_, StoredObject object_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const StoredObject object;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct SetReadonlyFileOperation final : public IMetadataOperation
{
    SetReadonlyFileOperation(std::string path_, const std::string & compatible_key_prefix, IDisk & disk_);

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct TruncateMetadataFileOperation final : public IMetadataOperation
{
    TruncateMetadataFileOperation(std::string path_, size_t target_size_, const std::string & compatible_key_prefix, IDisk & disk_);
    ~TruncateMetadataFileOperation() override = default;

    TruncateFileOperationOutcomePtr getOutcome();

    void execute() override;
    void undo() override;

private:
    const std::string path;
    const size_t target_size;
    const std::string & compatible_key_prefix;
    IDisk & disk;

    TruncateFileOperationOutcomePtr outcome;

    std::unique_ptr<WriteFileOperation> write_operation;
};

}
