#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

class MetadataStorageFromDisk;
class MetadataStorageFromDiskTransaction;
class IDisk;

/// Per inode, whether the operations of one transaction that remove its hard links agree that its
/// blobs may be released. False once any of those links is retention-listed. Shared by the unlink
/// operations of a transaction, because each of them only ever sees its own link.
using InodeReleaseVeto = std::unordered_map<int64_t, bool>;

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

    mode_t old_mode{};
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

    // True once execute() has confirmed the file exists on disk.
    // Used by undo() to distinguish "file was created by this operation"
    // (safe to delete on undo) from "file already existed, execute() failed
    // before overwriting it" (must NOT delete — the file is unchanged).
    bool file_existed = false;
};

struct UnlinkFileOperation final : public IMetadataOperation
{
    UnlinkFileOperation(std::string path_, bool if_exists_, bool should_remove_objects_, const std::string & compatible_key_prefix_, IDisk & disk_, StoredObjects & objects_to_remove_, InodeReleaseVeto & inode_release_allowed_);

    void tryUnlinkMetadataFile();

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const bool if_exists;
    const bool should_remove_objects;
    const std::string & compatible_key_prefix;
    IDisk & disk;
    StoredObjects & objects_to_remove;
    InodeReleaseVeto & inode_release_allowed;

    std::optional<std::string> tmp_file_path;
    std::optional<int64_t> inode;
    std::unique_ptr<WriteFileOperation> write_operation;
    /// Candidates only. Released in finalize() iff this unlink drops the last hard link.
    StoredObjects removed_objects;
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
    RemoveRecursiveOperation(std::string path_, IMetadataTransaction::ShouldRemoveObjectsPredicate should_remove_objects_, const std::string & compatible_key_prefix_, IDisk & disk_, StoredObjects & objects_to_remove_);

    void traverseFile(const std::string & leaf);
    void traverseDirectory(const std::string & mid_path);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const IMetadataTransaction::ShouldRemoveObjectsPredicate should_remove_objects;
    const std::string & compatible_key_prefix;
    IDisk & disk;
    StoredObjects & objects_to_remove;

    /// One metadata file being removed. Entries share an inode when the subtree holds
    /// hard links to the same file.
    struct RemovalCandidate
    {
        std::string relative_path;
        int64_t inode;
        bool should_remove_its_objects;
        StoredObjects objects;
    };

    std::optional<std::string> temp_file_path;
    std::optional<std::string> temp_directory_path;
    std::unordered_set<int64_t> visited_inodes;
    std::vector<std::unique_ptr<WriteFileOperation>> write_operations;
    std::vector<RemovalCandidate> removal_candidates;
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
    ReplaceFileOperation(std::string path_from_, std::string path_to_, const std::string & compatible_key_prefix, IDisk & disk_, StoredObjects & objects_to_remove_, InodeReleaseVeto & inode_release_allowed_);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path_from;
    const std::string path_to;
    const std::string & compatible_key_prefix;
    IDisk & disk;
    StoredObjects & objects_to_remove;
    InodeReleaseVeto & inode_release_allowed;

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
    RewriteFileOperation(std::string path_, StoredObjects objects_, const std::string & compatible_key_prefix_, IDisk & disk_, StoredObjects & objects_to_remove_);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const StoredObjects objects;
    const std::string & compatible_key_prefix;
    IDisk & disk;
    StoredObjects & objects_to_remove;

    std::unique_ptr<WriteFileOperation> write_operation;
    StoredObjects removed_objects;
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
    TruncateMetadataFileOperation(std::string path_, size_t target_size_, const std::string & compatible_key_prefix, IDisk & disk_, StoredObjects & objects_to_remove_);

    void execute() override;
    void undo() override;
    void finalize() override;

private:
    const std::string path;
    const size_t target_size;
    const std::string & compatible_key_prefix;
    IDisk & disk;
    StoredObjects & objects_to_remove;

    std::unique_ptr<WriteFileOperation> write_operation;
    StoredObjects removed_objects;
};

}
