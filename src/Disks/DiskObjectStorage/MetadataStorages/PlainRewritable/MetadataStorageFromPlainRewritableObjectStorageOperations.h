#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoWithRetries.h>

#include <filesystem>
#include <memory>
#include <optional>

namespace DB
{

class MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation final : public IMetadataOperation
{
private:
    const std::shared_ptr<Preconditions> preconditions;
    const std::shared_ptr<FsSnapshot> fs_tree;

public:
    MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation(
        std::shared_ptr<Preconditions> preconditions_,
        std::shared_ptr<FsSnapshot> fs_tree_);

    void execute() override;
};

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    const bool recursive;
    const std::filesystem::path path;
    const std::string directory_remote_path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;

    /// Set after all validation and before the write, so `undo` runs exactly when `execute` may have changed object
    /// storage; see `blob_move_prepared` of the move operation.
    bool undo_prepared = false;

public:
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        bool recursive_,
        std::filesystem::path path_,
        std::string directory_remote_path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> from_tree_info;

    std::unique_ptr<WriteBufferFromFileBase> createWriteBuf(const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_content);
    void rewriteSingleDirectory(const std::filesystem::path & from, const std::filesystem::path & to, WriteBuffer & buffer);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;

    DirectoryRemoteInfo info;
    /// Set once `info` is captured and before the removal; see `blob_move_prepared` of the move operation.
    bool undo_prepared = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        std::filesystem::path path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const StoredObject object;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

public:
    /// Records a file in the filesystem and changes nothing in object storage, so it has nothing to reverse.
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        std::string path_,
        StoredObject object_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
};

class MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const bool if_exists;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    std::filesystem::path remote_source_path;
    std::filesystem::path remote_tmp_path;
    /// Set once both keys are known and before the first write; see `blob_move_prepared` of the move operation.
    bool blob_removal_prepared = false;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path path_,
        bool if_exists_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

/// Throws an exception if path_to_ already exists.
class MetadataStorageFromPlainObjectStorageCopyFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    /// Set once both keys are known and before the copy; see `blob_move_prepared` of the move operation.
    bool undo_prepared = false;

public:
    MetadataStorageFromPlainObjectStorageCopyFileOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

/**
 * @brief MetadataStorageFromPlainObjectStorageMoveFileOperation move file from {path_from, remote_path_from} to {path_to, remote_path_to}.
 *  If `replacable` is enabled, the target file will be replaced if exists. If disabled, the target file must not exist.
 *  Both source and target files must not be directories.
 */
class MetadataStorageFromPlainObjectStorageMoveFileOperation final : public IMetadataOperation
{
private:
    bool replaceable{false};
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    std::filesystem::path tmp_remote_path_from;
    std::filesystem::path tmp_remote_path_to;
    std::optional<FileRemoteInfo> file_from_remote_info;
    /// Set once the keys above are known and before the first write, so that `undo` knows `execute` may have changed
    /// object storage. It does not claim that any particular write landed; `undo` finds that out for itself.
    bool blob_move_prepared{false};
    bool had_existing_target{false};

public:
    MetadataStorageFromPlainObjectStorageMoveFileOperation(
        bool replaceable_,
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);
    /**
     * @brief Move a file from remote_path_from to remote_path_to
     *  1. Copy remote_path_to (if exists) to tmp_remote_path_from, which is used to restore the target file in case of failure.
     *  2. Copy remote_path_from to tmp_remote_path_to, which is used to restore the source file in case of failure.
     *  3. Copy remote_path_from to remote_path_to.
     *  4. Remove remote_path_to.
     *  5. Update fs_tree
     */
    void execute() override;
    /**
     * @brief Undo the `execute` logic:
     *  1. If remote_path_from is copied to remote_path_to, remove remote_path_to
     *  2. Restore remote_path_from from tmp_remote_path_from if it is copied.
     *  3. Restore remote_path_to from tmp_remote_path_to if it is copied.
     *  5. Update fs_tree
     */
    void undo() override;
    /**
     * @brief Finalize `execute` logic
     *  1. Remove tmp_remote_path_from if exists
     *  2. Remove tmp_remote_path_to if exists
     */
    void finalize() override;
};

class MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoWithRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    const LoggerPtr log;

    std::filesystem::path tmp_path;
    std::unique_ptr<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation> move_to_tmp_op;
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> subtree_remote_info;
    bool move_tried = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
        std::filesystem::path path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoWithRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

}
