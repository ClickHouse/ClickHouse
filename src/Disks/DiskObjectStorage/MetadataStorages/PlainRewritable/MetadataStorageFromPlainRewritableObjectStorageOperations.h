#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoRetries.h>

#include <filesystem>
#include <memory>
#include <optional>

namespace DB
{

/// Whether a file move must be done by changing the file lists of the directories instead of copying the blob.
/// Used both when the transaction is planned and when it is committed.
bool isMetadataOnlyMove(
    const FsSnapshot & fs_tree,
    const DirectoryRemoteInfo & directory_from,
    const DirectoryRemoteInfo & directory_to,
    const std::string & blob_key_from,
    const std::optional<std::string> & blob_key_of_existing_target);

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
    const UndoRetriesPtr undo_retries;

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
        UndoRetriesPtr undo_retries_);

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
    const UndoRetriesPtr undo_retries;

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> from_tree_info;

    std::unique_ptr<WriteBufferFromFileBase> createWriteBuf(const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_logical_path);
    void rewriteSingleDirectory(const std::filesystem::path & from, const std::filesystem::path & to, const DirectoryRemoteInfo & remote_info, WriteBuffer & buffer);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_);

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
    const UndoRetriesPtr undo_retries;

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
        UndoRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

/// Records a file whose blob has already been written.
/// The blob key is relative to the common key prefix; an empty key means the default location, `<directory remote path>/<file name>`.
/// A blob at the default location only fits a directory with the implicit file list; a directory with the explicit file list
/// gets its `prefix.path` rewritten with the new file, and a blob outside of the default location switches the directory to that form.
class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const StoredObject object;
    const std::string blob_key;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    std::optional<DirectoryRemoteInfo> previous_directory_info;
    bool prefix_path_written = false;
    std::optional<StoredObject> replaced_blob;

public:
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        std::string path_,
        StoredObject object_,
        std::string blob_key_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
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
    const UndoRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    /// A file of a directory with the implicit file list, whose blob is not shared, is removed together with its blob:
    /// otherwise the blob would be discovered as a file again when the metadata is loaded. To be able to undo the removal,
    /// the blob is first copied to a temporary location.
    std::filesystem::path remote_source_path;
    std::filesystem::path remote_tmp_path;
    /// Set once both keys are known and before the first write; see `blob_move_prepared` of the move operation.
    bool blob_removal_prepared = false;

    /// Otherwise the file is removed from the explicit file list of the directory (switching the directory to that form
    /// if needed), and the blob is removed after the commit if this was its last link.
    std::optional<DirectoryRemoteInfo> previous_directory_info;
    bool prefix_path_written = false;
    std::optional<StoredObject> blob_to_remove;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path path_,
        bool if_exists_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

/// Duplicates the blob of the source file. Used when hard links are disabled for the disk, so that the metadata
/// stays in the form that older servers can read (see `MetadataStorageFromPlainRewritableObjectStorage`).
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
    const UndoRetriesPtr undo_retries;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    bool copy_attempted = false;
    std::optional<DirectoryRemoteInfo> previous_directory_info;
    bool prefix_path_written = false;

public:
    MetadataStorageFromPlainObjectStorageCopyFileOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

/// Creates a hard link: a file in the target directory that shares the blob of the source file.
/// The target directory is switched to the explicit file list form, and the number of links to the blob is incremented.
/// Throws an exception if path_to_ already exists.
class MetadataStorageFromPlainObjectStorageHardLinkOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const UndoRetriesPtr undo_retries;

    std::optional<DirectoryRemoteInfo> previous_directory_info;
    bool prefix_path_written = false;

public:
    MetadataStorageFromPlainObjectStorageHardLinkOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_);

    void execute() override;
    void undo() override;
};

/**
 * @brief MetadataStorageFromPlainObjectStorageMoveFileOperation move file from {path_from, remote_path_from} to {path_to, remote_path_to}.
 *  If `replacable` is enabled, the target file will be replaced if exists. If disabled, the target file must not exist.
 *  Both source and target files must not be directories.
 *
 *  When both directories have the implicit file list and the blobs involved are not shared, the blob is moved by copying.
 *  Otherwise the move only changes the file lists of the two directories (switching them to the explicit form if needed),
 *  and the blob stays where it is.
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
    const UndoRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    bool metadata_only_move{false};
    std::optional<DirectoryRemoteInfo> previous_directory_info_from;
    std::optional<DirectoryRemoteInfo> previous_directory_info_to;
    bool prefix_path_written_from{false};
    bool prefix_path_written_to{false};
    std::optional<StoredObject> replaced_blob;

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
        UndoRetriesPtr undo_retries_,
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
    const UndoRetriesPtr undo_retries;
    StoredObjects & removed_objects;

    const LoggerPtr log;

    std::filesystem::path tmp_path;
    std::unique_ptr<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation> move_to_tmp_op;
    /// The metadata objects of the removed directories and the blobs whose last links were inside the removed subtree.
    StoredObjects objects_to_remove;
    bool move_tried = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
        std::filesystem::path path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        UndoRetriesPtr undo_retries_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

}
