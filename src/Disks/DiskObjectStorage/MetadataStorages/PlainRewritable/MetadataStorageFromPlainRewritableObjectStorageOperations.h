#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableBlobRefcounts.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>

#include <filesystem>
#include <memory>

namespace DB
{

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    const bool recursive;
    const std::filesystem::path path;
    const std::string directory_remote_path;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    bool created_directory = false;

public:
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        bool recursive_,
        std::filesystem::path path_,
        std::string directory_remote_path_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> from_tree_info;
    std::unordered_set<std::string> changed_paths;
    bool moved_in_memory = false;

    std::unique_ptr<WriteBufferFromFileBase> createWriteBuf(const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_logical_path);
    void rewriteSingleDirectory(
        const std::filesystem::path & to, const DirectoryRemoteInfo & remote_info, WriteBuffer & buffer);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    DirectoryRemoteInfo info;
    bool remove_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        std::filesystem::path path_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const StoredObject object;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts;

    std::string relative_object_key;
    bool written = false;
    bool rewrote_prefix_path = false;

public:
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        std::string path_,
        StoredObject object_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const bool if_exists;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts;
    StoredObjects & removed_objects;

    std::filesystem::path remote_source_path;
    std::filesystem::path remote_tmp_path;
    std::string relative_object_key;
    std::optional<FileRemoteInfo> file_remote_info;
    bool used_explicit_unlink = false;
    bool metadata_updated = false;
    bool blob_remove_scheduled = false;
    bool copy_started = false;
    bool remove_started = false;
    bool remove_finished = false;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path path_,
        bool if_exists_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

/// Creates a hard link by sharing the source blob (dest directory becomes explicit).
class MetadataStorageFromPlainObjectStorageHardLinkOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts;

    std::string relative_object_key;
    bool link_created = false;

public:
    MetadataStorageFromPlainObjectStorageHardLinkOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_);

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
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts;
    StoredObjects & removed_objects;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    std::filesystem::path tmp_remote_path_from;
    std::filesystem::path tmp_remote_path_to;
    std::string relative_object_key_from;
    std::string relative_object_key_to;
    std::string relative_object_key_replaced;
    std::optional<FileRemoteInfo> file_from_remote_info;
    std::optional<FileRemoteInfo> file_to_remote_info;
    bool metadata_only_move{false};
    bool updated_blob_refcounts{false};
    bool decremented_replaced{false};
    bool moved_existing_source_file{false};
    bool moved_existing_target_file{false};
    bool created_target_file{false};
    bool moved_file{false};

public:
    MetadataStorageFromPlainObjectStorageMoveFileOperation(
        bool replaceable_,
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
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
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<InMemoryDirectoryTree> fs_tree;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    const std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts;
    StoredObjects & removed_objects;

    const LoggerPtr log;

    std::filesystem::path tmp_path;
    std::unique_ptr<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation> move_to_tmp_op;
    bool move_tried = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
        std::filesystem::path path_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

}
