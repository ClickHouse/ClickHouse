#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <filesystem>
#include <memory>

namespace DB
{

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;
    const bool recursive;

    std::string object_key_prefix;
    bool created_directory = false;

public:
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        /// path_ must end with a trailing '/'.
        std::filesystem::path && path_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_,
        bool recursive_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path path_to;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> from_tree_info;
    std::unordered_set<std::string> changed_paths;
    bool moved_in_memory = false;

    std::unique_ptr<WriteBufferFromFileBase> createWriteBuf(const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_content);
    void rewriteSingleDirectory(const std::filesystem::path & from, const std::filesystem::path & to, WriteBuffer & buffer);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        /// Both path_from_ and path_to_ must end with a trailing '/'.
        std::filesystem::path && path_from_,
        std::filesystem::path && path_to_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;

    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    DirectoryRemoteInfo info;
    bool remove_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        /// path_ must end with a trailing '/'.
        std::filesystem::path && path_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;

    bool written = false;

public:
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        const std::string & path, InMemoryDirectoryTree & fs_tree_, ObjectStoragePtr object_storage_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    std::filesystem::path remote_path;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;

    bool unlinked = false;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path && path_, InMemoryDirectoryTree & fs_tree_, ObjectStoragePtr object_storage_);

    void execute() override;
    void undo() override;
};

/// Throws an exception if path_to_ already exists.
class MetadataStorageFromPlainObjectStorageCopyFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path remote_path_from;
    std::filesystem::path path_to;
    std::filesystem::path remote_path_to;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;

    bool copy_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageCopyFileOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_);

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
    std::filesystem::path path_from;
    std::filesystem::path remote_path_from;
    std::filesystem::path path_to;
    std::filesystem::path remote_path_to;
    std::filesystem::path tmp_remote_path_from;
    std::filesystem::path tmp_remote_path_to;
    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;


    bool moved_existing_source_file{false};
    bool moved_existing_target_file{false};
    bool created_target_file{false};
    bool moved_file{false};

public:
    MetadataStorageFromPlainObjectStorageMoveFileOperation(
        bool replaceable_,
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_);
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
    std::filesystem::path path;

    InMemoryDirectoryTree & fs_tree;
    ObjectStoragePtr object_storage;
    ObjectMetadataCachePtr object_metadata_cache;
    const std::string metadata_key_prefix;
    const LoggerPtr log;

    std::filesystem::path tmp_path;
    std::unique_ptr<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation> move_to_tmp_op;
    bool move_tried = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
        /// path_ must end with a trailing '/'.
        std::filesystem::path && path_,
        InMemoryDirectoryTree & fs_tree_,
        ObjectStoragePtr object_storage_,
        ObjectMetadataCachePtr object_metadata_cache_,
        const std::string & metadata_key_prefix_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

}
