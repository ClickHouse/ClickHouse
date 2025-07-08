#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <filesystem>
#include <map>


namespace DB
{

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;
    const std::string object_key_prefix;

public:
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        /// path_ must end with a trailing '/'.
        std::filesystem::path && path_,
        InMemoryDirectoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};


class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path path_to;
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    bool write_finalized = false;

    std::unique_ptr<WriteBufferFromFileBase>
    createWriteBuf(const std::filesystem::path & expected_path, const std::filesystem::path & new_path, bool validate_content);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        /// Both path_from_ and path_to_ must end with a trailing '/'.
        std::filesystem::path && path_from_,
        std::filesystem::path && path_to_,
        InMemoryDirectoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};


class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;

    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;
    const std::string metadata_key_prefix;

    InMemoryDirectoryPathMap::RemotePathInfo info;
    bool remove_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        /// path_ must end with a trailing '/'.
        std::filesystem::path && path_,
        InMemoryDirectoryPathMap & path_map_,
        ObjectStoragePtr object_storage_,
        const std::string & metadata_key_prefix_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};


class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;

    bool written = false;

public:
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        const std::string & path, InMemoryDirectoryPathMap & path_map_, ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};


class MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    std::filesystem::path remote_path;
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;

    bool unlinked = false;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path && path_, InMemoryDirectoryPathMap & path_map_, ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
};

/// Throws an exception if path_to_ already exists.
class MetadataStorageFromPlainObjectStorageCopyFileOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path remote_path_from;
    std::filesystem::path path_to;
    std::filesystem::path remote_path_to;
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;

    bool copied = false;

public:
    MetadataStorageFromPlainObjectStorageCopyFileOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        InMemoryDirectoryPathMap & path_map_,
        ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
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
    InMemoryDirectoryPathMap & path_map;
    ObjectStoragePtr object_storage;


    bool moved_existing_source_file{false};
    bool moved_existing_target_file{false};
    bool moved_file{false};

public:
    MetadataStorageFromPlainObjectStorageMoveFileOperation(
        bool replaceable_,
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        InMemoryDirectoryPathMap & path_map_,
        ObjectStoragePtr object_storage_);
    /**
     * @brief Move a file from remote_path_from to remote_path_to
     *  1. Copy remote_path_to (if exists) to tmp_remote_path_from, which is used to restore the target file in case of failure.
     *  2. Copy remote_path_from to tmp_remote_path_to, which is used to restore the source file in case of failure.
     *  3. Copy remote_path_from to remote_path_to.
     *  4. Remove remote_path_to.
     *  5. Update path_map
     * @param metadata_lock the lock of metadata.
     */
    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    /**
     * @brief Undo the `execute` logic:
     *  1. If remote_path_from is copied to remote_path_to, remove remote_path_to
     *  2. Restore remote_path_from from tmp_remote_path_from if it is copied.
     *  3. Restore remote_path_to from tmp_remote_path_to if it is copied.
     *  5. Update path_map
     * @param metadata_lock the lock of metadata.
     */
    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;
    /**
     * @brief Finalize `execute` logic
     *  1. Remove tmp_remote_path_from if exists
     *  2. Remove tmp_remote_path_to if exists
     */
    void finalize() override;
};
}
