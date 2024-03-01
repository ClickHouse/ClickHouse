#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <filesystem>
#include <map>

namespace DB
{

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;
    MetadataStorageFromPlainObjectStorage::PathMap & path_map;
    ObjectStoragePtr object_storage;

    bool write_created = false;

public:
    // Assuming that paths are normalized.
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        std::filesystem::path && path_, MetadataStorageFromPlainObjectStorage::PathMap & path_map_, ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path_from;
    std::filesystem::path path_to;
    MetadataStorageFromPlainObjectStorage::PathMap & path_map;
    ObjectStoragePtr object_storage;

    bool write_created = false;
    bool write_finalized = false;

    std::unique_ptr<WriteBufferFromFileBase>
    write(const std::filesystem::path & from, const std::filesystem::path & to, bool validate_content);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path && path_from_,
        std::filesystem::path && path_to_,
        MetadataStorageFromPlainObjectStorage::PathMap & path_map_,
        ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo() override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::filesystem::path path;

    MetadataStorageFromPlainObjectStorage::PathMap & path_map;
    ObjectStoragePtr object_storage;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        std::filesystem::path && path_, MetadataStorageFromPlainObjectStorage::PathMap & path_map_, ObjectStoragePtr object_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;
    void undo() override;
};

}
