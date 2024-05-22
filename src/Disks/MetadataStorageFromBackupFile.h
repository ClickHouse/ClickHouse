#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/IDisk.h>


namespace DB
{

class MetadataStorageFromBackupFile final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromBackupFileTransaction;

    struct MetadataStorageFromBackupFilePseudoFileSystemNode
    {
    public:
        String name;
        String path;
        uint64_t file_size = 0;
        bool is_file = false;
        bool is_directory = false;
        std::vector<String> children = {};
    };
    std::unordered_map<String /* path */, MetadataStorageFromBackupFilePseudoFileSystemNode> nodes;

public:
    explicit MetadataStorageFromBackupFile(const String & path_to_backup_file);

    MetadataTransactionPtr createTransaction() override;

    const String & getPath() const override;

    MetadataStorageType getType() const override;

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    uint64_t getFileSize(const String & path) const override;

    std::vector<String> listDirectory(const String & path) const override;

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    StoredObjects getStorageObjects(const String & path) const override;

    struct stat stat(const String & /* path */) const override { return {}; }

    Poco::Timestamp getLastModified(const String & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }
    uint32_t getHardlinkCount(const String & /* path */) const override
    {
        return 1;
    }

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
};

class MetadataStorageFromBackupFileTransaction final : public IMetadataTransaction
{
private:
    DiskPtr disk;
    const MetadataStorageFromBackupFile & metadata_storage;

public:
    explicit MetadataStorageFromBackupFileTransaction(
        const MetadataStorageFromBackupFile & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;

    void createEmptyMetadataFile(const String & /* path */) override
    {
        /// No metadata, no need to create anything.
    }

    void createMetadataFile(const String & /* path */, ObjectStorageKey /* object_key */, uint64_t /* size_in_bytes */) override
    {
        /// Noop
    }

    void createDirectory(const String & /* path */) override
    {
        /// Noop
    }

    void createDirectoryRecursive(const String & /* path */)  override
    {
        /// Noop
    }

    void commit() override
    {
        /// Nothing to commit.
    }

    bool supportsChmod() const override { return false; }
};
}

