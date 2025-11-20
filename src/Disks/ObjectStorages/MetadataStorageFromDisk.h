#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataOperationsHolder.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/IDisk.h>

namespace DB
{

/// Stores metadata on a separate disk
/// (used for object storages, like S3 and related).
class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromDiskTransaction;

    mutable SharedMutex metadata_mutex;
    DiskPtr disk;
    String compatible_key_prefix;

public:
    MetadataStorageFromDisk(DiskPtr disk_, String compatible_key_prefix);

    MetadataTransactionPtr createTransaction() override;

    bool supportWritingWithAppend() const override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Local; }

    /// Metadata on disk for an empty file can store empty list of blobs and size=0
    bool supportsEmptyFilesWithoutBlobs() const override { return true; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    bool supportsChmod() const override { return disk->supportsChmod(); }

    bool supportsStat() const override { return disk->supportsStat(); }

    struct stat stat(const String & path) const override { return disk->stat(path); }

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::string readInlineDataToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    DiskPtr getDisk() const { return disk; }

    StoredObjects getStorageObjects(const std::string & path) const override;

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> & lock) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> & lock) const;

    bool isReadOnly() const override { return disk->isReadOnly(); }
};

class MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    const MetadataStorageFromDisk & metadata_storage;
    MetadataOperationsHolder operations;

public:
    explicit MetadataStorageFromDiskTransaction(const MetadataStorageFromDisk & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    ~MetadataStorageFromDiskTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final;

    void commit(const TransactionCommitOptionsVariant & options) final;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;

    bool supportAddingBlobToMetadata() override { return true; }

    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override { return metadata_storage.supportsChmod(); }

    void chmod(const String & path, mode_t mode) override;

    void setReadOnly(const std::string & path) override;

    void unlinkFile(const std::string & path) override;

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;

    void removeDirectory(const std::string & path) override;

    void removeRecursive(const std::string & path) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    TruncateFileOperationOutcomePtr truncateFile(const std::string & src_path, size_t target_size) override;

    std::optional<StoredObjects> tryGetBlobsFromTransactionIfExists(const std::string & path) const override;
};

}
