#pragma once

#include <Common/SharedMutex.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataOperationsHolder.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>

namespace DB
{

struct UnlinkMetadataFileOperationOutcome;
using UnlinkMetadataFileOperationOutcomePtr = std::shared_ptr<UnlinkMetadataFileOperationOutcome>;

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

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Local; }

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
};

class MetadataStorageFromDiskTransaction final : public IMetadataTransaction, private MetadataOperationsHolder
{
private:
    const MetadataStorageFromDisk & metadata_storage;

public:
    explicit MetadataStorageFromDiskTransaction(const MetadataStorageFromDisk & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    ~MetadataStorageFromDiskTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final;

    void commit() final;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void createEmptyMetadataFile(const std::string & path) override;

    void createMetadataFile(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes) override;

    void addBlobToMetadata(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override { return metadata_storage.supportsChmod(); }

    void chmod(const String & path, mode_t mode) override;

    void setReadOnly(const std::string & path) override;

    void unlinkFile(const std::string & path) override;

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;

    void removeDirectory(const std::string & path) override;

    void removeRecursive(const std::string & path) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;

    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;

    TruncateFileOperationOutcomePtr truncateFile(const std::string & src_path, size_t target_size) override;

};


}
