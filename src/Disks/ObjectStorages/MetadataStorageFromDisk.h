#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>

namespace DB
{

class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromDiskTransaction;

    mutable std::shared_mutex metadata_mutex;

    DiskPtr disk;
    std::string object_storage_root_path;

public:
    MetadataStorageFromDisk(DiskPtr disk_, const std::string & object_storage_root_path_);

    MetadataTransactionPtr createTransaction() const override;

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    DiskPtr getDisk() const { return disk; }

    StoredObjects getStorageObjects(const std::string & path) const override;

    std::string getObjectStorageRootPath() const override { return object_storage_root_path; }

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<std::shared_mutex> & lock) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> & lock) const;

    /// check and fix furcated metadata files
    void checkAndFixMetadataHardLinkUnlocked(const std::string & path, std::unique_lock<std::shared_mutex> & lock) const;
    void checkAndFixMetadataHardLink(const std::string & path) const override;
};

class MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    const MetadataStorageFromDisk & metadata_storage;

    std::vector<MetadataOperationPtr> operations;
    MetadataFromDiskTransactionState state{MetadataFromDiskTransactionState::PREPARING};

    void addOperation(MetadataOperationPtr && operation);

    void rollback(size_t until_pos);

public:
    explicit MetadataStorageFromDiskTransaction(const MetadataStorageFromDisk & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    ~MetadataStorageFromDiskTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final;

    void commit() final;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void createEmptyMetadataFile(const std::string & path) override;

    void createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

    void createMetadataFileFromContent(const std::string & path, const std::string & content) override;

    void addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

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

    void unlinkMetadata(const std::string & path) override;
};


}
