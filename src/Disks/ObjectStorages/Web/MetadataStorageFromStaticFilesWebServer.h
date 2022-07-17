#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>


namespace DB
{

class MetadataStorageFromStaticFilesWebServer final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

    const WebObjectStorage & object_storage;
    std::string root_path;

    void assertExists(const std::string & path) const;

    bool initializeIfNeeded(const std::string & path) const;

public:
    explicit MetadataStorageFromStaticFilesWebServer(const WebObjectStorage & object_storage_);

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

    StoredObjects getStorageObjects(const std::string & path) const override;

    std::string getObjectStorageRootPath() const override { return ""; }
};

class MetadataStorageFromStaticFilesWebServerTransaction final : public IMetadataTransaction
{
private:
    DiskPtr disk;
    const MetadataStorageFromStaticFilesWebServer & metadata_storage;

public:
    explicit MetadataStorageFromStaticFilesWebServerTransaction(
        const MetadataStorageFromStaticFilesWebServer & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    ~MetadataStorageFromStaticFilesWebServerTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;

    void commit() override;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void createEmptyMetadataFile(const std::string & path) override;

    void createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

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
