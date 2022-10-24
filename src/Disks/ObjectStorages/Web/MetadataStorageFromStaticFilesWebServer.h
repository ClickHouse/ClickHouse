#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/ReadOnlyMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/IDisk.h>


namespace DB
{

class MetadataStorageFromStaticFilesWebServer final : public ReadOnlyMetadataStorage
{
private:
    friend class MetadataStorageFromStaticFilesWebServerTransaction;

    const WebObjectStorage & object_storage;
    std::string root_path;

    void assertExists(const std::string & path) const;

    bool initializeIfNeeded(const std::string & path, std::optional<bool> throw_on_error = std::nullopt) const;

public:
    explicit MetadataStorageFromStaticFilesWebServer(const WebObjectStorage & object_storage_);

    MetadataTransactionPtr createTransaction() const override;

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;

    std::string getObjectStorageRootPath() const override { return ""; }

    struct stat stat(const String & /* path */) const override { return {}; }
};

class MetadataStorageFromStaticFilesWebServerTransaction final : public ReadOnlyMetadataTransaction
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

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;
};

}
