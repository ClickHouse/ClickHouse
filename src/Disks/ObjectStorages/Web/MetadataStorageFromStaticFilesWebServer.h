#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>


namespace DB
{

class MetadataStorageFromStaticFilesWebServer final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromStaticFilesWebServerTransaction;
    using FileType = WebObjectStorage::FileType;

    const WebObjectStorage & object_storage;

    void assertExists(const std::string & path) const;

public:
    explicit MetadataStorageFromStaticFilesWebServer(const WebObjectStorage & object_storage_);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::StaticWeb; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    struct stat stat(const String & /* path */) const override { return {}; }

    Poco::Timestamp getLastModified(const std::string & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }
    uint32_t getHardlinkCount(const std::string & /* path */) const override
    {
        return 1;
    }

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
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

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;

    void createEmptyMetadataFile(const std::string & /* path */) override
    {
        /// No metadata, no need to create anything.
    }

    void createMetadataFile(const std::string & /* path */, ObjectStorageKey /* object_key */, uint64_t /* size_in_bytes */) override
    {
        /// Noop
    }

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;

    void commit() override
    {
        /// Nothing to commit.
    }

    bool supportsChmod() const override { return false; }
};

}
