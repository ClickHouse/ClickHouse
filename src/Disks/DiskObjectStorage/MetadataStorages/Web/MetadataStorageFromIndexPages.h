#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>

#include <Poco/URI.h>

namespace DB
{

class MetadataStorageFromIndexPages final : public IMetadataStorage
{
private:
    const WebObjectStorage & object_storage;
    LoggerPtr log;
    Poco::URI base_uri;

    std::string makeListingURL(const std::string & path) const;
    std::string readIndexPage(const std::string & url) const;
    std::vector<std::string> extractURLs(const std::string & page_body, const std::string & listing_url, const std::string & path) const;

    bool tryListDirectory(const std::string & path, std::vector<std::string> & result) const;

public:
    explicit MetadataStorageFromIndexPages(const WebObjectStorage & object_storage_);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::WebIndex; }

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
    bool isReadOnly() const override { return true; }
    bool areBlobPathsRandom() const override { return false; }
};

}
