#pragma once

#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>

#include <Poco/URI.h>

#include <optional>

namespace DB
{

class MetadataStorageFromIndexPages final : public IMetadataStorage
{
private:
    const WebObjectStorage & object_storage;
    LoggerPtr log;

    struct IndexPage
    {
        std::string body;
        std::string final_url;
    };

    [[nodiscard]] std::vector<std::string> makeListingURLs(const std::string & path, size_t shard_index) const;
    [[nodiscard]] IndexPage readIndexPage(const std::string & url) const;
    [[nodiscard]] std::vector<std::string> extractURLs(
        const std::string & page_body,
        const std::string & listing_url,
        const std::string & listing_prefix_url,
        const std::string & base_url,
        const std::string & source_url,
        const std::string & path) const;

    bool tryListDirectory(
        const std::string & path,
        RelativePathsWithMetadata & result,
        std::optional<size_t> shard_index,
        const std::optional<String> & path_for_glob_matching) const;

public:
    explicit MetadataStorageFromIndexPages(const WebObjectStorage & object_storage_);

    MetadataTransactionPtr createTransaction() override;

    [[nodiscard]] const std::string & getPath() const override;

    [[nodiscard]] MetadataStorageType getType() const override { return MetadataStorageType::WebIndex; }

    [[nodiscard]] bool existsFile(const std::string & path) const override;
    [[nodiscard]] bool existsDirectory(const std::string & path) const override;
    [[nodiscard]] bool existsFileOrDirectory(const std::string & path) const override;

    [[nodiscard]] uint64_t getFileSize(const String & path) const override;
    [[nodiscard]] std::optional<uint64_t> getFileSizeIfExists(const String & path) const override;

    [[nodiscard]] std::vector<std::string> listDirectory(const std::string & path) const override;
    [[nodiscard]] RelativePathsWithMetadata listDirectoryWithMetadata(
        const std::string & path,
        std::optional<size_t> shard_index = std::nullopt,
        const std::optional<String> & path_for_glob_matching = std::nullopt) const;

    [[nodiscard]] DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    [[nodiscard]] StoredObjects getStorageObjects(const std::string & path) const override;
    [[nodiscard]] std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    [[nodiscard]] struct stat stat(const String & /* path */) const override { return {}; }

    [[nodiscard]] Poco::Timestamp getLastModified(const std::string & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }
    [[nodiscard]] uint32_t getHardlinkCount(const std::string & /* path */) const override
    {
        return 1;
    }

    [[nodiscard]] bool supportsChmod() const override { return false; }
    [[nodiscard]] bool supportsStat() const override { return false; }
    [[nodiscard]] bool isReadOnly() const override { return true; }
    [[nodiscard]] bool isRemote() const override { return true; }
    [[nodiscard]] bool areBlobPathsRandom() const override { return false; }
    /// Nothing is ever written, hence nothing is ever removed or replicated.
    [[nodiscard]] bool hasDeadBlobsQueue() const override { return false; }
    [[nodiscard]] bool hasMissingBlobsQueue() const override { return false; }
};

}
