#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>

namespace DB
{

class MetadataStorageFromCacheObjectStorage : public IMetadataStorage
{
    friend class MetadataStorageFromCacheObjectStorageTransaction;

public:
    explicit MetadataStorageFromCacheObjectStorage(MetadataStoragePtr underlying_);
    ~MetadataStorageFromCacheObjectStorage() override = default;

    MetadataStoragePtr getUnderlying() const;

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;
    MetadataStorageType getType() const override;
    std::string getZooKeeperName() const override;
    std::string getZooKeeperPath() const override;

    bool supportsEmptyFilesWithoutBlobs() const override;
    bool areBlobPathsRandom() const override;

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const std::string & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    bool supportsChmod() const override;
    bool supportsStat() const override;
    struct stat stat(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;
    bool isDirectoryEmpty(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;
    std::string readInlineDataToString(const std::string & path) const override;

    void startup() override;
    void shutdown() override;
    void refresh(UInt64 not_sooner_than_milliseconds) override;

    std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    bool isReadOnly() const override;
    bool isTransactional() const override;
    bool isPlain() const override;
    bool isWriteOnce() const override;
    bool supportWritingWithAppend() const override;

    BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsRemoved(const StoredObjects & blobs) override;

    BlobsToReplicate getBlobsToReplicate(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsReplicated(const BlobsToReplicate & blobs) override;
    bool hasUnreplicatedBlobs(const Location & location_to_check) override;

    void updateCache(const std::vector<std::string> & paths, bool recursive, bool enforce_fresh, std::string * serialized_cache_update_description) override;
    void updateCacheFromSerializedDescription(const std::string & serialized_cache_update_description) override;
    void invalidateCache(const std::string & path) override;
    void dropCache() override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override;

private:
    const MetadataStoragePtr underlying;

    std::mutex removed_objects_mutex;
    StoredObjectSet objects_to_remove TSA_GUARDED_BY(removed_objects_mutex);
};

class MetadataStorageFromCacheObjectStorageTransaction : public IMetadataTransaction
{
public:
    explicit MetadataStorageFromCacheObjectStorageTransaction(MetadataTransactionPtr underlying_, MetadataStorageFromCacheObjectStorage & metadata_storage_);
    ~MetadataStorageFromCacheObjectStorageTransaction() override = default;

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    void writeStringToFile(const std::string & path, const std::string & data) override;
    void writeInlineDataToFile(const std::string & path, const std::string & data) override;
    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override;
    void chmod(const String & path, mode_t mode) override;

    void setReadOnly(const std::string & path) override;

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;

    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    void recordBlobsReplication(const StoredObject & blob, const Locations & missing_locations) override;
    StoredObjects getSubmittedForRemovalBlobs() override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;
    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;
    void truncateFile(const std::string & path, size_t size) override;

private:
    const MetadataTransactionPtr underlying;
    MetadataStorageFromCacheObjectStorage & metadata_storage;
};

}
