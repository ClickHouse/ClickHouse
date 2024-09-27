#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/InMemoryPathMap.h>
#include <Disks/ObjectStorages/MetadataOperationsHolder.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <Common/CacheBase.h>

#include <map>
#include <string>
#include <unordered_set>


namespace DB
{

struct InMemoryPathMap;
struct UnlinkMetadataFileOperationOutcome;
using UnlinkMetadataFileOperationOutcomePtr = std::shared_ptr<UnlinkMetadataFileOperationOutcome>;

/// Object storage is used as a filesystem, in a limited form:
/// - no directory concept, files only
/// - no stat/chmod/...
/// - no move/...
/// - limited unlink support
///
/// Also it has excessive API calls.
///
/// It is used to allow BACKUP/RESTORE to ObjectStorage (S3/...) with the same
/// structure as on disk MergeTree, and does not require metadata from a local disk to restore.
class MetadataStorageFromPlainObjectStorage : public IMetadataStorage
{
private:
    friend class MetadataStorageFromPlainObjectStorageTransaction;
    mutable std::optional<CacheBase<String, uint64_t>> file_sizes_cache;

protected:
    ObjectStoragePtr object_storage;
    String storage_path_prefix;

    mutable SharedMutex metadata_mutex;

public:
    MetadataStorageFromPlainObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t file_sizes_cache_size);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Plain; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    DiskPtr getDisk() const { return {}; }

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & /* path */) const override
    {
        /// Required by MergeTree
        return {};
    }

    uint32_t getHardlinkCount(const std::string & /* path */) const override
    {
        return 0;
    }

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }

protected:
    /// Get the object storage prefix for storing metadata files.
    virtual std::string getMetadataKeyPrefix() const { return object_storage->getCommonKeyPrefix(); }

    /// Returns a map of virtual filesystem paths to paths in the object storage.
    virtual std::shared_ptr<InMemoryPathMap> getPathMap() const { throwNotImplemented(); }
};

class MetadataStorageFromPlainObjectStorageTransaction final : public IMetadataTransaction, private MetadataOperationsHolder
{
private:
    MetadataStorageFromPlainObjectStorage & metadata_storage;
    ObjectStoragePtr object_storage;

    std::vector<MetadataOperationPtr> operations;

public:
    explicit MetadataStorageFromPlainObjectStorageTransaction(
        MetadataStorageFromPlainObjectStorage & metadata_storage_, ObjectStoragePtr object_storage_)
        : metadata_storage(metadata_storage_), object_storage(object_storage_)
    {}

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;

    void addBlobToMetadata(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes) override;

    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        /// Noop
    }

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

    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void unlinkFile(const std::string & path) override;
    void removeDirectory(const std::string & path) override;

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;

    void commit() override;

    bool supportsChmod() const override { return false; }
};
}
