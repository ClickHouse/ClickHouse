#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/InMemoryDirectoryPathMap.h>
#include <Disks/ObjectStorages/MetadataOperationsHolder.h>
#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <Common/CacheBase.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <Poco/Timestamp.h>


namespace DB
{

struct InMemoryDirectoryPathMap;
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

protected:
    struct ObjectMetadataEntry
    {
        uint64_t file_size;
        time_t last_modified;
    };
    using ObjectMetadataEntryPtr = std::shared_ptr<ObjectMetadataEntry>;

    ObjectStoragePtr object_storage;
    const String storage_path_prefix;

    mutable std::optional<CacheBase<UInt128, ObjectMetadataEntry>> object_metadata_cache;

    mutable SharedMutex metadata_mutex;

public:
    MetadataStorageFromPlainObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size);

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

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const String & path) const override;

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
    virtual std::shared_ptr<InMemoryDirectoryPathMap> getPathMap() const { throwNotImplemented(); }

    ObjectMetadataEntryPtr getObjectMetadataEntryWithCache(const std::string & path) const;
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

    void addBlobToMetadata(const std::string & /* path */, ObjectStorageKey /* object_key */, uint64_t /* size_in_bytes */) override
    {
        // Noop
    }

    void setLastModified(const String &, const Poco::Timestamp &) override
    {
        /// Noop
    }

    void createEmptyMetadataFile(const std::string & /* path */) override;

    void createMetadataFile(const std::string & /* path */, ObjectStorageKey /* object_key */, uint64_t /* size_in_bytes */) override;

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
