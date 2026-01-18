#pragma once

#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>

#include <Core/Types.h>

#include <Common/CacheBase.h>
#include <Common/ObjectStorageKeyGenerator.h>

namespace DB
{

struct ObjectMetadataEntry
{
    uint64_t file_size;
    time_t last_modified;
};
using ObjectMetadataEntryPtr = std::shared_ptr<ObjectMetadataEntry>;
using ObjectMetadataCachePtr = std::shared_ptr<CacheBase<UInt128, ObjectMetadataEntry>>;

/// Object storage is used as a filesystem, in a limited form:
/// - no directory concept, files only
/// - no stat/chmod/...
/// - no move/...
/// - limited unlink support
///
/// Also it has excessive API calls.
///
/// It is used to allow BACKUP/RESTORE/ATTACH from ObjectStorage (S3/...) with the same
/// structure as on disk MergeTree, and does not require metadata from a local disk to restore.
class MetadataStorageFromPlainObjectStorage : public IMetadataStorage
{
    friend class MetadataStorageFromPlainObjectStorageTransaction;

    ObjectMetadataEntryPtr getObjectMetadataEntryWithCache(const std::string & path) const;

public:
    MetadataStorageFromPlainObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, size_t object_metadata_cache_size);

    MetadataStorageType getType() const override { return MetadataStorageType::Plain; }
    const std::string & getPath() const override { return storage_path_full; }
    uint32_t getHardlinkCount(const std::string & /* path */) const override { return 0; }
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return false; }
    bool areBlobPathsRandom() const override { return false; }
    bool isPlain() const override { return true; }
    bool isWriteOnce() const override { return true; }

    MetadataTransactionPtr createTransaction() override;

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const String & path) const override;

private:
    const ObjectStoragePtr object_storage;
    const String storage_path_prefix;
    const String storage_path_full;

    mutable ObjectMetadataCachePtr object_metadata_cache;
};

class MetadataStorageFromPlainObjectStorageTransaction : public IMetadataTransaction
{
public:
    explicit MetadataStorageFromPlainObjectStorageTransaction(MetadataStorageFromPlainObjectStorage & metadata_storage_, ObjectStoragePtr object_storage_);

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;
    std::optional<StoredObjects> tryGetBlobsFromTransactionIfExists(const std::string & path) const override;

    void createMetadataFile(const std::string & /*path*/, const StoredObjects & /* objects */) override {}
    void createDirectory(const std::string & /*path*/) override {}
    void createDirectoryRecursive(const std::string & /*path*/) override {}
    void commit(const TransactionCommitOptionsVariant & /*options*/) override {}
    bool supportsChmod() const override { return false; }

    void unlinkFile(const std::string & path) override;
    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;

    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string &) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;

private:
    MetadataStorageFromPlainObjectStorage & metadata_storage;
    ObjectStoragePtr object_storage;
};

}
