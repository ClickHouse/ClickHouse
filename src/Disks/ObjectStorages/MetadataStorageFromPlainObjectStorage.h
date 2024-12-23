#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>


namespace DB
{

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
/// structure as on disk MergeTree, and does not requires metadata from local
/// disk to restore.
class MetadataStorageFromPlainObjectStorage final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromPlainObjectStorageTransaction;

    ObjectStoragePtr object_storage;
    String storage_path_prefix;

public:
    MetadataStorageFromPlainObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Plain; }

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    DiskPtr getDisk() const { return {}; }

    StoredObjects getStorageObjects(const std::string & path) const override;

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
};

class MetadataStorageFromPlainObjectStorageTransaction final : public IMetadataTransaction
{
private:
    const MetadataStorageFromPlainObjectStorage & metadata_storage;

    std::vector<MetadataOperationPtr> operations;
public:
    explicit MetadataStorageFromPlainObjectStorageTransaction(const MetadataStorageFromPlainObjectStorage & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    const IMetadataStorage & getStorageForNonTransactionalReads() const override;

    void addBlobToMetadata(const std::string & path, ObjectStorageKey object_key, uint64_t size_in_bytes) override;

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

    void unlinkFile(const std::string & path) override;
    void removeDirectory(const std::string & path) override;

    UnlinkMetadataFileOperationOutcomePtr unlinkMetadata(const std::string & path) override;

    void commit() override
    {
        /// TODO: rewrite with transactions
    }

    bool supportsChmod() const override { return false; }
};

}
