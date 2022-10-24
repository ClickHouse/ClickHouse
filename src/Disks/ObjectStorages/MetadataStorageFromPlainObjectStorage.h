#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/ReadOnlyMetadataStorage.h>
#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>
#include <Disks/ObjectStorages/MetadataStorageFromDiskTransactionOperations.h>


namespace DB
{

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
///
/// NOTE: Inheritance from ReadOnlyMetadataStorage is used here to throw
/// NOT_IMPLEMENTED error for lost of unsupported methods (mtime/move/stat/...)
class MetadataStorageFromPlainObjectStorage final : public ReadOnlyMetadataStorage
{
private:
    friend class MetadataStorageFromPlainObjectStorageTransaction;

    ObjectStoragePtr object_storage;
    std::string object_storage_root_path;

public:
    MetadataStorageFromPlainObjectStorage(
        ObjectStoragePtr object_storage_,
        const std::string & object_storage_root_path_);

    MetadataTransactionPtr createTransaction() const override;

    const std::string & getPath() const override;

    bool exists(const std::string & path) const override;

    bool isFile(const std::string & path) const override;

    bool isDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    DiskPtr getDisk() const { return {}; }

    StoredObjects getStorageObjects(const std::string & path) const override;

    std::string getObjectStorageRootPath() const override { return object_storage_root_path; }

private:
    std::filesystem::path getAbsolutePath(const std::string & path) const;
};

class MetadataStorageFromPlainObjectStorageTransaction final : public ReadOnlyMetadataTransaction
{
private:
    const MetadataStorageFromPlainObjectStorage & metadata_storage;

    std::vector<MetadataOperationPtr> operations;
public:
    MetadataStorageFromPlainObjectStorageTransaction(const MetadataStorageFromPlainObjectStorage & metadata_storage_)
        : metadata_storage(metadata_storage_)
    {}

    ~MetadataStorageFromPlainObjectStorageTransaction() override = default;

    const IMetadataStorage & getStorageForNonTransactionalReads() const final;

    void addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes) override;

    void createDirectory(const std::string & path) override;

    void createDirectoryRecursive(const std::string & path) override;

    void unlinkFile(const std::string & path) override;

    void unlinkMetadata(const std::string & path) override;
};

}
