#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

namespace DB
{

/// In-memory metadata storage for object storage disks.
/// Keeps all file metadata (path -> blobs mapping) and directory structure in memory.
/// Nothing is persisted. On restart, all data is lost.
/// Intended for temporary tables.
class MetadataStorageInMemory final : public IMetadataStorage
{
    friend class MetadataStorageInMemoryTransaction;

public:
    MetadataStorageInMemory(std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_);

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Memory; }

    bool supportsEmptyFilesWithoutBlobs() const override { return true; }
    bool areBlobPathsRandom() const override { return true; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::string readInlineDataToString(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;

    bool isReadOnly() const override { return false; }

    bool supportWritingWithAppend() const override { return true; }

    BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsRemoved(const StoredObjects & blobs) override;

private:
    /// Shared blob ownership: multiple FileEntry instances (hardlinks) can share
    /// the same BlobGroup. Objects are only removed when the last reference is gone.
    struct BlobGroup
    {
        StoredObjects objects;
        int32_t ref_count = 1; /// number of FileEntry instances sharing this group
    };

    struct FileEntry
    {
        std::shared_ptr<BlobGroup> blob_group;
        std::string inline_data;
        bool read_only = false;
        Poco::Timestamp last_modified;

        FileEntry() : blob_group(std::make_shared<BlobGroup>()) {}
    };

    FileEntry * findFile(const std::string & path) const;

    mutable SharedMutex metadata_mutex;

    const std::string compatible_key_prefix;
    const ObjectStorageKeyGeneratorPtr key_generator;
    const std::string root_path;

    /// File metadata: path -> FileEntry
    mutable std::unordered_map<std::string, FileEntry> files;
    /// Set of known directories
    mutable std::set<std::string> directories;

    std::mutex removed_objects_mutex;
    StoredObjectSet objects_to_remove TSA_GUARDED_BY(removed_objects_mutex);
};

class MetadataStorageInMemoryTransaction final : public IMetadataTransaction
{
public:
    explicit MetadataStorageInMemoryTransaction(MetadataStorageInMemory & metadata_storage_);

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    void writeStringToFile(const std::string & path, const std::string & data) override;
    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override { return false; }

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;

    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void removeDirectory(const std::string & path) override;

    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;
    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;

    void truncateFile(const std::string & path, size_t target_size) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;

private:
    /// Each deferred operation captures what to do on commit.
    using Operation = std::function<void()>;
    std::vector<Operation> operations;
    StoredObjects objects_to_remove;

    MetadataStorageInMemory & metadata_storage;
};

}
