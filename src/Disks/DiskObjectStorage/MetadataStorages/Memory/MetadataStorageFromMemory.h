#pragma once

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Memory/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

namespace DB
{

/// In-memory metadata storage over standard `DiskObjectStorageMetadata` records, kept as an
/// `InMemoryDirectoryTree`. Starts with an empty root; operations of its transactions apply
/// immediately and enforce the same path topology as the on-disk backends. A record with
/// `ref_count > 0` shares its blobs with another owner, so unlinking it never releases them.
class MetadataStorageFromMemory final : public IMetadataStorage
{
    /// The record of the file at `path`; throws if there is no file there.
    const DiskObjectStorageMetadata & getRecordUnlocked(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path) const;

    void assertExists(const std::string & path) const;

    /// The unique record whose objects contain `remote_path`; throws if none.
    DiskObjectStorageMetadata & findRecordOfBlobUnlocked(const std::string & remote_path);

public:
    /// `key_generator_` may be null for a storage that only resolves existing blobs.
    MetadataStorageFromMemory(std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_);

    ~MetadataStorageFromMemory() override = default;

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Memory; }

    bool existsFile(const std::string & path) const override;

    bool existsDirectory(const std::string & path) const override;

    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    /// Direct children of `path` as full paths, in lexicographic order.
    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::string readInlineDataToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;

    bool supportsStat() const override { return false; }

    struct stat stat(const String & path) const override;

    bool supportsChmod() const override { return false; }

    bool isReadOnly() const override { return false; }

    bool areBlobPathsRandom() const override { return true; }

    /// An empty file is a record with no object and no inline data, so no zero-byte blob is uploaded.
    bool supportsEmptyFilesWithoutBlobs() const override { return true; }

    bool supportsInlineData() const override { return true; }

    bool appliesOperationsEagerly() const override { return true; }

    ObjectStorageKeyGeneratorPtr getKeyGenerator() const override { return key_generator; }

    /// True if any shared-blob (`ref_count > 0`) mark is present. Marks are consumed by the
    /// storage's owner, so a freshly created or loaded storage must have none.
    bool hasTransientBuildState() const;

private:
    friend class MetadataStorageFromMemoryTransaction;

    std::string compatible_key_prefix;
    ObjectStorageKeyGeneratorPtr key_generator;

    InMemoryDirectoryTree tree;

    mutable SharedMutex metadata_mutex;
};

using MetadataStorageFromMemoryPtr = std::shared_ptr<MetadataStorageFromMemory>;

/// Transaction over `MetadataStorageFromMemory`: every operation applies immediately and
/// `commit` is trivial.
class MetadataStorageFromMemoryTransaction final : public IMetadataTransaction
{
public:
    explicit MetadataStorageFromMemoryTransaction(MetadataStorageFromMemory & storage_) : storage(storage_) {}

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    bool supportsChmod() const override { return false; }

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;
    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;

    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    void recordBlobsReplication(const StoredObject & blob, const Locations & missing_locations) override;
    StoredObjects getSubmittedForRemovalBlobs() override;

    /// Mark the record holding the blob as shared / owned; the record must exist.
    void incrementBlobRefCount(const std::string & blob) override;
    void decrementBlobRefCount(const std::string & blob) override;

    /// Blobs of removed or overwritten sole-owner records; the caller deletes them physically.
    /// Clears the accumulator.
    std::vector<String> takePendingOwnRemovals();

    /// Records accumulated by `recordBlobsReplication`, keyed by blob. Clears the accumulator.
    std::unordered_map<String, Locations> takeReplicationRecords();

private:
    /// Queue the blobs of a removed/overwritten record for disposal; `ref_count > 0` releases nothing.
    void releaseRecordUnlocked(const DiskObjectStorageMetadata & metadata);

    /// Insert a file `metadata` at `path`, disposing of the blob of a file it overwrites.
    void putRecordUnlocked(const std::string & path, DiskObjectStorageMetadata metadata);

    MetadataStorageFromMemory & storage;

    std::vector<String> pending_own_removals;
    std::unordered_map<String, Locations> replication_records;
};

}
