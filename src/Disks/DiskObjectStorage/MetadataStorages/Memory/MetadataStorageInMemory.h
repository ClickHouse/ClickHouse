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
    /// Per-inode state shared by all hardlinks: object list, inline data, and modification time.
    /// On disk, these come from the metadata file content / inode mtime, so hardlinks observe
    /// the same values. The in-memory storage mirrors that by sharing this struct via shared_ptr.
    /// `objects` are removed when the last reference is gone.
    struct BlobGroup
    {
        StoredObjects objects;
        std::string inline_data;
        Poco::Timestamp last_modified;
        int32_t ref_count = 1; /// number of FileEntry instances sharing this group
    };

    struct FileEntry
    {
        std::shared_ptr<BlobGroup> blob_group;

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

    /// Operation-level rollback journal. Each operation records, before mutating,
    /// what is needed to restore the original state of the touched entries.
    /// On a partial-failure exception we replay these in reverse, so commit cost
    /// stays proportional to changed entries instead of the whole `files` map.
    /// All `record*` helpers are idempotent within a transaction, so multiple
    /// operations touching the same key (or shared `BlobGroup`) preserve only the
    /// pre-transaction state.
    void recordFileBefore(const std::string & path);
    void recordBlobGroupBefore(const std::shared_ptr<MetadataStorageInMemory::BlobGroup> & group);
    void recordDirInsert(const std::string & dir);
    void recordDirErase(const std::string & dir);
    void rollback();

    /// Pre-mutation snapshot of every file entry touched by this transaction.
    /// `nullopt` means the path did not exist before the transaction started.
    std::unordered_map<std::string, std::optional<MetadataStorageInMemory::FileEntry>> files_undo;
    /// Pre-mutation content of every `BlobGroup` mutated in place. The `shared_ptr` copy
    /// in `BlobGroupSnapshot::alive` keeps the group alive across the whole transaction:
    /// without it, a group whose only reference is erased mid-transaction can be freed,
    /// then a new allocation may reuse the same address, causing the rollback step to
    /// dereference an unrelated `BlobGroup`.
    struct BlobGroupSnapshot
    {
        std::shared_ptr<MetadataStorageInMemory::BlobGroup> alive;
        MetadataStorageInMemory::BlobGroup snapshot;
    };
    std::unordered_map<MetadataStorageInMemory::BlobGroup *, BlobGroupSnapshot> blob_group_undo;
    /// Directory entries inserted by this transaction (rollback: erase).
    std::set<std::string> dirs_inserted;
    /// Directory entries erased by this transaction (rollback: re-insert).
    std::set<std::string> dirs_erased;

    MetadataStorageInMemory & metadata_storage;
};

}
