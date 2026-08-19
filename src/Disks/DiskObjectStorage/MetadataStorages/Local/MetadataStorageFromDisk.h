#pragma once

#include <Disks/DiskObjectStorage/Replication/Location.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageTransactionState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/IDisk.h>

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <base/defines.h>

namespace DB
{

/// Stores metadata on a separate disk
/// (used for object storages, like S3 and related).
class MetadataStorageFromDisk final : public IMetadataStorage
{
private:
    friend class MetadataStorageFromDiskTransaction;

    mutable SharedMutex metadata_mutex;
    const DiskPtr disk;
    const std::string compatible_key_prefix;
    const ObjectStorageKeyGeneratorPtr key_generator;

    mutable std::mutex removed_objects_mutex;
    InMemoryRemovalQueue objects_to_remove TSA_GUARDED_BY(removed_objects_mutex);

    static constexpr std::string_view SYSTEM_METADATA_DIR = ".metadata";
    static constexpr std::string_view REMOVAL_LOG_FILE = ".metadata/blobs_to_remove.log";

    enum RemovalLogVersion : UInt32
    {
        V0 = 0, /// initial binary format
    };

    static constexpr RemovalLogVersion REMOVAL_LOG_CURRENT_VERSION = RemovalLogVersion::V0;

    enum RemovalLogEntryType : UInt8
    {
        ADD = 0,
        REMOVED = 1,
    };

    /// Persistence for the removal queue. Returns true if compaction is needed (version mismatch or truncated entries).
    bool loadRemovalLog() TSA_REQUIRES(removed_objects_mutex);
    void appendToRemovalLog(RemovalLogEntryType entry_type, const StoredObjects & blobs) TSA_REQUIRES(removed_objects_mutex);
    void compactRemovalLog() TSA_REQUIRES(removed_objects_mutex);

    /// Number of REMOVED entries in the log that haven't been compacted yet.
    size_t removal_log_stale_entries TSA_GUARDED_BY(removed_objects_mutex) = 0;

    bool persist_removal_queue;
    size_t removal_log_compaction_threshold;
    LoggerPtr log;

public:
    MetadataStorageFromDisk(DiskPtr disk_, String compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_, bool persist_removal_queue_, size_t removal_log_compaction_threshold_);

    MetadataTransactionPtr createTransaction() override;

    bool supportWritingWithAppend() const override;

    const std::string & getPath() const override;

    MetadataStorageType getType() const override { return MetadataStorageType::Local; }

    /// Metadata on disk for an empty file can store empty list of blobs and size=0
    bool supportsEmptyFilesWithoutBlobs() const override { return true; }
    bool areBlobPathsRandom() const override { return true; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const String & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;

    time_t getLastChanged(const std::string & path) const override;

    bool supportsChmod() const override { return disk->supportsChmod(); }

    bool supportsStat() const override { return disk->supportsStat(); }

    struct stat stat(const String & path) const override { return disk->stat(path); }

    std::vector<std::string> listDirectory(const std::string & path) const override;

    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;

    std::string readInlineDataToString(const std::string & path) const override;

    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    DiskPtr getDisk() const { return disk; }

    StoredObjects getStorageObjects(const std::string & path) const override;

    DiskObjectStorageMetadataPtr readMetadata(const std::string & path) const;

    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> & lock) const;
    DiskObjectStorageMetadataPtr readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> & lock) const;

    bool isReadOnly() const override { return disk->isReadOnly(); }

    void startup() override;

    BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsRemoved(const StoredObjects & blobs) override;
    bool hasPendingRemovalBlobs(const StoredObjects & blobs) const override;
};

class MetadataStorageFromDiskTransaction final : public IMetadataTransaction
{
private:
    MetadataStorageFromDisk & metadata_storage;

    /// We collect all removed in transaction blobs here. After successful tx commit
    /// these blobs will be scheduled for background removal (into in-memory queue of outdated blobs).
    StoredObjects objects_to_remove;
    MetadataOperationsHolder operations;

public:
    explicit MetadataStorageFromDiskTransaction(MetadataStorageFromDisk & metadata_storage_);

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant &options) override;

    void writeStringToFile(const std::string & path, const std::string & data) override;

    void writeInlineDataToFile(const std::string & path, const std::string & data) override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;

    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;

    bool supportsChmod() const override { return metadata_storage.supportsChmod(); }

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

    void truncateFile(const std::string & src_path, size_t target_size) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;
};

}
