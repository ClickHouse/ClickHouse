#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageRemoteMetadataRestoreHelper.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Common/re2.h>

#include "config.h"


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

/// Disk build on top of IObjectStorage. Use additional disk (local for example)
/// for metadata storage. Metadata is a small files with mapping from local paths to
/// objects in object storage, like:
/// "/var/lib/clickhouse/data/db/table/all_0_0_0/columns.txt" -> /xxxxxxxxxxxxxxxxxxxx
///                                                           -> /yyyyyyyyyyyyyyyyyyyy
class DiskObjectStorage : public IDisk
{

friend class DiskObjectStorageReservation;
friend class DiskObjectStorageRemoteMetadataRestoreHelper;

public:
    DiskObjectStorage(
        const String & name_,
        const String & object_key_prefix_,
        MetadataStoragePtr metadata_storage_,
        ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix);

    /// Create fake transaction
    DiskTransactionPtr createTransaction() override;

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }

    bool supportZeroCopyReplication() const override { return true; }

    bool supportParallelWrite() const override { return object_storage->supportParallelWrite(); }

    const String & getPath() const override { return metadata_storage->getPath(); }

    StoredObjects getStorageObjects(const String & local_path) const override;

    const std::string & getCacheName() const override { return object_storage->getCacheName(); }

    std::optional<UInt64> getTotalSpace() const override { return {}; }
    std::optional<UInt64> getAvailableSpace() const override { return {}; }
    std::optional<UInt64> getUnreservedSpace() const override { return {}; }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool existsFile(const String & path) const override;
    bool existsDirectory(const String & path) const override;
    bool existsFileOrDirectory(const String & path) const override;

    void createFile(const String & path) override;

    size_t getFileSize(const String & path) const override;

    void moveFile(const String & from_path, const String & to_path) override;

    void moveFile(const String & from_path, const String & to_path, bool should_send_metadata);

    void replaceFile(const String & from_path, const String & to_path) override;

    void removeFile(const String & path) override { removeSharedFile(path, false); }

    void removeFileIfExists(const String & path) override { removeSharedFileIfExists(path, false); }

    void removeRecursive(const String & path) override { removeSharedRecursive(path, false, {}); }

    void removeSharedFile(const String & path, bool delete_metadata_only) override;

    void removeSharedFileIfExists(const String & path, bool delete_metadata_only) override;

    void removeSharedRecursive(const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only) override;

    void truncateFile(const String & path, size_t size) override;

    MetadataStoragePtr getMetadataStorage() override { return metadata_storage; }

    UInt32 getRefCount(const String & path) const override;

    /// Return metadata for each file path. Also, before serialization reset
    /// ref_count for each metadata to zero. This function used only for remote
    /// fetches/sends in replicated engines. That's why we reset ref_count to zero.
    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    String getUniqueId(const String & path) const override;

    bool checkUniqueId(const String & id) const override;

    void createHardLink(const String & src_path, const String & dst_path) override;
    void createHardLink(const String & src_path, const String & dst_path, bool should_send_metadata);

    void listFiles(const String & path, std::vector<String> & file_names) const override;

    void setReadOnly(const String & path) override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    void removeDirectory(const String & path) override;

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) const override;

    time_t getLastChanged(const String & path) const override;

    bool isRemote() const override { return true; }

    void shutdown() override;

    void startupImpl(ContextPtr context) override;

    ReservationPtr reserve(UInt64 bytes) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    Strings getBlobPath(const String & path) const override;
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void copyFile( /// NOLINT
        const String & from_file_path,
        IDisk & to_disk,
        const String & to_file_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings = {},
        const std::function<void()> & cancellation_hook = {}
        ) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context_, const String &, const DisksMap &) override;

    void restoreMetadataIfNeeded(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context);

    void onFreeze(const String & path) override;

    void syncRevision(UInt64 revision) override;

    UInt64 getRevision() const override;

    ObjectStoragePtr getObjectStorage() override;

    DiskObjectStoragePtr createDiskObjectStorage() override;

    bool supportsCache() const override;

    /// Is object storage read only?
    /// For example: WebObjectStorage is read only as it allows to read from a web server
    /// with static files, so only read-only operations are allowed for this storage.
    bool isReadOnly() const override;

    /// Is object write-once?
    /// For example: S3PlainObjectStorage is write once, this means that it
    /// does support BACKUP to this disk, but does not support INSERT into
    /// MergeTree table on this disk.
    bool isWriteOnce() const override;

    bool supportsHardLinks() const override;

    /// Get structure of object storage this disk works with. Examples:
    /// DiskObjectStorage(S3ObjectStorage)
    /// DiskObjectStorage(CachedObjectStorage(S3ObjectStorage))
    /// DiskObjectStorage(CachedObjectStorage(CachedObjectStorage(S3ObjectStorage)))
    String getStructure() const { return fmt::format("DiskObjectStorage-{}({})", getName(), object_storage->getName()); }

    /// Add a cache layer.
    /// Example: DiskObjectStorage(S3ObjectStorage) -> DiskObjectStorage(CachedObjectStorage(S3ObjectStorage))
    /// There can be any number of cache layers:
    /// DiskObjectStorage(CachedObjectStorage(...CacheObjectStorage(S3ObjectStorage)...))
    void wrapWithCache(FileCachePtr cache, const FileCacheSettings & cache_settings, const String & layer_name);

    /// Get names of all cache layers. Name is how cache is defined in configuration file.
    NameSet getCacheLayersNames() const override;

    bool supportsStat() const override { return metadata_storage->supportsStat(); }
    struct stat stat(const String & path) const override;

    bool supportsChmod() const override { return metadata_storage->supportsChmod(); }
    void chmod(const String & path, mode_t mode) override;

#if USE_AWS_S3
    std::shared_ptr<const S3::Client> getS3StorageClient() const override;
    std::shared_ptr<const S3::Client> tryGetS3StorageClient() const override;
#endif

private:

    /// Create actual disk object storage transaction for operations
    /// execution.
    DiskTransactionPtr createObjectStorageTransaction();
    DiskTransactionPtr createObjectStorageTransactionToAnotherDisk(DiskObjectStorage& to_disk);

    String getReadResourceName() const;
    String getWriteResourceName() const;

    const String object_key_prefix;
    LoggerPtr log;

    MetadataStoragePtr metadata_storage;
    ObjectStoragePtr object_storage;
    DataSourceDescription data_source_description;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;

    bool tryReserve(UInt64 bytes);
    void sendMoveMetadata(const String & from_path, const String & to_path);

    const bool send_metadata;

    mutable std::mutex resource_mutex;
    String read_resource_name;
    String write_resource_name;

    std::unique_ptr<DiskObjectStorageRemoteMetadataRestoreHelper> metadata_helper;
};

using DiskObjectStoragePtr = std::shared_ptr<DiskObjectStorage>;

class DiskObjectStorageReservation final : public IReservation
{
public:
    DiskObjectStorageReservation(const std::shared_ptr<DiskObjectStorage> & disk_, UInt64 size_)
        : disk(disk_)
        , size(size_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {}

    UInt64 getSize() const override { return size; }

    std::optional<UInt64> getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override;

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override;

    ~DiskObjectStorageReservation() override;

private:
    DiskObjectStoragePtr disk;
    UInt64 size;
    UInt64 unreserved_space;
    CurrentMetrics::Increment metric_increment;
};

}
