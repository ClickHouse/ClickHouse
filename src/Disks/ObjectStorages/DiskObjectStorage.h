#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadataHelper.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <re2/re2.h>

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
friend class DiskObjectStorageMetadataHelper;

public:
    DiskObjectStorage(
        const String & name_,
        const String & remote_fs_root_path_,
        const String & log_name,
        DiskPtr metadata_disk_,
        ObjectStoragePtr && object_storage_,
        DiskType disk_type_,
        bool send_metadata_,
        uint64_t thread_pool_size);

    DiskType getType() const override { return disk_type; }

    bool supportZeroCopyReplication() const override { return true; }

    bool supportParallelWrite() const override { return true; }

    using Metadata = DiskObjectStorageMetadata;
    using MetadataUpdater = std::function<bool(Metadata & metadata)>;

    const String & getName() const override { return name; }

    const String & getPath() const override { return metadata_disk->getPath(); }

    std::vector<String> getRemotePaths(const String & local_path) const override;

    void getRemotePathsRecursive(const String & local_path, std::vector<LocalPathWithRemotePaths> & paths_map) override;

    std::string getCacheBasePath() const override
    {
        return object_storage->getCacheBasePath();
    }

    /// Methods for working with metadata. For some operations (like hardlink
    /// creation) metadata can be updated concurrently from multiple threads
    /// (file actually rewritten on disk). So additional RW lock is required for
    /// metadata read and write, but not for create new metadata.
    Metadata readMetadata(const String & path) const;
    Metadata readMetadataUnlocked(const String & path, std::shared_lock<std::shared_mutex> &) const;
    Metadata readUpdateAndStoreMetadata(const String & path, bool sync, MetadataUpdater updater);
    Metadata readUpdateStoreMetadataAndRemove(const String & path, bool sync, MetadataUpdater updater);

    Metadata readOrCreateUpdateAndStoreMetadata(const String & path, WriteMode mode, bool sync, MetadataUpdater updater);

    Metadata createAndStoreMetadata(const String & path, bool sync);
    Metadata createUpdateAndStoreMetadata(const String & path, bool sync, MetadataUpdater updater);

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

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

    void removeFromRemoteFS(const std::vector<String> & paths);

    DiskPtr getMetadataDiskIfExistsOrSelf() override { return metadata_disk; }

    UInt32 getRefCount(const String & path) const override;

    /// Return metadata for each file path. Also, before serialization reset
    /// ref_count for each metadata to zero. This function used only for remote
    /// fetches/sends in replicated engines. That's why we reset ref_count to zero.
    std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    String getUniqueId(const String & path) const override;

    bool checkObjectExists(const String & path) const;
    bool checkUniqueId(const String & id) const override;

    void createHardLink(const String & src_path, const String & dst_path) override;
    void createHardLink(const String & src_path, const String & dst_path, bool should_send_metadata);

    void listFiles(const String & path, std::vector<String> & file_names) override;

    void setReadOnly(const String & path) override;

    bool isDirectory(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    void removeDirectory(const String & path) override;

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    bool isRemote() const override { return true; }

    void shutdown() override;

    void startup(ContextPtr context) override;

    ReservationPtr reserve(UInt64 bytes) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context_, const String &, const DisksMap &) override;

    void restoreMetadataIfNeeded(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context);

    void onFreeze(const String & path) override;

    void syncRevision(UInt64 revision) override;

    UInt64 getRevision() const override;
private:
    const String name;
    const String remote_fs_root_path;
    Poco::Logger * log;
    DiskPtr metadata_disk;

    const DiskType disk_type;
    ObjectStoragePtr object_storage;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;

    mutable std::shared_mutex metadata_mutex;
    void removeMetadata(const String & path, std::vector<String> & paths_to_remove);

    void removeMetadataRecursive(const String & path, std::unordered_map<String, std::vector<String>> & paths_to_remove);

    std::optional<UInt64> tryReserve(UInt64 bytes);

    bool send_metadata;

    std::unique_ptr<DiskObjectStorageMetadataHelper> metadata_helper;
};

class DiskObjectStorageReservation final : public IReservation
{
public:
    DiskObjectStorageReservation(const std::shared_ptr<DiskObjectStorage> & disk_, UInt64 size_)
        : disk(disk_)
        , size(size_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {}

    UInt64 getSize() const override { return size; }

    UInt64 getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override;

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override;

    ~DiskObjectStorageReservation() override;

private:
    std::shared_ptr<DiskObjectStorage> disk;
    UInt64 size;
    UInt64 unreserved_space;
    CurrentMetrics::Increment metric_increment;
};

}
