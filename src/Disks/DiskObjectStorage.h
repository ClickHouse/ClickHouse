#pragma once

#include <Disks/IDisk.h>
#include <Disks/IObjectStorage.h>
#include <re2/re2.h>

namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

class DiskObjectStorageMetadataHelper;


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

    struct Metadata;
    using MetadataUpdater = std::function<bool(Metadata & metadata)>;

    const String & getName() const final override { return name; }

    const String & getPath() const final override { return metadata_disk->getPath(); }

    std::vector<String> getRemotePaths(const String & local_path) const final override;

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

struct DiskObjectStorage::Metadata
{
    using Updater = std::function<bool(DiskObjectStorage::Metadata & metadata)>;
    /// Metadata file version.
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    /// Remote FS objects paths and their sizes.
    std::vector<BlobPathWithSize> remote_fs_objects;

    /// URI
    const String & remote_fs_root_path;

    /// Relative path to metadata file on local FS.
    const String metadata_file_path;

    DiskPtr metadata_disk;

    /// Total size of all remote FS (S3, HDFS) objects.
    size_t total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    ///
    /// FIXME: Why we are tracking it explicetly, without
    /// info from filesystem????
    UInt32 ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

    Metadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_);

    void addObject(const String & path, size_t size);

    static Metadata readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_);
    static Metadata readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static Metadata readUpdateStoreMetadataAndRemove(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);

    static Metadata createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync);
    static Metadata createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static Metadata createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite);

    /// Serialize metadata to string (very same with saveToBuffer)
    std::string serializeToString();

private:
    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false);
    void saveToBuffer(WriteBuffer & buffer, bool sync);
    void load();
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

class DiskObjectStorageMetadataHelper
{
public:
    static constexpr UInt64 LATEST_REVISION = std::numeric_limits<UInt64>::max();
    static constexpr UInt64 UNKNOWN_REVISION = 0;

    DiskObjectStorageMetadataHelper(DiskObjectStorage * disk_, ReadSettings read_settings_)
        : disk(disk_)
        , read_settings(std::move(read_settings_))
    {
    }

    struct RestoreInformation
    {
        UInt64 revision = LATEST_REVISION;
        String source_namespace;
        String source_path;
        bool detached = false;
    };

    using Futures = std::vector<std::future<void>>;

    void createFileOperationObject(const String & operation_name, UInt64 revision, const ObjectAttributes & metadata) const;
    void findLastRevision();

    static int readSchemaVersion(IObjectStorage * object_storage, const String & source_path);
    void saveSchemaVersion(const int & version) const;
    void updateObjectMetadata(const String & key, const ObjectAttributes & metadata) const;
    void migrateFileToRestorableSchema(const String & path) const;
    void migrateToRestorableSchemaRecursive(const String & path, Futures & results);
    void migrateToRestorableSchema();

    void restore(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context);
    void readRestoreInformation(RestoreInformation & restore_information);
    void restoreFiles(IObjectStorage * source_object_storage, const RestoreInformation & restore_information);
    void processRestoreFiles(IObjectStorage * source_object_storage, const String & source_path, const std::vector<String> & keys) const;
    void restoreFileOperations(IObjectStorage * source_object_storage, const RestoreInformation & restore_information);

    std::atomic<UInt64> revision_counter = 0;
    inline static const String RESTORE_FILE_NAME = "restore";

    /// Object contains information about schema version.
    inline static const String SCHEMA_VERSION_OBJECT = ".SCHEMA_VERSION";
    /// Version with possibility to backup-restore metadata.
    static constexpr int RESTORABLE_SCHEMA_VERSION = 1;
    /// Directories with data.
    const std::vector<String> data_roots {"data", "store"};

    DiskObjectStorage * disk;

    ObjectStoragePtr object_storage_from_another_namespace;

    ReadSettings read_settings;
};

}
