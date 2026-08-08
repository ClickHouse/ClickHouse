#pragma once
#include <Core/NamesAndTypes.h>
#include <Disks/WriteMode.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <base/types.h>
#include <Common/TransactionID.h>

#include <memory>
#include <mutex>
#include <optional>

#include <boost/core/noncopyable.hpp>
#include <Poco/Timestamp.h>

namespace DB
{
struct ReadSettings;
class ReadBufferFromFileBase;
class ReadPipeline;
class WriteBufferFromFileBase;

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

struct CanRemoveDescription
{
    bool can_remove_anything;
    NameSet files_not_to_remove;
};

using CanRemoveCallback = std::function<CanRemoveDescription()>;

class IDataPartStorageIterator
{
public:
    /// Iterate to the next file.
    virtual void next() = 0;

    /// Return `true` if the iterator points to a valid element.
    virtual bool isValid() const = 0;

    /// Return `true` if the iterator points to a file.
    virtual bool isFile() const = 0;

    /// Name of the file that the iterator currently points to.
    virtual std::string name() const = 0;

    /// Path of the file that the iterator currently points to.
    virtual std::string path() const = 0;

    virtual ~IDataPartStorageIterator() = default;
};

using DataPartStorageIteratorPtr = std::unique_ptr<IDataPartStorageIterator>;

struct MergeTreeDataPartChecksums;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class ISyncGuard;
using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct BackupSettings;

struct WriteSettings;

class TemporaryFileOnDisk;


struct HardlinkedFiles
{
    /// Shared table uuid where hardlinks live
    std::string source_table_shared_id;
    /// Hardlinked from part
    std::string source_part_name;
    /// Hardlinked files list
    NameSet hardlinks_from_source_part;
};

/// This is an abstraction of storage for data part files.
/// Ideally, it is assumed to contain read-only methods from IDisk.
/// It is not fulfilled now, but let's try our best.
class IDataPartStorage : public boost::noncopyable
{
public:
    virtual ~IDataPartStorage() = default;

    virtual MergeTreeDataPartStorageType getType() const = 0;

    /// Methods to get path components of a data part.
    virtual std::string getFullPath() const = 0;         /// '/var/lib/clickhouse/data/database/table/moving/all_1_5_1'
    virtual std::string getRelativePath() const = 0;     ///                          'database/table/moving/all_1_5_1'
    virtual std::string getPartDirectory() const = 0;    ///                                                'all_1_5_1'
    virtual std::string getFullRootPath() const = 0;     /// '/var/lib/clickhouse/data/database/table/moving'
    virtual std::string getParentDirectory() const = 0;  ///                                                '' (or 'detached' for 'detached/all_1_5_1')
    /// Can add it if needed                             ///                          'database/table/moving'
    /// virtual std::string getRelativeRootPath() const = 0;

    /// Get a storage for projection.
    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name, bool use_parent_transaction = true) = 0; // NOLINT

    virtual std::shared_ptr<IDataPartStorage> getProjectionNoInitialize(const std::string & name, bool use_parent_transaction = true) // NOLINT
    {
        return getProjection(name, use_parent_transaction);
    }

    virtual std::shared_ptr<const IDataPartStorage> getProjection(const std::string & name) const = 0;

    /// Part directory exists.
    virtual bool exists() const = 0;

    /// File inside part directory exists. Specified path is relative to the part path.
    virtual bool existsFile(const std::string & name) const = 0;
    virtual bool existsDirectory(const std::string & name) const = 0;

    /// Modification time for part directory.
    virtual Poco::Timestamp getLastModified() const = 0;

    /// Iterate part directory. Iteration in subdirectory is not needed yet.
    virtual DataPartStorageIteratorPtr iterate() const = 0;

    /// Get metadata for a file inside path dir.
    virtual Poco::Timestamp getFileLastModified(const std::string & file_name) const = 0;
    virtual size_t getFileSize(const std::string & file_name) const = 0;
    /// Uncompressed size of a packed skip-index substream from the archive index, or nullopt if
    /// unknown (non-packed file, or v0 archive). Callers fall back to the compressed size.
    virtual std::optional<UInt64> getPackedFileUncompressedSize(const std::string & /*file_name*/) const { return {}; }
    virtual UInt32 getRefCount(const std::string & file_name) const = 0;

    /// Get path on remote filesystem from file name on local filesystem.
    virtual std::vector<std::string> getRemotePaths(const std::string & file_name) const = 0;

    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    /// Convenience wrapper: calls prepareRead() + pipeline.build().
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const;

    /// Populate a ReadPipeline with the stages needed to read from this part storage.
    /// Every implementation must override this method.
    virtual void prepareRead(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const = 0;

    virtual std::unique_ptr<ReadBufferFromFileBase> readFileIfExists(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const
    {
        if (existsFile(name))
            return readFile(name, settings, read_hint);
        return {};
    }

    struct ProjectionChecksums
    {
        const std::string & name;
        const MergeTreeDataPartChecksums & checksums;
    };

    /// Everything the removal of a data part depends on, gathered in one place.
    /// can_remove_callback answers whether shared data may be removed (specific for DiskObjectStorage).
    /// projections and checksums are needed to avoid recursive listing.
    struct RemoveParams
    {
        CanRemoveCallback can_remove_callback;
        const MergeTreeDataPartChecksums & checksums;
        std::list<ProjectionChecksums> projections = {};
        bool is_temp = false;
        LoggerPtr log = nullptr;
    };

    /// Remove data part.
    virtual void remove(RemoveParams params) = 0;

    /// Get a name like 'prefix_partdir_tryN' which does not exist in a root dir.
    /// TODO: remove it.
    virtual std::optional<String> getRelativePathForPrefix(
        LoggerPtr log, const String & prefix, bool detached, bool broken) const = 0;

    /// Reset part directory, used for in-memory parts.
    /// TODO: remove it.
    virtual void setRelativePath(const std::string & path) = 0;

    /// Some methods from IDisk. Needed to avoid getting internal IDisk interface.
    virtual std::string getDiskName() const = 0;
    virtual std::string getDiskType() const = 0;
    virtual bool isStoredOnRemoteDisk() const { return false; }
    virtual std::optional<String> getCacheName() const { return std::nullopt; }
    virtual bool supportZeroCopyReplication() const { return false; }
    virtual bool supportParallelWrite() const = 0;
    virtual bool isBroken() const = 0;
    virtual bool isReadonly() const = 0;

    /// Get a path for internal disk if relevant. It is used mainly for logging.
    virtual std::string getDiskPath() const = 0;

    /// Reserve space on the same disk.
    /// Probably we should try to remove it later.
    /// TODO: remove constness
    virtual ReservationPtr reserve(UInt64 /*bytes*/) const  { return nullptr; }
    virtual ReservationPtr tryReserve(UInt64 /*bytes*/) const  { return nullptr; }

    /// A leak of abstraction.
    /// Return some uniq string for file.
    /// Required for distinguish different copies of the same part on remote FS.
    virtual String getUniqueId() const = 0;

    /// Represents metadata which is required for fetching of part.
    struct ReplicatedFilesDescription
    {
        using InputBufferGetter = std::function<std::unique_ptr<ReadBuffer>()>;

        struct ReplicatedFileDescription
        {
            InputBufferGetter input_buffer_getter;
            size_t file_size{};
        };

        std::map<String, ReplicatedFileDescription> files;

        /// Unique string that is used to distinguish different
        /// copies of the same part on remote disk
        String unique_id;
    };

    virtual ReplicatedFilesDescription getReplicatedFilesDescription(const NameSet & file_names) const = 0;
    virtual ReplicatedFilesDescription getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const = 0;

    using TemporaryFilesOnDisks = std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>>;

    /// Everything the backup of a data part depends on, gathered in one place.
    struct BackupParams
    {
        const MergeTreeDataPartChecksums & checksums;
        NameSet files_without_checksums;
        String path_in_backup;
        const BackupSettings & backup_settings;
        bool make_temporary_hard_links = false;
        /// Output: new entries are added here.
        BackupEntries & backup_entries;
        TemporaryFilesOnDisks * temp_dirs = nullptr;
        bool is_projection_part = false;
        bool allow_backup_broken_projection = false;
    };

    /// Create a backup of a data part.
    /// This method adds a new entry to params.backup_entries.
    /// Also creates a new tmp_dir for internal disk (if disk is mentioned the first time).
    virtual void backup(const BackupParams & params) const = 0;

    /// Creates hardlinks into 'to/dir_path' for every file in data part.
    /// Some files can be copied instead of hardlinks. It's because of details of zero copy replication
    /// implementation which relies on paths of some blobs in S3. For example if we want to hardlink
    /// the whole part during mutation we shouldn't hardlink checksums.txt, because otherwise
    /// zero-copy locks for different parts will be on the same path in zookeeper.
    ///
    /// If `external_transaction` is provided, the disk operations (creating directories, hardlinking,
    /// etc) won't be applied immediately; instead, they'll be added to external_transaction, which the
    /// caller then needs to commit.

    struct ClonePartParams
    {
        MergeTreeTransactionPtr txn = NO_TRANSACTION_PTR;
        HardlinkedFiles * hardlinked_files = nullptr;
        bool copy_instead_of_hardlink = false;
        NameSet files_to_copy_instead_of_hardlinks = {};
        bool keep_metadata_version = false;
        bool make_source_readonly = false;
        DiskTransactionPtr external_transaction = nullptr;
        std::optional<int32_t> metadata_version_to_write = std::nullopt;
        NameSet invalidated_columns_to_write = {};
        /// fsync the cloned/frozen directories (the clone subtree plus the ancestor chain up to
        /// the disk root) so the new hardlink directory entries survive a power loss. Only honored
        /// by freeze() on a local disk, outside an external transaction.
        bool fsync_part_directory = false;
    };

    /// For packed storage the whole data.packed archive is rewritten (copied) during a clone whenever
    /// any file it contains must be copied instead of hardlinked, the metadata version is overwritten,
    /// or the version is dropped. When that happens none of the archive's logical members (of the part
    /// or its packed projections) are hardlinked from the source, so the caller must not record them as
    /// shared blobs. Full storage hardlinks members individually and has no such archive, so it never
    /// copies a whole archive.
    virtual bool cloneCopiesWholeArchive(const ClonePartParams & /*params*/) const { return false; }

    virtual std::shared_ptr<IDataPartStorage> freeze(
        const std::string & to,
        const std::string & dir_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        const ClonePartParams & params) const = 0;

    virtual std::shared_ptr<IDataPartStorage> freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const = 0;

    /// Make a full copy of a data part into 'to/dir_path' (possibly to a different disk).
    virtual std::shared_ptr<IDataPartStorage> clonePart(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & disk,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        LoggerPtr log,
        const std::function<void()> & cancellation_hook
        ) const = 0;

    /// Change part's root. from_root should be a prefix path of current root path.
    /// Right now, this is needed for rename table query.
    virtual void changeRootPath(const std::string & from_root, const std::string & to_root) = 0;

    virtual void createDirectories() = 0;
    virtual void createProjection(const std::string & name) = 0;

    /// Hint for the preferred on-disk order of files. Packed storage uses it to lay out the
    /// single archive; storages that keep files separately can ignore it (default no-op).
    virtual void setPreferredFileOrder(const Strings & /*file_names*/) {}

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) = 0;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & name,
        size_t buf_size,
        const WriteSettings & settings)
    {
        return writeFile(name, buf_size, WriteMode::Rewrite, settings);
    }

    /// A special const method to write transaction file.
    /// It's const, because file with transaction metadata
    /// can be modified after part creation.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeTransactionFile(const String & txn_file_name, WriteMode mode) const = 0;

    virtual void createFile(const String & name) = 0;
    virtual void moveFile(const String & from_name, const String & to_name) = 0;
    virtual void replaceFile(const String & from_name, const String & to_name) = 0;

    virtual void removeFile(const String & name) = 0;
    virtual void removeFileIfExists(const String & name) = 0;
    virtual void removeRecursive() = 0;
    virtual void removeSharedRecursive(bool keep_in_remote_fs) = 0;

    virtual SyncGuardPtr getDirectorySyncGuard() const { return nullptr; }

    virtual void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) = 0;
    virtual void copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) = 0;

    /// Options of the rename operation which do not describe the destination itself.
    struct RenameParams
    {
        LoggerPtr log = nullptr;
        bool remove_new_dir_if_exists = false;
        bool fsync_part_dir = false;
    };

    /// Rename part.
    /// Ideally, new_root_path should be the same as current root (but it is not true).
    /// Examples are: 'all_1_2_1' -> 'detached/all_1_2_1'
    ///               'moving/tmp_all_1_2_1' -> 'all_1_2_1'
    virtual void rename(
        std::string new_root_path,
        std::string new_part_dir,
        const RenameParams & params) = 0;

    /// Starts a transaction of mutable operations.
    virtual void beginTransaction() = 0;
    /// Commits a transaction of mutable operations.
    virtual void commitTransaction() = 0;
    /// Prepares transaction to commit.
    /// It may be flush of buffered data or similar.
    virtual void precommitTransaction() = 0;
    virtual bool hasActiveTransaction() const = 0;

    /// Returns true if underlying filesystem is case-insensitive,
    /// e.g. file_name and FILE_NAME are the same files.
    virtual bool isCaseInsensitive() const = 0;
};

using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class IMergeTreeDataPart;
struct IMergeTreeIndex;
class ColumnsStatistics;
class PackedFilesReader;

/// ===== Facets of the data part composition =====
///
/// A data part is composed of an immutable main part (column data, marks, primary index)
/// plus optional sub-blocks stored inside the part directory but logically separate:
/// projections, secondary indexes and statistics - all re-materializable from the main part.
/// Each facet below owns the on-disk structure of one sub-block; IMergeTreeDataPart composes
/// them and stays the logical representation.

/// Projection sub-block of a data part.
///
/// Defines the placement and naming of a projection directory ("<name>.proj",
/// or "<name>.tmp_proj" while the projection is being materialized) inside its
/// parent part, and gives access to the physical layout of both sides.
class IDataPartProjectionStorage
{
public:
    virtual ~IDataPartProjectionStorage() = default;

    static constexpr std::string_view PROJECTION_DIRECTORY_SUFFIX = ".proj";
    static constexpr std::string_view TEMPORARY_PROJECTION_DIRECTORY_SUFFIX = ".tmp_proj";

    /// "proj_a" -> "proj_a.proj" (or "proj_a.tmp_proj" for a temporary projection).
    static String getDirectoryName(const String & projection_name, bool is_temporary = false)
    {
        return projection_name + String(is_temporary ? TEMPORARY_PROJECTION_DIRECTORY_SUFFIX : PROJECTION_DIRECTORY_SUFFIX);
    }

    static bool isProjectionDirectoryName(std::string_view directory_name)
    {
        return directory_name.ends_with(PROJECTION_DIRECTORY_SUFFIX);
    }

    static bool isTemporaryProjectionDirectoryName(std::string_view directory_name)
    {
        return directory_name.ends_with(TEMPORARY_PROJECTION_DIRECTORY_SUFFIX);
    }

    /// Name of the projection this storage belongs to.
    virtual String getProjectionName() const = 0;

    /// True for a projection which is being materialized ("<name>.tmp_proj").
    virtual bool isTemporary() const = 0;

    /// Physical layout of the projection itself.
    virtual IDataPartStorage & getStorage() = 0;
    virtual const IDataPartStorage & getStorage() const = 0;

    /// Physical layout of the parent part the projection is placed in.
    virtual const IDataPartStorage & getParentStorage() const = 0;
};

using DataPartProjectionStoragePtr = std::unique_ptr<IDataPartProjectionStorage>;

/// On-disk implementation of the projection sub-block: the projection's own storage
/// (rooted at "<parent>/<name>.proj") together with the parent part storage it is placed in.
class DataPartProjectionStorage final : public IDataPartProjectionStorage
{
public:
    DataPartProjectionStorage(
        String projection_name_,
        MutableDataPartStoragePtr storage_,
        DataPartStoragePtr parent_storage_);

    String getProjectionName() const override { return projection_name; }
    bool isTemporary() const override;

    IDataPartStorage & getStorage() override { return *storage; }
    const IDataPartStorage & getStorage() const override { return *storage; }

    const IDataPartStorage & getParentStorage() const override { return *parent_storage; }

private:
    String projection_name;
    MutableDataPartStoragePtr storage;
    DataPartStoragePtr parent_storage;
};

/// Secondary (skip) indexes sub-block of a data part.
///
/// Knows how index substreams are laid out on disk - standalone "skp_idx_*" files or
/// members of the per-part "skp_idx.packed" archive - and answers structural questions
/// about them. A single index can mix both layouts (small substreams stay in the archive,
/// large ones spill into standalone files), so the layout is a per-file property answered
/// by probing, not a per-part implementation split.
class IDataPartIndexStorage
{
public:
    virtual ~IDataPartIndexStorage() = default;

    /// True iff any substream of the index is stored inside the part's
    /// "skp_idx.packed" archive. Probes what the part actually holds, not the
    /// writer's current substream set, so a legacy member inside the archive
    /// on an upgraded part is found.
    virtual bool isInPackedArchive(const IMergeTreeIndex & index) const = 0;

    /// True iff the part physically stores the index. Probes both the current
    /// and the legacy data file extensions (".idx2" and ".idx").
    virtual bool exists(const String & index_name, bool escape_index_filenames) const = 0;
};

using DataPartIndexStoragePtr = std::unique_ptr<IDataPartIndexStorage>;

/// On-disk implementation of the secondary indexes sub-block. Answers structural questions
/// from the part's checksums and its physical storage (including the "skp_idx.packed"
/// archive overlay that lives in DataPartStorageOnDiskBase).
class DataPartIndexStorage final : public IDataPartIndexStorage
{
public:
    explicit DataPartIndexStorage(const IMergeTreeDataPart & part_) : part(part_) {}

    bool isInPackedArchive(const IMergeTreeIndex & index) const override;
    bool exists(const String & index_name, bool escape_index_filenames) const override;

private:
    const IMergeTreeDataPart & part;
};

/// Statistics sub-block of a data part.
///
/// Loads column statistics from their on-disk representation. The representation is a
/// per-part property decided by the part's contents (see createDataPartStatisticsStorage),
/// so each representation gets its own implementation of load.
class IDataPartStatisticsStorage
{
public:
    virtual ~IDataPartStatisticsStorage() = default;

    /// Load statistics for all columns of the part.
    virtual ColumnsStatistics load() const = 0;

    /// Load statistics only for the given columns. Statistics of a parent column are
    /// kept when its ".null" subcolumn is requested (optimize_functions_to_subcolumns).
    virtual ColumnsStatistics load(const Names & required_columns) const = 0;
};

using DataPartStatisticsStoragePtr = std::unique_ptr<IDataPartStatisticsStorage>;

/// Statistics stored as a single "statistics.packed" archive with one virtual
/// "statistics_<col>.stats" member per column (parts written with the packed
/// statistics format on full/regular part storage).
class DataPartStatisticsStoragePacked final : public IDataPartStatisticsStorage
{
public:
    /// Out-of-line: the members require the complete PackedFilesReader type.
    explicit DataPartStatisticsStoragePacked(const IMergeTreeDataPart & part_);
    ~DataPartStatisticsStoragePacked() override;

    ColumnsStatistics load() const override;
    ColumnsStatistics load(const Names & required_columns) const override;

private:
    ColumnsStatistics loadImpl(const NameSet & required_columns) const;

    /// Reader for the archive index, lazily created on first load.
    const PackedFilesReader & getReader() const;

    const IMergeTreeDataPart & part;

    mutable std::mutex reader_mutex;
    mutable std::unique_ptr<PackedFilesReader> reader;
};

/// Statistics stored as one standalone compressed "statistics_<col>.stats" file per
/// column (legacy parts, and packed part storage where the files live inside data.packed).
class DataPartStatisticsStorageWide final : public IDataPartStatisticsStorage
{
public:
    explicit DataPartStatisticsStorageWide(const IMergeTreeDataPart & part_) : part(part_) {}

    ColumnsStatistics load() const override;
    ColumnsStatistics load(const Names & required_columns) const override;

private:
    ColumnsStatistics loadImpl(const NameSet & required_columns) const;

    const IMergeTreeDataPart & part;
};

/// Picks the statistics representation of the part: "statistics.packed" archive when the
/// part's checksums list one (and the part is on on-disk storage), per-column files
/// otherwise. Must be called after the part's checksums are loaded - the decision is
/// made once per part object.
DataPartStatisticsStoragePtr createDataPartStatisticsStorage(const IMergeTreeDataPart & part);

/// A holder that encapsulates data part storage and
/// gives access to const storage from const methods
/// and to mutable storage from non-const methods.
class DataPartStorageHolder : public boost::noncopyable
{
public:
    explicit DataPartStorageHolder(MutableDataPartStoragePtr storage_)
        : storage(std::move(storage_))
    {
    }

    IDataPartStorage & getDataPartStorage() { return *storage; }
    const IDataPartStorage & getDataPartStorage() const { return *storage; }

    MutableDataPartStoragePtr getDataPartStoragePtr() { return storage; }
    DataPartStoragePtr getDataPartStoragePtr() const { return storage; }

private:
    MutableDataPartStoragePtr storage;
};

inline bool isFullPartStorage(const IDataPartStorage & storage)
{
    return storage.getType() == MergeTreeDataPartStorageType::Full;
}

inline bool isPackedPartStorage(const IDataPartStorage & storage)
{
    return storage.getType() == MergeTreeDataPartStorageType::Packed;
}

}
