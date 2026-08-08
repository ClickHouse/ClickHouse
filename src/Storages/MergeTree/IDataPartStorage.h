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

class PackedFilesReader;
class PackedFilesWriter;

/// The data-part storage layer is split into a family of abstractions - the part itself, its
/// projections, and its index / statistics / manifest components - which were historically mixed
/// into the single IDataPartStorage. Each abstraction is declared here and implemented by its OWN
/// class at every implementation level (DataPartStorageOnDiskBase / OnDiskFull / OnDiskPacked files),
/// so each owns its disk operations and file-naming/detection knowledge separately.
class IDataPartStorage;
class IDataPartManifestStorage;
class IDataPartIndexStorage;
class IDataPartStatisticsStorage;
class IDataPartProjectionStorage;

using DataPartProjectionStoragePtr = std::shared_ptr<const IDataPartProjectionStorage>;
using MutableDataPartProjectionStoragePtr = std::shared_ptr<IDataPartProjectionStorage>;

/// Access protocol for a data part's manifest - a future per-part inventory of the part's files and
/// components. Declared now as the structural home for that protocol; no manifest exists yet.
class IDataPartManifestStorage
{
public:
    virtual ~IDataPartManifestStorage() = default;

    /// Whether this part currently has a manifest. Always false until manifests are introduced.
    virtual bool hasManifest() const = 0;
};

/// Physical-layer access to a data part's skip-index files, including the optional per-part
/// skp_idx.packed archive that bundles index substreams. Owns where index files live relative to the
/// part (standalone on disk, or as members of an archive) and the archive read/copy/filter operations.
class IDataPartIndexStorage
{
public:
    virtual ~IDataPartIndexStorage() = default;

    /// True iff @name resolves to a substream inside this part's skp_idx.packed archive (and not a
    /// standalone file on disk).
    virtual bool isFileInPackedSkipIndicesArchive(const std::string & name) const = 0;

    /// True iff this part has a packed skip-index archive (skp_idx.packed).
    virtual bool hasSkipIndicesPackedArchive() const = 0;

    /// Copy the named virtual files from this storage's skp_idx.packed archive into @target so the
    /// writer can ship a complete archive after also writing fresh recalc'd entries. Names not present
    /// in the archive are skipped silently.
    virtual void copyPackedSkipIndicesFilesInto(
        const NameSet & file_names,
        PackedFilesWriter & target,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) const = 0;

    /// Rewrite this storage's skp_idx.packed into a fresh archive on @new_storage, dropping any member
    /// whose name is in @dropped_skip_index_archive_file_names; updates @checksums accordingly.
    virtual void filterPackedSkipIndicesArchiveTo(
        const NameSet & dropped_skip_index_archive_file_names,
        IDataPartStorage & new_storage,
        const WriteSettings & write_settings,
        const ReadSettings & read_settings,
        MergeTreeDataPartChecksums & checksums,
        bool sync) const = 0;

    /// Seed this storage's cached archive reader from @source's archive (used when the unchanged
    /// skp_idx.packed is hardlinked from @source into this new part).
    virtual void seedSkipIndicesPackedReaderFrom(const IDataPartStorage & source) const = 0;
};

/// Physical-layer access to a data part's column-statistics files. Owns the statistics file naming,
/// detection, and the packed statistics archive ("statistics.packed") access; the per-column wide
/// layout flows through the part storage's generic read/write primitives.
class IDataPartStatisticsStorage
{
public:
    virtual ~IDataPartStatisticsStorage() = default;

    /// True iff @file_name is a per-column statistics file ("statistics_<column>.stats").
    static bool isStatisticsFile(const std::string & file_name);

    /// Lazily-opened, cached reader over the packed statistics archive. The caller decides whether
    /// the part has the archive (from checksums) before calling; probing the disk here would wrongly
    /// pick up a leftover archive that the part does not own.
    virtual const PackedFilesReader * getPackedStatisticsReader(const ReadSettings & read_settings) const = 0;

    /// Read one member of the packed statistics archive, resolving the archive's current location
    /// (the part may have been renamed or moved since the reader was opened).
    virtual std::unique_ptr<ReadBufferFromFileBase> readPackedStatisticsFile(
        const String & file_name, const ReadSettings & read_settings, size_t file_size) const = 0;
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

    /// Get a storage for projection. A projection is stored exactly like a part, so its storage is a
    /// distinctly-typed IDataPartProjectionStorage that carries the projection naming/identity.
    virtual MutableDataPartProjectionStoragePtr getProjection(const std::string & name, bool use_parent_transaction = true) = 0; // NOLINT

    virtual MutableDataPartProjectionStoragePtr getProjectionNoInitialize(const std::string & name, bool use_parent_transaction = true) // NOLINT
    {
        return getProjection(name, use_parent_transaction);
    }

    virtual DataPartProjectionStoragePtr getProjection(const std::string & name) const = 0;

    /// Component storages of this part: separate objects, each owning its component's disk
    /// operations and file naming/detection over this storage's files. May be null for a storage
    /// that does not implement the component (callers guard, mirroring the concrete-storage casts
    /// these accessors replace).
    virtual IDataPartManifestStorage * getManifestStorage() { return nullptr; }
    virtual const IDataPartManifestStorage * getManifestStorage() const { return nullptr; }
    virtual IDataPartIndexStorage * getIndexStorage() { return nullptr; }
    virtual const IDataPartIndexStorage * getIndexStorage() const { return nullptr; }
    virtual IDataPartStatisticsStorage * getStatisticsStorage() { return nullptr; }
    virtual const IDataPartStatisticsStorage * getStatisticsStorage() const { return nullptr; }

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

    /// Remove data part.
    /// can_remove_shared_data, names_not_to_remove are specific for DiskObjectStorage.
    /// projections, checksums are needed to avoid recursive listing
    virtual void remove(
        CanRemoveCallback && can_remove_callback,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        bool is_temp,
        LoggerPtr log) = 0;

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

    /// Create a backup of a data part.
    /// This method adds a new entry to backup_entries.
    /// Also creates a new tmp_dir for internal disk (if disk is mentioned the first time).
    using TemporaryFilesOnDisks = std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>>;
    virtual void backup(
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        const BackupSettings & backup_settings,
        bool make_temporary_hard_links,
        BackupEntries & backup_entries,
        TemporaryFilesOnDisks * temp_dirs,
        bool is_projection_part,
        bool allow_backup_broken_projection) const = 0;

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

    /// Rename part.
    /// Ideally, new_root_path should be the same as current root (but it is not true).
    /// Examples are: 'all_1_2_1' -> 'detached/all_1_2_1'
    ///               'moving/tmp_all_1_2_1' -> 'all_1_2_1'
    virtual void rename(
        std::string new_root_path,
        std::string new_part_dir,
        LoggerPtr log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) = 0;

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

/// Storage for a projection sub-part. A projection is physically stored just like a part, so this
/// is-a IDataPartStorage; the extra surface is the projection identity (its name and the ".proj" /
/// ".tmp_proj" directory naming) that used to be reconstructed by callers from the part directory.
/// IDataPartStorage is a virtual base so the concrete projection storages can join this interface
/// with the concrete part-storage implementations without duplicating the storage subobject.
class IDataPartProjectionStorage : public virtual IDataPartStorage
{
public:
    /// Projection name: the part directory without its projection suffix.
    std::string getProjectionName() const;

    /// True iff this storage's directory is a projection directory ("<name>.proj" / "<name>.tmp_proj").
    bool isProjection() const;

    /// Directory name of a projection: "<name>.proj", or "<name>.tmp_proj" while it is being built.
    static std::string getProjectionDirectoryName(const std::string & projection_name, bool is_temp);

    /// True iff @directory_name is a projection directory ("*.proj" or "*.tmp_proj").
    static bool isProjectionDirectoryName(const std::string & directory_name);

    static constexpr auto PROJECTION_DIRECTORY_SUFFIX = ".proj";
    static constexpr auto TEMP_PROJECTION_DIRECTORY_SUFFIX = ".tmp_proj";
};

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
