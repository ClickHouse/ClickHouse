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

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string_view>

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
///  Commit-last invariant for FLAT projection siblings: materializing writes projections first and the main part dir last (its presence
/// marks completeness); dismantling removes it first.
class IDataPartStorage : public boost::noncopyable
{
public:
    virtual ~IDataPartStorage() = default;

    virtual MergeTreeDataPartStorageType getType() const = 0;

    /// Placement of one projection sub-part under its parent storage: paths derive from the parent live (survives renames), the location
    /// may not exist on disk, must not outlive the parent.
    struct Projection
    {
        /// Naming vocabulary for projection dirs; every scanner classifies names through it, nothing else spells the pattern.
        enum class Status : uint8_t
        {
            None,
            Live,
            Temp,
        };

        /// On-disk layout: LEGACY_NESTED = <root>/<part_dir>/<name>.proj, FLAT = <root>/<part_dir>.<name>.proj. Distinct from the
        /// user-facing setting enum DB::ProjectionStorageFormat; NONE = not configured yet.
        enum class StorageFormat : uint8_t
        {
            NONE,
            LEGACY_NESTED,
            FLAT,
        };

        const IDataPartStorage * parent = nullptr;
        String name;                    /// bare logical name: "p", "p_1" -- no extension
        StorageFormat format = StorageFormat::NONE;
        bool is_temp = false;

        /// "p.proj" / "p_1.tmp_proj" -- the logical key used across the codebase.
        String dirName() const { return dirName(name, is_temp); }
        /// Root the dir lives in, relative to the disk (differs between NESTED and FLAT).
        String rootPath() const;
        /// rootPath()/physical dir name (FLAT physical name is "<parent_dir>.<dirName()>").
        String relativePath() const;
        /// Live disk probe via the parent.
        bool exists() const;

        static String ext() { return ".proj"; }
        static String extTmp() { return ".tmp_proj"; }
        static String dirName(const String & name_, bool is_temp_) { return name_ + (is_temp_ ? extTmp() : ext()); }

        /// Classifies a directory basename: "*.proj" -> Live, "*.tmp_proj" -> Temp, else None.
        static Status dirNameType(std::string_view dir_name);

        /// Owner part dir of a projection dir ("" if not one). Call as ("<root>", "<owner>.<name>.proj") for FLAT or ("<root>/<owner>",
        /// "<name>.proj") for NESTED; told apart by dots, a dotted NESTED name reads as FLAT.
        static String owner(const std::string & root, std::string_view dir_name);
    };

    /// Key: dirName() -- the same string stored in checksums and passed to getProjectionStorage.
    using Projections = std::map<String, Projection, std::less<>>;

    /// Compatibility alias: the enum lives on Projection so both the descriptor and the storage share it.
    using ProjectionStorageFormat = Projection::StorageFormat;

    /// Methods to get path components of a data part.
    virtual std::string getFullPath() const = 0;         /// '/var/lib/clickhouse/data/database/table/moving/all_1_5_1'
    virtual std::string getRelativePath() const = 0;     ///                          'database/table/moving/all_1_5_1'
    virtual std::string getPartDirectory() const = 0;    ///                                                'all_1_5_1'
    virtual std::string getFullRootPath() const = 0;     /// '/var/lib/clickhouse/data/database/table/moving'
    virtual std::string getParentDirectory() const = 0;  ///                                                '' (or 'detached' for 'detached/all_1_5_1')
    /// Can add it if needed                             ///                          'database/table/moving'
    /// virtual std::string getRelativeRootPath() const = 0;

    /// The owned projection set, seeded by the logical layer and kept true by the dir-mutating verbs. Throws LOGICAL_ERROR if never seeded:
    /// a disk scan could adopt residue of a same-named part.
    virtual Projections getProjections() const = 0;

    /// Atomically replace the owned set (entries are re-parented to this storage); {} is a valid set.
    virtual void setProjections(Projections projections) = 0;

    /// Raw scan of the on-disk projection dirs across both layouts, residue included; does not touch the owned set. For disk-truth paths
    /// only (checksums reconstruction, part consistency checks).
    virtual Projections detectProjections() const = 0;

    /// Same, but the parts-root listing was already taken by the caller (an operation processing many parts scans the directory once and
    /// reuses it); only the nested per-part-dir half is scanned here.
    virtual Projections detectProjections(const Strings & root_dir_entries) const = 0;

    /// Resolve only the given dirName() candidates by direct probes -- no listing, O(candidates) stats. Manifest-driven paths:
    /// checksums+metadata name every adoptable dir, so probing loses nothing.
    virtual Projections probeProjections(const Strings & candidate_dir_names) const = 0;

    /// Owned-set membership by dirName().
    virtual bool hasProjection(const std::string & dir_name) const = 0;

    /// Owned descriptor; throws LOGICAL_ERROR if unknown.
    virtual Projection getProjection(const std::string & dir_name) const = 0;
    virtual std::optional<Projection> tryGetProjection(const std::string & dir_name) const = 0;

    /// "Where would it live": descriptor for a not-owned name via the configured format; registers nothing.
    virtual Projection projectionPlacement(const std::string & dir_name) const = 0;

    /// {disk-relative root, physical dir basename} of a (hypothetical) projection dir; the layout arithmetic behind Projection's accessors.
    /// Absolute path for logging = getDiskPath()/root/dir.
    virtual std::pair<std::string, std::string> getProjectionRootAndDir(const std::string & dir_name, ProjectionStorageFormat format) const = 0;
    virtual bool existsProjectionDir(const std::string & dir_name, ProjectionStorageFormat format) const = 0;

    /// Layout used when this storage creates a projection directory
    virtual ProjectionStorageFormat getProjectionStorageFormat() const = 0;

    /// Configure the layout for projection directories this storage will create
    virtual void setProjectionStorageFormat(ProjectionStorageFormat format) = 0;

    /// Whether the table runs zero-copy replication, i.e. blobs may be shared cross-replica invisibly to the
    /// local refcount. Default true = fail-safe: residue sweeps then keep remote blobs. Seeded by the part builder.
    virtual bool isZeroCopyReplicationEnabled() const = 0;
    virtual void setZeroCopyReplicationEnabled(bool value) = 0;

    /// Sub-part storage bound to the projection's directory.
    virtual std::shared_ptr<IDataPartStorage> getProjectionStorage(const std::string & dir_name, bool use_parent_transaction = true) = 0; // NOLINT
    virtual std::shared_ptr<const IDataPartStorage> getProjectionStorage(const std::string & dir_name) const = 0;

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
    /// `path_in_backup` is the full destination dir of THIS storage; the caller names it (a projection goes under its logical
    /// "<name>.proj"), so the backup layout is independent of the on-disk layout.
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
    };

    /// Materializes a copy of the part at 'to/dir_path' (FLAT projection siblings first, main dir last). `dst_disk` == nullptr means the
    /// part's own disk; a different disk cannot hardlink, so it always copies.
    virtual std::shared_ptr<IDataPartStorage> freeze(
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

    /// The only three operations that mutate projection directories; each keeps the owned set true.

    /// Creates the on-disk directory for a projection sub-part and records it in the owned set. Sweeps a stale leftover directory at the
    /// target first (tmp part names repeat across attempts).
    virtual Projection createProjection(const std::string & dir_name) = 0;

    /// Only a temporary projection may be removed alone; a normal one goes only with its parent part.
    virtual void removeProjection(const Projection & projection) = 0;

    /// Rename within this part (e.g. "p_1.tmp_proj" -> "p.proj"); layout kept, returns the new descriptor.
    /// `fsync` makes the rename entry durable (syncs the enclosing directory).
    virtual Projection renameProjection(const Projection & projection, const std::string & new_dir_name, bool fsync) = 0;

    /// Repoints a projection sub-part's storage at where this part's owned set says the projection lives now (used after the part or the
    /// projection dir was renamed).
    virtual void syncProjectionStoragePath(const Projection & projection, IDataPartStorage & projection_storage) const = 0;

    /// If a dir exists at the placement, remove it with a log line: residue of a failed operation on a same-named
    /// part. Remote blobs are kept only when zero-copy replication may share them invisibly to the local refcount.
    virtual void removeProjectionResidue(const Projection & placement) = 0;

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
    /// FLAT projection siblings move with the part dir; rename picks the parent/sibling order from the destination (parent last when
    /// entering the live namespace, first when leaving it).
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

inline String IDataPartStorage::Projection::rootPath() const
{
    return parent->getProjectionRootAndDir(dirName(), format).first;
}

inline String IDataPartStorage::Projection::relativePath() const
{
    auto [root, dir] = parent->getProjectionRootAndDir(dirName(), format);
    return std::filesystem::path(root) / dir;
}

inline bool IDataPartStorage::Projection::exists() const
{
    return parent->existsProjectionDir(dirName(), format);
}

}
