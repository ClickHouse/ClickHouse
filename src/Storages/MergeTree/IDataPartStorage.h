#pragma once
#include <IO/ReadSettings.h>
#include <base/types.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <optional>

namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;


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

    virtual ~IDataPartStorageIterator() = default;
};

using DataPartStorageIteratorPtr = std::unique_ptr<IDataPartStorageIterator>;

struct MergeTreeDataPartChecksums;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IStoragePolicy;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class ISyncGuard;
using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;

struct WriteSettings;

class TemporaryFileOnDisk;

class IDataPartStorageBuilder;
using DataPartStorageBuilderPtr = std::shared_ptr<IDataPartStorageBuilder>;

/// This is an abstraction of storage for data part files.
/// Ideally, it is assumed to contains read-only methods from IDisk.
/// It is not fulfilled now, but let's try our best.
class IDataPartStorage
{
public:
    virtual ~IDataPartStorage() = default;

    /// Methods to get path components of a data part.
    virtual std::string getFullPath() const = 0;      /// '/var/lib/clickhouse/data/database/table/moving/all_1_5_1'
    virtual std::string getRelativePath() const = 0;  ///                          'database/table/moving/all_1_5_1'
    virtual std::string getPartDirectory() const = 0; ///                                                'all_1_5_1'
    virtual std::string getFullRootPath() const = 0;  /// '/var/lib/clickhouse/data/database/table/moving'
    /// Can add it if needed                          ///                          'database/table/moving'
    /// virtual std::string getRelativeRootPath() const = 0;

    /// Get a storage for projection.
    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name) const = 0;

    /// Part directory exists.
    virtual bool exists() const = 0;
    /// File inside part directory exists. Specified path is relative to the part path.
    virtual bool exists(const std::string & name) const = 0;
    virtual bool isDirectory(const std::string & name) const = 0;

    /// Modification time for part directory.
    virtual Poco::Timestamp getLastModified() const = 0;
    /// Iterate part directory. Iteration in subdirectory is not needed yet.
    virtual DataPartStorageIteratorPtr iterate() const = 0;

    /// Get metadata for a file inside path dir.
    virtual size_t getFileSize(const std::string & file_name) const = 0;
    virtual UInt32 getRefCount(const std::string & file_name) const = 0;

    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const = 0;

    virtual void loadVersionMetadata(VersionMetadata & version, Poco::Logger * log) const = 0;
    virtual void checkConsistency(const MergeTreeDataPartChecksums & checksums) const = 0;

    struct ProjectionChecksums
    {
        const std::string & name;
        const MergeTreeDataPartChecksums & checksums;
    };

    /// Remove data part.
    /// can_remove_shared_data, names_not_to_remove are specific for DiskObjectStorage.
    /// projections, checksums are needed to avoid recursive listing
    virtual void remove(
        bool can_remove_shared_data,
        const NameSet & names_not_to_remove,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        Poco::Logger * log) const = 0;

    /// Get a name like 'prefix_partdir_tryN' which does not exist in a root dir.
    /// TODO: remove it.
    virtual std::string getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached) const = 0;

    /// Reset part directory, used for im-memory parts.
    /// TODO: remove it.
    virtual void setRelativePath(const std::string & path) = 0;
    virtual void onRename(const std::string & new_root_path, const std::string & new_part_dir) = 0;

    /// Some methods from IDisk. Needed to avoid getting internal IDisk interface.
    virtual std::string getDiskName() const = 0;
    virtual std::string getDiskType() const = 0;
    virtual bool isStoredOnRemoteDisk() const { return false; }
    virtual bool supportZeroCopyReplication() const { return false; }
    virtual bool supportParallelWrite() const = 0;
    virtual bool isBroken() const = 0;
    virtual void syncRevision(UInt64 revision) = 0;
    virtual UInt64 getRevision() const = 0;
    virtual std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & paths) const = 0;
    /// Get a path for internal disk if relevant. It is used mainly for logging.
    virtual std::string getDiskPath() const = 0;

    /// Check if data part is stored on one of the specified disk in set.
    using DisksSet = std::unordered_set<DiskPtr>;
    virtual DisksSet::const_iterator isStoredOnDisk(const DisksSet & disks) const { return disks.end(); }

    /// Reserve space on the same disk.
    /// Probably we should try to remove it later.
    virtual ReservationPtr reserve(UInt64 /*bytes*/) const { return nullptr; }
    virtual ReservationPtr tryReserve(UInt64 /*bytes*/) const { return nullptr; }
    virtual size_t getVolumeIndex(const IStoragePolicy &) const { return 0; }

    /// Some methods which change data part internals possibly after creation.
    /// Probably we should try to remove it later.
    virtual void writeChecksums(const MergeTreeDataPartChecksums & checksums, const WriteSettings & settings) const = 0;
    virtual void writeColumns(const NamesAndTypesList & columns, const WriteSettings & settings) const = 0;
    virtual void writeVersionMetadata(const VersionMetadata & version, bool fsync_part_dir) const = 0;
    virtual void appendCSNToVersionMetadata(const VersionMetadata & version, VersionMetadata::WhichCSN which_csn) const = 0;
    virtual void appendRemovalTIDToVersionMetadata(const VersionMetadata & version, bool clear) const = 0;
    virtual void writeDeleteOnDestroyMarker(Poco::Logger * log) const = 0;
    virtual void removeDeleteOnDestroyMarker() const = 0;
    virtual void removeVersionMetadata() const = 0;

    /// A leak of abstraction.
    /// Return some uniq string for file.
    /// Required for distinguish different copies of the same part on remote FS.
    virtual String getUniqueId() const = 0;

    /// A leak of abstraction
    virtual bool shallParticipateInMerges(const IStoragePolicy &) const { return true; }

    /// Create a backup of a data part.
    /// This method adds a new entry to backup_entries.
    /// Also creates a new tmp_dir for internal disk (if disk is mentioned the first time).
    using TemporaryFilesOnDisks = std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>>;
    virtual void backup(
        TemporaryFilesOnDisks & temp_dirs,
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        const String & path_in_backup,
        BackupEntries & backup_entries) const = 0;

    /// Creates hardlinks into 'to/dir_path' for every file in data part.
    /// Callback is called after hardlinks are created, but before 'delete-on-destroy.txt' marker is removed.
    virtual std::shared_ptr<IDataPartStorage> freeze(
        const std::string & to,
        const std::string & dir_path,
        bool make_source_readonly,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        bool copy_instead_of_hardlink) const = 0;

    /// Make a full copy of a data part into 'to/dir_path' (possibly to a different disk).
    virtual std::shared_ptr<IDataPartStorage> clone(
        const std::string & to,
        const std::string & dir_path,
        const DiskPtr & disk,
        Poco::Logger * log) const = 0;

    /// Change part's root. from_root should be a prefix path of current root path.
    /// Right now, this is needed for rename table query.
    virtual void changeRootPath(const std::string & from_root, const std::string & to_root) = 0;

    /// Leak of abstraction as well. We should use builder as one-time object which allow
    /// us to build parts, while storage should be read-only method to access part properties
    /// related to disk. However our code is really tricky and sometimes we need ad-hoc builders.
    virtual DataPartStorageBuilderPtr getBuilder() const = 0;
};

using DataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// This interface is needed to write data part.
class IDataPartStorageBuilder
{
public:
    virtual ~IDataPartStorageBuilder() = default;

    /// Reset part directory, used for im-memory parts
    virtual void setRelativePath(const std::string & path) = 0;

    virtual std::string getPartDirectory() const = 0;
    virtual std::string getFullPath() const = 0;
    virtual std::string getRelativePath() const = 0;

    virtual bool exists() const = 0;

    virtual void createDirectories() = 0;
    virtual void createProjection(const std::string & name) = 0;

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & name, size_t buf_size, const WriteSettings & settings) = 0;

    virtual void removeFile(const String & name) = 0;
    virtual void removeFileIfExists(const String & name) = 0;
    virtual void removeRecursive() = 0;
    virtual void removeSharedRecursive(bool keep_in_remote_fs) = 0;

    virtual SyncGuardPtr getDirectorySyncGuard() const { return nullptr; }

    virtual void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) const = 0;

    virtual ReservationPtr reserve(UInt64 /*bytes*/) { return nullptr; }

    virtual std::shared_ptr<IDataPartStorageBuilder> getProjection(const std::string & name) const = 0;

    virtual DataPartStoragePtr getStorage() const = 0;

    /// Rename part.
    /// Ideally, new_root_path should be the same as current root (but it is not true).
    /// Examples are: 'all_1_2_1' -> 'detached/all_1_2_1'
    ///               'moving/tmp_all_1_2_1' -> 'all_1_2_1'
    ///
    /// To notify storage also call onRename for it with first two args
    virtual void rename(
        const std::string & new_root_path,
        const std::string & new_part_dir,
        Poco::Logger * log,
        bool remove_new_dir_if_exists,
        bool fsync_part_dir) = 0;

    virtual void commit() = 0;
};

}
