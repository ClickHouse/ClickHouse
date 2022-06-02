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

/// This is an abstraction of storage for data part files.
/// Generally, it contains read-only methods from IDisk.
class IDataPartStorage
{
private:
public:
    virtual ~IDataPartStorage() = default;

    /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const = 0;

    virtual bool exists() const = 0;
    virtual bool exists(const std::string & path) const = 0;
    virtual bool isDirectory(const std::string & path) const = 0;

    virtual Poco::Timestamp getLastModified() const = 0;

    virtual DataPartStorageIteratorPtr iterate() const = 0;
    virtual DataPartStorageIteratorPtr iterateDirectory(const std::string & path) const = 0;

    struct ProjectionChecksums
    {
        const std::string & name;
        const MergeTreeDataPartChecksums & checksums;
    };

    virtual void remove(
        bool can_remove_shared_data,
        const NameSet & names_not_to_remove,
        const MergeTreeDataPartChecksums & checksums,
        std::list<ProjectionChecksums> projections,
        Poco::Logger * log) const = 0;

    virtual size_t getFileSize(const std::string & path) const = 0;
    virtual UInt32 getRefCount(const String &) const { return 0; }

    virtual std::string getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached) const = 0;

    /// Reset part directory, used for im-memory parts
    virtual void setRelativePath(const std::string & path) = 0;

    virtual std::string getPartDirectory() const = 0;
    // virtual std::string getRootPath() const = 0;
    // std::string getPath() const; // getRootPath() / getPartDirectory()
    //
    // virtual std::string getFullRootPath() const = 0;
    // virtual std::string getFullPath() const = 0; // getFullRootPath() / getPartDirectory()

    virtual std::string getFullPath() const = 0;
    virtual std::string getFullRootPath() const = 0;
    virtual std::string getFullRelativePath() const = 0; // -> getPath

    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    virtual bool isStoredOnRemoteDisk() const { return false; }
    virtual bool supportZeroCopyReplication() const { return false; }
    virtual bool supportParallelWrite() const = 0;
    virtual bool isBroken() const = 0;
    virtual std::string getDiskPathForLogs() const = 0;

    /// Should remove it later
    virtual void writeChecksums(const MergeTreeDataPartChecksums & checksums, const WriteSettings & settings) const = 0;
    virtual void writeColumns(const NamesAndTypesList & columns, const WriteSettings & settings) const = 0;
    virtual void writeVersionMetadata(const VersionMetadata & version, bool fsync_part_dir) const = 0;
    virtual void appendCSNToVersionMetadata(const VersionMetadata & version, VersionMetadata::WhichCSN which_csn) const = 0;
    virtual void appendRemovalTIDToVersionMetadata(const VersionMetadata & version, bool clear) const = 0;
    virtual void writeDeleteOnDestroyMarker(Poco::Logger * log) const = 0;

    virtual void removeDeleteOnDestroyMarker() const = 0;
    virtual void removeVersionMetadata() const = 0;

    virtual void loadVersionMetadata(VersionMetadata & version, Poco::Logger * log) const = 0;

    virtual void checkConsistency(const MergeTreeDataPartChecksums & checksums) const = 0;

    virtual ReservationPtr reserve(UInt64 /*bytes*/) const { return nullptr; }
    virtual ReservationPtr tryReserve(UInt64 /*bytes*/) const { return nullptr; }
    virtual size_t getVolumeIndex(const IStoragePolicy &) const { return 0; }

    /// A leak of abstraction.
    /// Return some uniq string for file.
    /// Required for distinguish different copies of the same part on remote FS.
    virtual String getUniqueId() const = 0;

    /// A leak of abstraction
    virtual bool shallParticipateInMerges(const IStoragePolicy &) const { return true; }

    /// A leak of abstraction
    using TemporaryFilesOnDisks = std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>>;
    virtual void backup(
        TemporaryFilesOnDisks & temp_dirs,
        const MergeTreeDataPartChecksums & checksums,
        const NameSet & files_without_checksums,
        BackupEntries & backup_entries) const = 0;

    /// A leak of abstraction
    virtual std::shared_ptr<IDataPartStorage> freeze(
        const std::string & to,
        const std::string & dir_path,
        bool make_source_readonly,
        std::function<void(const DiskPtr &)> save_metadata_callback,
        bool copy_instead_of_hardlink) const = 0;

    virtual std::shared_ptr<IDataPartStorage> clone(
        const std::string & to,
        const std::string & dir_path,
        Poco::Logger * log) const = 0;

    virtual void rename(const std::string & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync_part_dir) = 0;

    /// Change part's root. From should be a prefix path of current root path.
    /// Right now, this is needed for rename table query.
    virtual void changeRootPath(const std::string & from_root, const std::string & to_root) = 0;

    /// Disk name
    virtual std::string getName() const = 0;
    virtual std::string getDiskType() const = 0;

    using DisksSet = std::unordered_set<DiskPtr>;
    virtual DisksSet::const_iterator isStoredOnDisk(const DisksSet & disks) const { return disks.end(); }

    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name) const = 0;
};

using DataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class IDataPartStorageBuilder
{
public:
    virtual ~IDataPartStorageBuilder() = default;

    /// Reset part directory, used for im-memory parts
    virtual void setRelativePath(const std::string & path) = 0;

    virtual std::string getPartDirectory() const = 0;
    virtual std::string getFullPath() const = 0;
    virtual std::string getFullRelativePath() const = 0;

    virtual bool exists() const = 0;
    virtual bool exists(const std::string & path) const = 0;

    virtual void createDirectories() = 0;
    virtual void createProjection(const std::string & name) = 0;

    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const = 0;

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, const WriteSettings & settings) = 0;

    virtual void removeFile(const String & path) = 0;
    virtual void removeRecursive() = 0;
    virtual void removeSharedRecursive(bool keep_in_remote_fs) = 0;

    virtual SyncGuardPtr getDirectorySyncGuard() const { return nullptr; }

    virtual void createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to) const = 0;

    virtual ReservationPtr reserve(UInt64 /*bytes*/) { return nullptr; }

    virtual std::shared_ptr<IDataPartStorageBuilder> getProjection(const std::string & name) const = 0;

    virtual DataPartStoragePtr getStorage() const = 0;
};

using DataPartStorageBuilderPtr = std::shared_ptr<IDataPartStorageBuilder>;

}
