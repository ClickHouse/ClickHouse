#pragma once
#include <IO/ReadSettings.h>
#include <base/types.h>
#include <Core/NamesAndTypes.h>
#include <optional>

namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;


class IDiskDirectoryIterator;
using DiskDirectoryIteratorPtr = std::unique_ptr<IDiskDirectoryIterator>;

struct MergeTreeDataPartChecksums;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IStoragePolicy;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class IBackupEntry;
using BackupEntryPtr = std::unique_ptr<IBackupEntry>;
using BackupEntries = std::vector<std::pair<std::string, BackupEntryPtr>>;

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

    virtual bool exists(const std::string & path) const = 0;
    virtual bool exists() const = 0;

    virtual Poco::Timestamp getLastModified() const = 0;

    virtual DiskDirectoryIteratorPtr iterate() const = 0;
    virtual DiskDirectoryIteratorPtr iterateDirectory(const std::string & path) const = 0;

    struct ProjectionChecksums
    {
        const std::string & name;
        const MergeTreeDataPartChecksums & checksums;
    };

    virtual void remove(
        bool keep_shared_data,
        const MergeTreeDataPartChecksums & checksums, 
        std::list<ProjectionChecksums> projections,
        Poco::Logger * log) const = 0;

    virtual size_t getFileSize(const std::string & path) const = 0;

    virtual std::string getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached) const = 0;

    /// Reset part directory, used for im-memory parts
    virtual void setRelativePath(const std::string & path) = 0;

    virtual std::string getRelativePath() const = 0;
    virtual std::string getFullPath() const = 0;
    virtual std::string getFullRootPath() const = 0;
    virtual std::string getFullRelativePath() const = 0;

    virtual UInt64 calculateTotalSizeOnDisk() const = 0;

    virtual bool isStoredOnRemoteDisk() const { return false; }
    virtual bool supportZeroCopyReplication() const { return false; }
    virtual bool isBroken() const = 0;
    virtual std::string getDiskPathForLogs() const = 0;

    /// Should remove it later
    virtual void writeChecksums(MergeTreeDataPartChecksums & checksums) const = 0;
    virtual void writeColumns(NamesAndTypesList & columns) const = 0;
    virtual void writeDeleteOnDestroyMarker(Poco::Logger * log) const = 0;

    virtual void checkConsistency(const MergeTreeDataPartChecksums & checksums) const = 0;

    virtual ReservationPtr reserve(UInt64 /*bytes*/) { return nullptr; }

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
        std::function<void(const DiskPtr &)> save_metadata_callback) const = 0;

    virtual std::shared_ptr<IDataPartStorage> clone(
        const std::string & to,
        const std::string & dir_path,
        Poco::Logger * log) const = 0;

    virtual void rename(const String & new_relative_path, Poco::Logger * log, bool remove_new_dir_if_exists, bool fsync);

    /// Disk name
    virtual std::string getName() const = 0;

    virtual std::shared_ptr<IDataPartStorage> getProjection(const std::string & name) const = 0;
};

using DataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class IDataPartStorageBuilder
{
public:
    virtual ~IDataPartStorageBuilder() = default;

    /// Reset part directory, used for im-memory parts
    virtual void setRelativePath(const std::string & path) = 0;

    virtual std::string getRelativePath() const = 0;
    virtual std::string getFullPath() const = 0;

    virtual bool exists() const = 0;
    virtual bool exists(const std::string & path) const = 0;

    virtual void createDirectories() = 0;

    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const std::string & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const = 0;

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size /* = DBMS_DEFAULT_BUFFER_SIZE*/) = 0;

    virtual void removeFile(const String & path) = 0;
    virtual void removeRecursive() = 0;

    virtual ReservationPtr reserve(UInt64 /*bytes*/) { return nullptr; }

    virtual std::shared_ptr<IDataPartStorageBuilder> getProjection(const std::string & name) const = 0;

    virtual DataPartStoragePtr getStorage() const = 0;
};

using DataPartStorageBuilderPtr = std::shared_ptr<IDataPartStorageBuilder>;

}
