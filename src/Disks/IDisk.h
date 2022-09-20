#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>
#include <Core/Defines.h>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Disks/Executor.h>
#include <Disks/DiskType.h>
#include <IO/ReadSettings.h>

#include <memory>
#include <mutex>
#include <utility>
#include <boost/noncopyable.hpp>
#include <Poco/Timestamp.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

class IDiskDirectoryIterator;
using DiskDirectoryIteratorPtr = std::unique_ptr<IDiskDirectoryIterator>;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;
using Reservations = std::vector<ReservationPtr>;

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;
class MMappedFileCache;

/**
 * Mode of opening a file for write.
 */
enum class WriteMode
{
    Rewrite,
    Append
};

/**
 * Provide interface for reservation.
 */
class Space : public std::enable_shared_from_this<Space>
{
public:
    /// Return the name of the space object.
    virtual const String & getName() const = 0;

    /// Reserve the specified number of bytes.
    virtual ReservationPtr reserve(UInt64 bytes) = 0;

    virtual ~Space() = default;
};

using SpacePtr = std::shared_ptr<Space>;

/**
 * A guard, that should synchronize file's or directory's state
 * with storage device (e.g. fsync in POSIX) in its destructor.
 */
class ISyncGuard
{
public:
    ISyncGuard() = default;
    virtual ~ISyncGuard() = default;
};

using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

/**
 * A unit of storage persisting data and metadata.
 * Abstract underlying storage technology.
 * Responsible for:
 * - file management;
 * - space accounting and reservation.
 */
class IDisk : public Space
{
public:
    /// Default constructor.
    explicit IDisk(std::unique_ptr<Executor> executor_ = std::make_unique<SyncExecutor>()) : executor(std::move(executor_)) { }

    /// Root path for all files stored on the disk.
    /// It's not required to be a local filesystem path.
    virtual const String & getPath() const = 0;

    /// Total available space on the disk.
    virtual UInt64 getTotalSpace() const = 0;

    /// Space currently available on the disk.
    virtual UInt64 getAvailableSpace() const = 0;

    /// Space available for reservation (available space minus reserved space).
    virtual UInt64 getUnreservedSpace() const = 0;

    /// Amount of bytes which should be kept free on the disk.
    virtual UInt64 getKeepingFreeSpace() const { return 0; }

    /// Return `true` if the specified file exists.
    virtual bool exists(const String & path) const = 0;

    /// Return `true` if the specified file exists and it's a regular file (not a directory or special file type).
    virtual bool isFile(const String & path) const = 0;

    /// Return `true` if the specified file exists and it's a directory.
    virtual bool isDirectory(const String & path) const = 0;

    /// Return size of the specified file.
    virtual size_t getFileSize(const String & path) const = 0;

    /// Create directory.
    virtual void createDirectory(const String & path) = 0;

    /// Create directory and all parent directories if necessary.
    virtual void createDirectories(const String & path) = 0;

    /// Remove all files from the directory. Directories are not removed.
    virtual void clearDirectory(const String & path) = 0;

    /// Move directory from `from_path` to `to_path`.
    virtual void moveDirectory(const String & from_path, const String & to_path) = 0;

    /// Return iterator to the contents of the specified directory.
    virtual DiskDirectoryIteratorPtr iterateDirectory(const String & path) = 0;

    /// Return `true` if the specified directory is empty.
    bool isDirectoryEmpty(const String & path);

    /// Create empty file at `path`.
    virtual void createFile(const String & path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, an exception will be thrown .
    virtual void moveFile(const String & from_path, const String & to_path) = 0;

    /// Move the file from `from_path` to `to_path`.
    /// If a file with `to_path` path already exists, it will be replaced.
    virtual void replaceFile(const String & from_path, const String & to_path) = 0;

    /// Recursively copy data containing at `from_path` to `to_path` located at `to_disk`.
    virtual void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path);

    /// List files at `path` and add their names to `file_names`
    virtual void listFiles(const String & path, std::vector<String> & file_names) = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile( /// NOLINT
        const String & path,
        const ReadSettings & settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    virtual void removeFile(const String & path) = 0;

    /// Remove file if it exists.
    virtual void removeFileIfExists(const String & path) = 0;

    /// Remove directory. Throws exception if it's not a directory or if directory is not empty.
    virtual void removeDirectory(const String & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const String & path) = 0;

    /// Remove file. Throws exception if file doesn't exists or if directory is not empty.
    /// Differs from removeFile for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFile(const String & path, bool) { removeFile(path); }

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    /// Differs from removeRecursive for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedRecursive(const String & path, bool) { removeRecursive(path); }

    /// Remove file or directory if it exists.
    /// Differs from removeFileIfExists for S3/HDFS disks
    /// Second bool param is a flag to remove (true) or keep (false) shared data on S3
    virtual void removeSharedFileIfExists(const String & path, bool) { removeFileIfExists(path); }

    struct RemoveRequest
    {
        String path;
        bool if_exists = false;

        explicit RemoveRequest(String path_, bool if_exists_ = false)
            : path(std::move(path_)), if_exists(std::move(if_exists_))
        {
        }
    };

    using RemoveBatchRequest = std::vector<RemoveRequest>;

    /// Batch request to remove multiple files.
    /// May be much faster for blob storage.
    virtual void removeSharedFiles(const RemoveBatchRequest & files, bool keep_in_remote_fs)
    {
        for (const auto & file : files)
        {
            if (file.if_exists)
                removeSharedFileIfExists(file.path, keep_in_remote_fs);
            else
                removeSharedFile(file.path, keep_in_remote_fs);
        }
    }

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const String & path, const Poco::Timestamp & timestamp) = 0;

    /// Get last modified time of file or directory at `path`.
    virtual Poco::Timestamp getLastModified(const String & path) = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const String & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const String & src_path, const String & dst_path) = 0;

    /// Truncate file to specified size.
    virtual void truncateFile(const String & path, size_t size);

    /// Return disk type - "local", "s3", etc.
    virtual DiskType getType() const = 0;

    /// Involves network interaction.
    virtual bool isRemote() const = 0;

    /// Whether this disk support zero-copy replication.
    /// Overrode in remote fs disks.
    virtual bool supportZeroCopyReplication() const = 0;

    virtual bool isReadOnly() const { return false; }

    /// Check if disk is broken. Broken disks will have 0 space and not be used.
    virtual bool isBroken() const { return false; }

    /// Invoked when Global Context is shutdown.
    virtual void shutdown() {}

    /// Performs action on disk startup.
    virtual void startup() {}

    /// Return some uniq string for file, overrode for IDiskRemote
    /// Required for distinguish different copies of the same part on remote disk
    virtual String getUniqueId(const String & path) const { return path; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote FS disks (s3/hdfs)
    /// Required for remote disk to ensure that replica has access to data written by other node
    virtual bool checkUniqueId(const String & id) const { return exists(id); }

    /// Invoked on partitions freeze query.
    virtual void onFreeze(const String &) { }

    /// Returns guard, that insures synchronization of directory metadata with storage device.
    virtual SyncGuardPtr getDirectorySyncGuard(const String & path) const;

    /// Applies new settings for disk in runtime.
    virtual void applyNewSettings(const Poco::Util::AbstractConfiguration &, ContextPtr, const String &, const DisksMap &) {}

    /// Quite leaky abstraction. Some disks can use additional disk to store
    /// some parts of metadata. In general case we have only one disk itself and
    /// return pointer to it.
    ///
    /// Actually it's a part of IDiskRemote implementation but we have so
    /// complex hierarchy of disks (with decorators), so we cannot even
    /// dynamic_cast some pointer to IDisk to pointer to IDiskRemote.
    virtual std::shared_ptr<IDisk> getMetadataDiskIfExistsOrSelf() { return std::static_pointer_cast<IDisk>(shared_from_this()); }

    /// Very similar case as for getMetadataDiskIfExistsOrSelf(). If disk has "metadata"
    /// it will return mapping for each required path: path -> metadata as string.
    /// Only for IDiskRemote.
    virtual std::unordered_map<String, String> getSerializedMetadata(const std::vector<String> & /* paths */) const { return {}; }

    /// Return reference count for remote FS.
    /// You can ask -- why we have zero and what does it mean? For some unknown reason
    /// the decision was made to take 0 as "no references exist", but only file itself left.
    /// With normal file system we will get 1 in this case:
    /// $ stat clickhouse
    ///  File: clickhouse
    ///  Size: 3014014920      Blocks: 5886760    IO Block: 4096   regular file
    ///  Device: 10301h/66305d   Inode: 3109907     Links: 1
    /// Why we have always zero by default? Because normal filesystem
    /// manages hardlinks by itself. So you can always remove hardlink and all
    /// other alive harlinks will not be removed.
    virtual UInt32 getRefCount(const String &) const { return 0; }


protected:
    friend class DiskDecorator;

    /// Returns executor to perform asynchronous operations.
    virtual Executor & getExecutor() { return *executor; }

    /// Base implementation of the function copy().
    /// It just opens two files, reads data by portions from the first file, and writes it to the second one.
    /// A derived class may override copy() to provide a faster implementation.
    void copyThroughBuffers(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path);

private:
    std::unique_ptr<Executor> executor;
};

using DiskPtr = std::shared_ptr<IDisk>;
using Disks = std::vector<DiskPtr>;

/**
 * Iterator of directory contents on particular disk.
 */
class IDiskDirectoryIterator
{
public:
    /// Iterate to the next file.
    virtual void next() = 0;

    /// Return `true` if the iterator points to a valid element.
    virtual bool isValid() const = 0;

    /// Path to the file that the iterator currently points to.
    virtual String path() const = 0;

    /// Name of the file that the iterator currently points to.
    virtual String name() const = 0;

    virtual ~IDiskDirectoryIterator() = default;
};

/**
 * Information about reserved size on particular disk.
 */
class IReservation : boost::noncopyable
{
public:
    /// Get reservation size.
    virtual UInt64 getSize() const = 0;

    /// Get i-th disk where reservation take place.
    virtual DiskPtr getDisk(size_t i = 0) const = 0; /// NOLINT

    /// Get all disks, used in reservation
    virtual Disks getDisks() const = 0;

    /// Changes amount of reserved space.
    virtual void update(UInt64 new_size) = 0;

    /// Unreserves reserved space.
    virtual ~IReservation() = default;
};

/// Return full path to a file on disk.
inline String fullPath(const DiskPtr & disk, const String & path)
{
    return fs::path(disk->getPath()) / path;
}

/// Return parent path for the specified path.
inline String parentPath(const String & path)
{
    if (path.ends_with('/'))
        return fs::path(path).parent_path().parent_path() / "";
    return fs::path(path).parent_path() / "";
}

/// Return file name for the specified path.
inline String fileName(const String & path)
{
    return fs::path(path).filename();
}

/// Return directory path for the specified path.
inline String directoryPath(const String & path)
{
    return fs::path(path).parent_path() / "";
}

}
