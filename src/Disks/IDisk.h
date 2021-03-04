#pragma once

#include <Core/Defines.h>
#include <common/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Disks/Executor.h>

#include <memory>
#include <mutex>
#include <utility>
#include <boost/noncopyable.hpp>
#include <Poco/Path.h>
#include <Poco/Timestamp.h>


namespace CurrentMetrics
{
extern const Metric DiskSpaceReservedForMerge;
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

    /// Copy the file from `from_path` to `to_path`.
    virtual void copyFile(const String & from_path, const String & to_path) = 0;

    /// Recursively copy data containing at `from_path` to `to_path` located at `to_disk`.
    virtual void copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path);

    /// List files at `path` and add their names to `file_names`
    virtual void listFiles(const String & path, std::vector<String> & file_names) = 0;

    /// Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        size_t estimated_size = 0,
        size_t aio_threshold = 0,
        size_t mmap_threshold = 0) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        size_t estimated_size = 0,
        size_t aio_threshold = 0) = 0;

    /// Remove file or directory. Throws exception if file doesn't exists or if directory is not empty.
    virtual void remove(const String & path) = 0;

    /// Remove file or directory with all children. Use with extra caution. Throws exception if file doesn't exists.
    virtual void removeRecursive(const String & path) = 0;

    /// Remove file or directory if it exists.
    void removeIfExists(const String & path)
    {
        if (exists(path))
            remove(path);
    }

    /// Set last modified time to file or directory at `path`.
    virtual void setLastModified(const String & path, const Poco::Timestamp & timestamp) = 0;

    /// Get last modified time of file or directory at `path`.
    virtual Poco::Timestamp getLastModified(const String & path) = 0;

    /// Set file at `path` as read-only.
    virtual void setReadOnly(const String & path) = 0;

    /// Create hardlink from `src_path` to `dst_path`.
    virtual void createHardLink(const String & src_path, const String & dst_path) = 0;

    /// Wrapper for POSIX open
    virtual int open(const String & path, mode_t mode) const = 0;

    /// Wrapper for POSIX close
    virtual void close(int fd) const = 0;

    /// Wrapper for POSIX fsync
    virtual void sync(int fd) const = 0;

    /// Truncate file to specified size.
    virtual void truncateFile(const String & path, size_t size);

    /// Return disk type - "local", "s3", etc.
    virtual const String getType() const = 0;

    /// Invoked when Global Context is shutdown.
    virtual void shutdown() { }

    /// Returns executor to perform asynchronous operations.
    virtual Executor & getExecutor() { return *executor; }

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
    virtual DiskPtr getDisk(size_t i = 0) const = 0;

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
    return disk->getPath() + path;
}

/// Return parent path for the specified path.
inline String parentPath(const String & path)
{
    return Poco::Path(path).parent().toString();
}

/// Return file name for the specified path.
inline String fileName(const String & path)
{
    return Poco::Path(path).getFileName();
}
}
