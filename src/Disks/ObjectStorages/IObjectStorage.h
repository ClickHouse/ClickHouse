#pragma once

#include <filesystem>
#include <string>
#include <map>
#include <optional>

#include <Poco/Timestamp.h>
#include <Core/Defines.h>
#include <Common/Exception.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Common/ThreadPool.h>
#include <Common/IFileCache.h>
#include <Disks/WriteMode.h>


namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

using ObjectAttributes = std::map<std::string, std::string>;

/// Path to a file (always absolute) and its size.
struct PathWithSize
{
    std::string path;
    uint64_t bytes_size;

    PathWithSize() = default;

    PathWithSize(const std::string & path_, uint64_t bytes_size_)
        : path(path_)
        , bytes_size(bytes_size_)
    {}
};

/// List of paths with their sizes
using PathsWithSize = std::vector<PathWithSize>;
using RelativePathsWithSize = PathsWithSize;

struct ObjectMetadata
{
    uint64_t size_bytes;
    std::optional<Poco::Timestamp> last_modified;
    std::optional<ObjectAttributes> attributes;
};

using FinalizeCallback = std::function<void(size_t bytes_count)>;

/// Base class for all object storages which implement some subset of ordinary filesystem operations.
///
/// Examples of object storages are S3, Azure Blob Storage, HDFS.
class IObjectStorage
{
public:
    explicit IObjectStorage(FileCachePtr && cache_)
        : cache(std::move(cache_))
    {}

    /// Path exists or not
    virtual bool exists(const std::string & path) const = 0;

    /// List on prefix, return children (relative paths) with their sizes.
    virtual void listPrefix(const std::string & path, RelativePathsWithSize & children) const = 0;

    /// Get object metadata if supported. It should be possible to receive
    /// at least size of object
    virtual ObjectMetadata getObjectMetadata(const std::string & path) const = 0;

    /// Read single path from object storage
    virtual std::unique_ptr<SeekableReadBuffer> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Read multiple objects with common prefix
    virtual std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const PathsWithSize & paths_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) = 0;

    /// Remove object. Throws exception if object doesn't exists.
    virtual void removeObject(const std::string & path) = 0;

    /// Remove multiple objects. Some object storages can do batch remove in a more
    /// optimal way.
    virtual void removeObjects(const PathsWithSize & paths) = 0;

    /// Remove object on path if exists
    virtual void removeObjectIfExists(const std::string & path) = 0;

    /// Remove objects on path if exists
    virtual void removeObjectsIfExist(const PathsWithSize & paths) = 0;

    /// Copy object with different attributes if required
    virtual void copyObject( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) = 0;

    /// Copy object to another instance of object storage
    /// by default just read the object from source object storage and write
    /// to destination through buffers.
    virtual void copyObjectToAnotherObjectStorage( /// NOLINT
        const std::string & object_from,
        const std::string & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {});

    virtual ~IObjectStorage() = default;

    /// Path to directory with objects cache
    std::string getCacheBasePath() const;

    static AsynchronousReaderPtr getThreadPoolReader();

    static ThreadPool & getThreadPoolWriter();

    virtual void shutdown() = 0;

    virtual void startup() = 0;

    void removeFromCache(const std::string & path);

    /// Apply new settings, in most cases reiniatilize client and some other staff
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) = 0;

    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual String getObjectsNamespace() const = 0;

    /// FIXME: confusing function required for a very specific case. Create new instance of object storage
    /// in different namespace.
    virtual std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, ContextPtr context) = 0;

protected:
    FileCachePtr cache;
};

using ObjectStoragePtr = std::unique_ptr<IObjectStorage>;

}
