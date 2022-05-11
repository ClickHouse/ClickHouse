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
#include <Common/FileCache.h>


namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

using ObjectAttributes = std::map<std::string, std::string>;

/// Path to blob with it's size
struct BlobPathWithSize
{
    std::string relative_path;
    uint64_t bytes_size;

    BlobPathWithSize() = default;
    BlobPathWithSize(const BlobPathWithSize & other) = default;

    BlobPathWithSize(const std::string & relative_path_, uint64_t bytes_size_)
        : relative_path(relative_path_)
        , bytes_size(bytes_size_)
    {}
};

/// List of blobs with their sizes
using BlobsPathToSize = std::vector<BlobPathWithSize>;

struct ObjectMetadata
{
    uint64_t size_bytes;
    std::optional<Poco::Timestamp> last_modified;
    std::optional<ObjectAttributes> attributes;
};

using FinalizeCallback = std::function<void(size_t bytes_count)>;

class IObjectStorage
{
public:
    explicit IObjectStorage(FileCachePtr && cache_)
        : cache(std::move(cache_))
    {}

    virtual bool exists(const std::string & path) const = 0;

    virtual void listPrefix(const std::string & path, BlobsPathToSize & children) const = 0;

    virtual ObjectMetadata getObjectMetadata(const std::string & path) const = 0;

    virtual std::unique_ptr<SeekableReadBuffer> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    virtual std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const std::string & common_path_prefix,
        const BlobsPathToSize & blobs_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) = 0;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    virtual void removeObject(const std::string & path) = 0;

    virtual void removeObjects(const std::vector<std::string> & paths) = 0;

    /// Remove file if it exists.
    virtual void removeObjectIfExists(const std::string & path) = 0;

    virtual void removeObjectsIfExist(const std::vector<std::string> & paths) = 0;

    virtual void copyObject(const std::string & object_from, const std::string & object_to, std::optional<ObjectAttributes> object_to_attributes = {}) = 0;

    virtual ~IObjectStorage() = default;

    std::string getCacheBasePath() const;

    static AsynchronousReaderPtr getThreadPoolReader();

    static ThreadPool & getThreadPoolWriter();

    virtual void shutdown() = 0;

    virtual void startup() = 0;

    void removeFromCache(const std::string & path);

    virtual void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) = 0;

protected:
    FileCachePtr cache;
};

using ObjectStoragePtr = std::unique_ptr<IObjectStorage>;

}
