#pragma once

#include <filesystem>
#include <string>
#include <map>
#include <mutex>
#include <optional>

#include <Poco/Timestamp.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <Common/Exception.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/copyData.h>

#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/DiskType.h>
#include <Common/ThreadPool_fwd.h>
#include <Disks/WriteMode.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Disks/DirectoryIterator.h>
#include <Common/ThreadPool.h>
#include <Interpreters/threadPoolCallbackRunner.h>


namespace DB
{

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

using ObjectAttributes = std::map<std::string, std::string>;

struct ObjectMetadata
{
    uint64_t size_bytes;
    std::optional<Poco::Timestamp> last_modified;
    std::optional<ObjectAttributes> attributes;
};

struct RelativePathWithMetadata
{
    String relative_path;
    ObjectMetadata metadata{};

    RelativePathWithMetadata() = default;

    RelativePathWithMetadata(const String & relative_path_, const ObjectMetadata & metadata_)
        : relative_path(relative_path_), metadata(metadata_)
    {}
};

using RelativePathsWithMetadata = std::vector<RelativePathWithMetadata>;

class IObjectStorageIterator;
using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

/// Base class for all object storages which implement some subset of ordinary filesystem operations.
///
/// Examples of object storages are S3, Azure Blob Storage, HDFS.
class IObjectStorage
{
public:
    IObjectStorage() = default;

    virtual DataSourceDescription getDataSourceDescription() const = 0;

    virtual std::string getName() const = 0;

    /// Object exists or not
    virtual bool exists(const StoredObject & object) const = 0;

    /// Object exists or any child on the specified path exists.
    /// We have this method because object storages are flat for example
    /// /a/b/c/d may exist but /a/b/c may not. So this method will return true for
    /// /, /a, /a/b, /a/b/c, /a/b/c/d while exists will return true only for /a/b/c/d
    virtual bool existsOrHasAnyChild(const std::string & path) const;

    virtual void listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const;

    virtual ObjectStorageIteratorPtr iterate(const std::string & path_prefix) const;

    /// Get object metadata if supported. It should be possible to receive
    /// at least size of object
    virtual std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path) const;

    /// Get object metadata if supported. It should be possible to receive
    /// at least size of object
    virtual ObjectMetadata getObjectMetadata(const std::string & path) const = 0;

    /// Read single object
    virtual std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Read multiple objects with common prefix
    virtual std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const = 0;

    /// Open the file for write and return WriteBufferFromFileBase object.
    virtual std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) = 0;

    virtual bool isRemote() const = 0;

    /// Remove object. Throws exception if object doesn't exists.
    virtual void removeObject(const StoredObject & object) = 0;

    /// Remove multiple objects. Some object storages can do batch remove in a more
    /// optimal way.
    virtual void removeObjects(const StoredObjects & objects) = 0;

    /// Remove object on path if exists
    virtual void removeObjectIfExists(const StoredObject & object) = 0;

    /// Remove objects on path if exists
    virtual void removeObjectsIfExist(const StoredObjects & object) = 0;

    /// Copy object with different attributes if required
    virtual void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) = 0;

    /// Copy object to another instance of object storage
    /// by default just read the object from source object storage and write
    /// to destination through buffers.
    virtual void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {});

    virtual ~IObjectStorage() = default;

    virtual const std::string & getCacheName() const;

    static ThreadPool & getThreadPoolWriter();

    virtual void shutdown() = 0;

    virtual void startup() = 0;

    /// Apply new settings, in most cases reiniatilize client and some other staff
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration &,
        const std::string & /*config_prefix*/,
        ContextPtr)
    {}

    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual String getObjectsNamespace() const = 0;

    /// FIXME: confusing function required for a very specific case. Create new instance of object storage
    /// in different namespace.
    virtual std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, ContextPtr context) = 0;

    /// Generate blob name for passed absolute local path.
    /// Path can be generated either independently or based on `path`.
    virtual std::string generateBlobNameForPath(const std::string & path);

    /// Get unique id for passed absolute path in object storage.
    virtual std::string getUniqueId(const std::string & path) const { return path; }

    /// Remove filesystem cache.
    virtual void removeCacheIfExists(const std::string & /* path */) {}

    virtual bool supportsCache() const { return false; }

    virtual bool isReadOnly() const { return false; }
    virtual bool isWriteOnce() const { return false; }

    virtual bool supportParallelWrite() const { return false; }

    virtual ReadSettings getAdjustedSettingsFromMetadataFile(const ReadSettings & settings, const std::string & /* path */) const { return settings; }

    virtual WriteSettings getAdjustedSettingsFromMetadataFile(const WriteSettings & settings, const std::string & /* path */) const { return settings; }

    virtual ReadSettings patchSettings(const ReadSettings & read_settings) const;

    virtual WriteSettings patchSettings(const WriteSettings & write_settings) const;

private:
    mutable std::mutex throttlers_mutex;
    ThrottlerPtr remote_read_throttler;
    ThrottlerPtr remote_write_throttler;
};

using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

}
