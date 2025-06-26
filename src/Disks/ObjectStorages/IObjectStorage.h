#pragma once

#include <string>
#include <map>
#include <mutex>
#include <optional>
#include <filesystem>
#include <variant>

#include <Poco/Timestamp.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Defines.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/copyData.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKey.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/threadPoolCallbackRunner.h>

#include <Disks/DirectoryIterator.h>
#include <Disks/DiskType.h>
#include <Disks/ObjectStorages/MetadataStorageMetrics.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

#include <Interpreters/Context_fwd.h>
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/credentials/credentials.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>

namespace DB::AzureBlobStorage
{
class ContainerClientWrapper;
using ContainerClient = ContainerClientWrapper;

using ConnectionString = StrongTypedef<String, struct ConnectionStringTag>;

using AuthMethod = std::variant<
    ConnectionString,
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>,
    std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>,
    std::shared_ptr<Azure::Identity::ManagedIdentityCredential>>;

}


#endif

#if USE_AWS_S3
namespace DB::S3
{
class Client;
}
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

using ObjectAttributes = std::map<std::string, std::string>;

struct ObjectMetadata
{
    uint64_t size_bytes = 0;
    Poco::Timestamp last_modified;
    std::string etag;
    ObjectAttributes attributes;
};

struct DataLakeObjectMetadata;

struct RelativePathWithMetadata
{
    String relative_path;
    /// Object metadata: size, modification time, etc.
    std::optional<ObjectMetadata> metadata;
    /// Delta lake related object metadata.
    std::optional<DataLakeObjectMetadata> data_lake_metadata;

    RelativePathWithMetadata() = default;

    explicit RelativePathWithMetadata(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : relative_path(std::move(relative_path_))
        , metadata(std::move(metadata_))
    {}

    virtual ~RelativePathWithMetadata() = default;

    virtual std::string getFileName() const { return std::filesystem::path(relative_path).filename(); }
    virtual std::string getPath() const { return relative_path; }
    virtual bool isArchive() const { return false; }
    virtual std::string getPathToArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual size_t fileSizeInArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
};

struct ObjectKeyWithMetadata
{
    ObjectStorageKey key;
    ObjectMetadata metadata;

    ObjectKeyWithMetadata() = default;

    ObjectKeyWithMetadata(ObjectStorageKey key_, ObjectMetadata metadata_)
        : key(std::move(key_))
        , metadata(std::move(metadata_))
    {}
};

using RelativePathWithMetadataPtr = std::shared_ptr<RelativePathWithMetadata>;
using RelativePathsWithMetadata = std::vector<RelativePathWithMetadataPtr>;
using ObjectKeysWithMetadata = std::vector<ObjectKeyWithMetadata>;

class IObjectStorageIterator;
using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

class IObjectStorageKeysGenerator;
using ObjectStorageKeysGeneratorPtr = std::shared_ptr<IObjectStorageKeysGenerator>;

/// Base class for all object storages which implement some subset of ordinary filesystem operations.
///
/// Examples of object storages are S3, Azure Blob Storage, HDFS.
class IObjectStorage
{
public:
    IObjectStorage() = default;

    virtual std::string getName() const = 0;

    virtual ObjectStorageType getType() const = 0;

    /// The logical root or base path used to group a set of related objects.
    virtual std::string getRootPrefix() const { return ""; }

    /// Common object key prefix relative to the root path.
    virtual std::string getCommonKeyPrefix() const = 0;

    virtual std::string getDescription() const = 0;

    virtual const MetadataStorageMetrics & getMetadataStorageMetrics() const;

    /// Object exists or not
    virtual bool exists(const StoredObject & object) const = 0;

    /// Object exists or any child on the specified path exists.
    /// We have this method because object storages are flat for example
    /// /a/b/c/d may exist but /a/b/c may not. So this method will return true for
    /// /, /a, /a/b, /a/b/c, /a/b/c/d while exists will return true only for /a/b/c/d
    virtual bool existsOrHasAnyChild(const std::string & path) const;

    /// List objects recursively by certain prefix.
    virtual void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const;

    /// List objects recursively by certain prefix. Use it instead of listObjects, if you want to list objects lazily.
    virtual ObjectStorageIteratorPtr iterate(const std::string & path_prefix, size_t max_keys) const;

    /// Get object metadata if supported. It should be possible to receive
    /// at least size of object
    virtual std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path) const;

    /// Get object metadata if supported. It should be possible to receive
    /// at least size of object
    virtual ObjectMetadata getObjectMetadata(const std::string & path) const = 0;

    /// Read single object
    virtual std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
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

    /// Remove object on path if exists
    virtual void removeObjectIfExists(const StoredObject & object) = 0;

    /// Remove objects on path if exists
    virtual void removeObjectsIfExist(const StoredObjects & object) = 0;

    /// Copy object with different attributes if required
    virtual void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) = 0;

    /// Copy object to another instance of object storage
    /// by default just read the object from source object storage and write
    /// to destination through buffers.
    virtual void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {});

    virtual ~IObjectStorage() = default;

    virtual const std::string & getCacheName() const;

    static ThreadPool & getThreadPoolWriter();

    virtual void shutdown() = 0;

    virtual void startup() = 0;

    /// Apply new settings, in most cases reiniatilize client and some other staff
    struct ApplyNewSettingsOptions
    {
        bool allow_client_change = true;
    };
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /*config_prefix*/,
        ContextPtr /* context */,
        const ApplyNewSettingsOptions & /* options */) {}

    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual String getObjectsNamespace() const = 0;

    /// Generate blob name for passed absolute local path.
    /// Path can be generated either independently or based on `path`.
    virtual ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const = 0;

    /// Object key prefix for local paths in the directory 'path'.
    virtual ObjectStorageKey
    generateObjectKeyPrefixForDirectoryPath(const std::string & /* path */, const std::optional<std::string> & /* key_prefix */) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'generateObjectKeyPrefixForDirectoryPath' is not implemented");
    }

    /// Returns whether this object storage generates a random blob name for each object.
    /// This function returns false if this object storage just adds some constant string to a passed path to generate a blob name.
    virtual bool areObjectKeysRandom() const = 0;

    /// Get unique id for passed absolute path in object storage.
    virtual std::string getUniqueId(const std::string & path) const { return path; }

    /// Remove filesystem cache.
    virtual void removeCacheIfExists(const std::string & /* path */) {}

    virtual bool supportsCache() const { return false; }

    virtual bool isReadOnly() const { return false; }
    virtual bool isWriteOnce() const { return false; }
    virtual bool isPlain() const { return false; }

    virtual bool supportParallelWrite() const { return false; }

    virtual ReadSettings patchSettings(const ReadSettings & read_settings) const;

    virtual WriteSettings patchSettings(const WriteSettings & write_settings) const;

    virtual void setKeysGenerator(ObjectStorageKeysGeneratorPtr) { }

#if USE_AZURE_BLOB_STORAGE
    virtual std::shared_ptr<const AzureBlobStorage::ContainerClient> getAzureBlobStorageClient() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for AzureBlobStorage");
    }

    virtual AzureBlobStorage::AuthMethod getAzureBlobStorageAuthMethod() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for AzureBlobStorage");
    }
#endif

#if USE_AWS_S3
    virtual std::shared_ptr<const S3::Client> getS3StorageClient()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for S3ObjectStorage");
    }
    virtual std::shared_ptr<const S3::Client> tryGetS3StorageClient() { return nullptr; }
#endif


private:
    mutable std::mutex throttlers_mutex;
    ThrottlerPtr remote_read_throttler;
    ThrottlerPtr remote_write_throttler;
};

using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

}
