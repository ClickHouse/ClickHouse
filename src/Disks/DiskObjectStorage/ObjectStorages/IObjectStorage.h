#pragma once

#include <string>
#include <map>
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
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/ThreadPool_fwd.h>

#include <Disks/DirectoryIterator.h>
#include <Disks/DiskType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

#include <Interpreters/Context_fwd.h>
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/core/credentials/credentials.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/identity/client_secret_credential.hpp>

namespace DB::AzureBlobStorage
{

class ContainerClientWrapper;
using ContainerClient = ContainerClientWrapper;

class StaticCredential : public Azure::Core::Credentials::TokenCredential
{
public:
    StaticCredential(std::string token_, std::chrono::system_clock::time_point expires_on_)
        : token(std::move(token_)), expires_on(expires_on_)
    {}

    Azure::Core::Credentials::AccessToken GetToken(
        Azure::Core::Credentials::TokenRequestContext const &,
        Azure::Core::Context const &) const override
    {
        return Azure::Core::Credentials::AccessToken { .Token = token, .ExpiresOn = expires_on };
    }

private:
    std::string token;
    std::chrono::system_clock::time_point expires_on;
};

using ConnectionString = StrongTypedef<String, struct ConnectionStringTag>;

using AuthMethod = std::variant<
    ConnectionString,
    std::shared_ptr<Azure::Identity::ClientSecretCredential>,
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>,
    std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>,
    std::shared_ptr<Azure::Identity::ManagedIdentityCredential>,
    std::shared_ptr<AzureBlobStorage::StaticCredential>>;


struct ConnectionParams;
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

class ReadPipeline;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

class ReadBufferFromFileBase;
class WriteBufferFromFileBase;

using ObjectAttributes = std::map<std::string, std::string>;

struct ObjectMetadata
{
    uint64_t size_bytes = 0;
    bool is_size_known = true;
    /// True if this metadata was obtained from a real object-storage request (HEAD/listing).
    /// False only for the skip_object_metadata placeholder.
    bool is_fetched = true;
    Poco::Timestamp last_modified;
    /// Whether `last_modified` carries a real modification time. Some object storages (e.g. a web server
    /// whose HTTP response has no `Last-Modified` header) cannot report it; leaving `last_modified` at the
    /// default epoch would make the schema/count caches treat the object as older than any cached entry and
    /// silently reuse a stale value. Cache validators must skip the cache when this is `false`.
    bool is_last_modified_known = true;
    std::string etag;
    ObjectAttributes tags;
    ObjectAttributes attributes;
};

struct DataLakeObjectMetadata;

struct RelativePathWithMetadata
{
    String relative_path;
    std::optional<size_t> read_source_index;
    std::optional<String> path_for_glob_matching;
    std::optional<String> path_for_deduplication;
    bool derive_file_name_from_url_path = false;
    /// Object metadata: size, modification time, etc.
    std::optional<ObjectMetadata> metadata;

    RelativePathWithMetadata() = default;

    explicit RelativePathWithMetadata(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : relative_path(std::move(relative_path_))
        , metadata(std::move(metadata_))
    {}

    RelativePathWithMetadata(String relative_path_, std::optional<size_t> read_source_index_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : relative_path(std::move(relative_path_))
        , read_source_index(read_source_index_)
        , metadata(std::move(metadata_))
    {}

    RelativePathWithMetadata(const RelativePathWithMetadata & other) = default;

    ~RelativePathWithMetadata() = default;

    std::string getFileName() const
    {
        if (!derive_file_name_from_url_path)
            return std::filesystem::path(relative_path).filename();

        /// Web index listings can carry a URL query/fragment in `relative_path` (for example,
        /// "data.tsv.gz?download=1"). They are not part of the file name and would defeat
        /// extension-based format/compression detection, so strip them only for web paths, consistent
        /// with how direct `url` reads use the URL path component.
        const auto pos = relative_path.find_first_of("?#");
        const std::string path_without_query = pos == std::string::npos ? relative_path : relative_path.substr(0, pos);
        return std::filesystem::path(path_without_query).filename();
    }
    std::string getPath() const { return relative_path; }
    std::string getPathForGlobMatching() const { return path_for_glob_matching.value_or(relative_path); }
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

struct SmallObjectDataWithMetadata
{
    std::string data;
    ObjectMetadata metadata;
};

using RelativePathWithMetadataPtr = std::shared_ptr<RelativePathWithMetadata>;
using RelativePathsWithMetadata = std::vector<RelativePathWithMetadataPtr>;
using ObjectKeysWithMetadata = std::vector<ObjectKeyWithMetadata>;

class IObjectStorageIterator;
using ObjectStorageIteratorPtr = std::shared_ptr<IObjectStorageIterator>;

/// Base class for all object storages which implement some subset of ordinary filesystem operations.
///
/// Examples of object storages are S3, Azure Blob Storage, HDFS.
class IObjectStorage
{
public:
    IObjectStorage() = default;

    virtual std::string getName() const = 0;

    /// Returns the disk name for logging purposes. Empty if not associated with a disk.
    virtual std::string getDiskName() const { return {}; }

    virtual ObjectStorageType getType() const = 0;

    /// The logical root or base path used to group a set of related objects.
    virtual std::string getRootPrefix() const { return ""; }

    /// Common object key prefix relative to the root path.
    virtual std::string getCommonKeyPrefix() const = 0;

    virtual std::string getDescription() const = 0;

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
    /// Optional start_after is supported for storages that can efficiently resume listing from a key.
    virtual ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix,
        size_t max_keys,
        bool with_tags,
        const std::optional<std::string> & start_after) const;

    /// Get object metadata if supported. It should be possible to receive at least size of object
    virtual ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const = 0;
    virtual ObjectMetadata getObjectMetadata(const RelativePathWithMetadata & object, bool with_tags) const
    {
        return getObjectMetadata(object.getPath(), with_tags);
    }

    /// Same as getObjectMetadata(), but ignores if object does not exist.
    virtual std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const = 0;
    virtual std::optional<ObjectMetadata> tryGetObjectMetadata(const RelativePathWithMetadata & object, bool with_tags) const
    {
        return tryGetObjectMetadata(object.getPath(), with_tags);
    }

    /// Read single object
    virtual std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const = 0;

    /// Populate a `ReadPipeline` with the source and any stages this storage needs.
    ///
    /// The "source" is the bottom layer of the read-buffer chain — a descriptor
    /// (not an actual buffer) that tells `ReadPipeline::build()` how to construct
    /// the base `ReadBufferFromFileBase`. For object storages the descriptor is
    /// `ObjectStorageSource { storage, read_hint }`; later stages (disk cache,
    /// gather, async-prefetch, decryption) wrap around the buffer it produces.
    ///
    /// Default: sets the source to this storage. `CachedObjectStorage` overrides
    /// to delegate to the inner storage and adds the disk cache stage.
    virtual void prepareRead(
        ObjectStoragePtr storage,
        const StoredObjects & objects,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        ReadPipeline & pipeline) const;

    /// Read small object into memory and return it as string
    /// Also contain consistent object metadata if available in this object storage.
    /// if size of object is larger than max_size_bytes throws exception
    ///
    /// NOTE: This method exists because it's impossible to get object metadata in generic way
    /// from readObject. ReadObject returns ReadBufferFromFileBase and most of implementations
    /// issue first request to object store only in first call of read/next method.
    ///
    /// WARN: Don't use this method for large objects, it will eat all memory.
    /// That is the reason why max_size_bytes is explicit and 0-value will not help to bypass this check.
    virtual SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        size_t max_size_bytes,
        std::optional<size_t> read_hint = {}) const;

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

        /// Force the client to be rebuilt even if the stored settings did not change. Used to re-resolve
        /// credentials under a different accessing context (e.g. re-applying the server-credential opt-in to a
        /// server-internal table whose client was built restricted at metadata load) without detaching the table.
        bool force_client_rebuild = false;
    };
    virtual void applyNewSettings(
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /*config_prefix*/,
        ContextPtr /* context */,
        const ApplyNewSettingsOptions & /* options */) {}

    /// Sometimes object storages have something similar to chroot or namespace, for example
    /// buckets in S3. If object storage doesn't have any namepaces return empty string.
    virtual String getObjectsNamespace() const = 0;

    /// Get unique id for passed absolute path in object storage.
    virtual std::string getUniqueId(const std::string & path) const { return path; }

    /// Remove filesystem cache.
    virtual void removeCacheIfExists(const std::string & /* path */) {}

    virtual bool supportsCache() const { return false; }

    virtual bool isReadOnly() const { return false; }

    virtual bool supportParallelWrite() const { return false; }

    virtual ReadSettings patchSettings(const ReadSettings & read_settings) const;

    virtual WriteSettings patchSettings(const WriteSettings & write_settings) const;

    virtual ObjectStorageKeyGeneratorPtr createKeyGenerator() const = 0;

#if USE_AZURE_BLOB_STORAGE
    virtual std::shared_ptr<const AzureBlobStorage::ContainerClient> getAzureBlobStorageClient() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This function is only implemented for AzureBlobStorage");
    }

    virtual const AzureBlobStorage::ConnectionParams & getAzureBlobStorageConnectionParams() const
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

    /// Invokes the catalog-vended credentials refresh callback (e.g. for Glue / Unity / REST
    /// data-lake tables) and atomically swaps the internal S3 client to one signed with the
    /// fresh credentials. Returns true if the callback was present and a new client was
    /// installed. Used by delta-kernel's `ExpiredToken` recovery path, which bypasses the
    /// `ReadBufferFromS3` / `getObjectMetadata` error handlers that normally invoke this.
    virtual bool tryRefreshCredentialsViaCallback() { return false; }

#if USE_AZURE_BLOB_STORAGE || USE_AWS_S3
    /// Assign tag on objects
    virtual void tagObjects(const StoredObjects &, const std::string &, const std::string &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The method 'tagObjects' is only implemented for S3 and Azure storages");
    }
#endif

    /// Returns the inner (unwrapped) object storage for decorator types such as `CachedObjectStorage`.
    /// Returns nullptr for non-decorator types, meaning this storage is already the base.
    virtual ObjectStoragePtr getUnderlying() { return nullptr; }
};

using ObjectStoragePtr = std::shared_ptr<IObjectStorage>;

}
