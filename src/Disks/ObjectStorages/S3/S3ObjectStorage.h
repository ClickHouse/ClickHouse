#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3Capabilities.h>
#include <memory>
#include <Storages/StorageS3Settings.h>
#include <Common/MultiVersion.h>


namespace DB
{

struct S3ObjectStorageSettings
{
    S3ObjectStorageSettings() = default;

    S3ObjectStorageSettings(
        const S3Settings::RequestSettings & request_settings_,
        uint64_t min_bytes_for_seek_,
        int32_t list_object_keys_size_,
        int32_t objects_chunk_size_to_delete_)
        : request_settings(request_settings_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , list_object_keys_size(list_object_keys_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
    {}

    S3Settings::RequestSettings request_settings;

    uint64_t min_bytes_for_seek;
    int32_t list_object_keys_size;
    int32_t objects_chunk_size_to_delete;
};


class S3ObjectStorage : public IObjectStorage
{
public:
    struct Clients
    {
        std::shared_ptr<S3::Client> client;
        std::shared_ptr<S3::Client> client_with_long_timeout;

        Clients() = default;
        Clients(std::shared_ptr<S3::Client> client, const S3ObjectStorageSettings & settings);
    };

private:
    friend class S3PlainObjectStorage;

    S3ObjectStorage(
        const char * logger_name,
        std::unique_ptr<S3::Client> && client_,
        std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
        String version_id_,
        const S3Capabilities & s3_capabilities_,
        String bucket_,
        String connection_string)
        : bucket(bucket_)
        , clients(std::make_unique<Clients>(std::move(client_), *s3_settings_))
        , s3_settings(std::move(s3_settings_))
        , s3_capabilities(s3_capabilities_)
        , version_id(std::move(version_id_))
    {
        data_source_description.type = DataSourceType::S3;
        data_source_description.description = connection_string;
        data_source_description.is_cached = false;
        data_source_description.is_encrypted = false;

        log = &Poco::Logger::get(logger_name);
    }

public:
    template <class ...Args>
    explicit S3ObjectStorage(std::unique_ptr<S3::Client> && client_, Args && ...args)
        : S3ObjectStorage("S3ObjectStorage", std::move(client_), std::forward<Args>(args)...)
    {
    }

    DataSourceDescription getDataSourceDescription() const override
    {
        return data_source_description;
    }

    std::string getName() const override { return "S3ObjectStorage"; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const override;

    ObjectStorageIteratorPtr iterate(const std::string & path_prefix) const override;

    /// Uses `DeleteObjectRequest`.
    void removeObject(const StoredObject & object) override;

    /// Uses `DeleteObjectsRequest` if it is allowed by `s3_capabilities`, otherwise `DeleteObjectRequest`.
    /// `DeleteObjectsRequest` is not supported on GCS, see https://issuetracker.google.com/issues/162653700 .
    void removeObjects(const StoredObjects & objects) override;

    /// Uses `DeleteObjectRequest`.
    void removeObjectIfExists(const StoredObject & object) override;

    /// Uses `DeleteObjectsRequest` if it is allowed by `s3_capabilities`, otherwise `DeleteObjectRequest`.
    /// `DeleteObjectsRequest` does not exist on GCS, see https://issuetracker.google.com/issues/162653700 .
    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    std::string getObjectsNamespace() const override { return bucket; }

    bool isRemote() const override { return true; }

    void setCapabilitiesSupportBatchDelete(bool value) { s3_capabilities.support_batch_delete = value; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    bool supportParallelWrite() const override { return true; }

private:
    void setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_);

    void removeObjectImpl(const StoredObject & object, bool if_exists);
    void removeObjectsImpl(const StoredObjects & objects, bool if_exists);

    std::string bucket;

    MultiVersion<Clients> clients;
    MultiVersion<S3ObjectStorageSettings> s3_settings;
    S3Capabilities s3_capabilities;

    const String version_id;

    Poco::Logger * log;
    DataSourceDescription data_source_description;
};

/// Do not encode keys, store as-is, and do not require separate disk for metadata.
/// But because of this does not support renames/hardlinks/attrs/...
///
/// NOTE: This disk has excessive API calls.
class S3PlainObjectStorage : public S3ObjectStorage
{
public:
    std::string generateBlobNameForPath(const std::string & path) override { return path; }
    std::string getName() const override { return "S3PlainObjectStorage"; }

    template <class ...Args>
    explicit S3PlainObjectStorage(Args && ...args)
        : S3ObjectStorage("S3PlainObjectStorage", std::forward<Args>(args)...)
    {
        data_source_description.type = DataSourceType::S3_Plain;
    }

    /// Notes:
    /// - supports BACKUP to this disk
    /// - does not support INSERT into MergeTree table on this disk
    bool isWriteOnce() const override { return true; }
};

}

#endif
