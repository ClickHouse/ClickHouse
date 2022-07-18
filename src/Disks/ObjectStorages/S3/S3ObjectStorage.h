#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3Capabilities.h>
#include <memory>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <Storages/StorageS3Settings.h>


namespace DB
{

struct S3ObjectStorageSettings
{
    S3ObjectStorageSettings() = default;

    S3ObjectStorageSettings(
        const S3Settings::ReadWriteSettings & s3_settings_,
        uint64_t min_bytes_for_seek_,
        int32_t list_object_keys_size_,
        int32_t objects_chunk_size_to_delete_)
        : s3_settings(s3_settings_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , list_object_keys_size(list_object_keys_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
    {}

    S3Settings::ReadWriteSettings s3_settings;

    uint64_t min_bytes_for_seek;
    int32_t list_object_keys_size;
    int32_t objects_chunk_size_to_delete;
};


class S3ObjectStorage : public IObjectStorage
{
public:
    S3ObjectStorage(
        std::unique_ptr<Aws::S3::S3Client> && client_,
        std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
        String version_id_,
        const S3Capabilities & s3_capabilities_,
        String bucket_,
        FileCachePtr cache_)
        : bucket(bucket_)
        , client(std::move(client_))
        , s3_settings(std::move(s3_settings_))
        , s3_capabilities(s3_capabilities_)
        , version_id(std::move(version_id_))
        , cache(cache_)
    {
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
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

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

    std::string generateBlobNameForPath(const std::string & path) override;

    bool isRemote() const override { return true; }

    void setCapabilitiesSupportBatchDelete(bool value) { s3_capabilities.support_batch_delete = value; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    bool supportsCache() const override { return true; }

    void removeCacheIfExists(const std::string & path_key) override;

    String getCacheBasePath() const override;

private:
    ReadSettings patchSettings(const ReadSettings & read_settings) const;

    void setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_);

    void setNewClient(std::unique_ptr<Aws::S3::S3Client> && client_);

    void copyObjectImpl(
        const String & src_bucket,
        const String & src_key,
        const String & dst_bucket,
        const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<ObjectAttributes> metadata = std::nullopt) const;

    void copyObjectMultipartImpl(
        const String & src_bucket,
        const String & src_key,
        const String & dst_bucket,
        const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<ObjectAttributes> metadata = std::nullopt) const;

    void removeObjectImpl(const StoredObject & object, bool if_exists);
    void removeObjectsImpl(const StoredObjects & objects, bool if_exists);

    Aws::S3::Model::HeadObjectOutcome requestObjectHeadData(const std::string & bucket_from, const std::string & key) const;

    std::string bucket;

    MultiVersion<Aws::S3::S3Client> client;
    MultiVersion<S3ObjectStorageSettings> s3_settings;
    S3Capabilities s3_capabilities;

    const String version_id;

    FileCachePtr cache;
};

}

#endif
