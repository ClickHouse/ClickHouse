#pragma once
#include <Disks/IObjectStorage.h>

#include <Common/config.h>

#if USE_AWS_S3

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
        uint64_t thread_pool_size_,
        int32_t list_object_keys_size_,
        int32_t objects_chunk_size_to_delete_)
        : s3_settings(s3_settings_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , thread_pool_size(thread_pool_size_)
        , list_object_keys_size(list_object_keys_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
    {}

    S3Settings::ReadWriteSettings s3_settings;

    uint64_t min_bytes_for_seek;
    uint64_t thread_pool_size;
    int32_t list_object_keys_size;
    int32_t objects_chunk_size_to_delete;
};


class S3ObjectStorage : public IObjectStorage
{
public:
    S3ObjectStorage(
        FileCachePtr && cache_,
        std::unique_ptr<Aws::S3::S3Client> && client_,
        std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
        String version_id_,
        String bucket_)
        : IObjectStorage(std::move(cache_))
        , bucket(bucket_)
        , client(std::move(client_))
        , s3_settings(std::move(s3_settings_))
        , version_id(std::move(version_id_))
    {}
        
    bool exists(const std::string & path) const override;

    std::unique_ptr<SeekableReadBuffer> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const std::string & common_path_prefix,
        const BlobsPathToSize & blobs_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, BlobsPathToSize & children) const override;
    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const std::string & path) override;

    void removeObjects(const std::vector<std::string> & paths) override;

    void removeObjectIfExists(const std::string & path) override;

    void removeObjectsIfExist(const std::vector<std::string> & paths) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject(const std::string & object_from, const std::string & object_to, std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_);

    void setNewClient(std::unique_ptr<Aws::S3::S3Client> && client_);

    void shutdown() override;

    void startup() override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override;

private:

    void copyObjectImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<ObjectAttributes> metadata = std::nullopt) const;

    void copyObjectMultipartImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<ObjectAttributes> metadata = std::nullopt) const;

    Aws::S3::Model::HeadObjectOutcome requestObjectHeadData(const std::string & bucket_from, const std::string & key) const;

    std::string bucket;

    MultiVersion<Aws::S3::S3Client> client;
    MultiVersion<S3ObjectStorageSettings> s3_settings;

    const String version_id;
};

}

#endif
