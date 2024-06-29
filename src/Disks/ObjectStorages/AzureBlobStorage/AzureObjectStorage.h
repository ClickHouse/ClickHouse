#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/MultiVersion.h>
#include <azure/storage/blobs.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{

struct AzureObjectStorageSettings
{
    AzureObjectStorageSettings(
        uint64_t max_single_part_upload_size_,
        uint64_t min_bytes_for_seek_,
        int max_single_read_retries_,
        int max_single_download_retries_,
        int list_object_keys_size_,
        size_t min_upload_part_size_,
        size_t max_upload_part_size_,
        size_t max_single_part_copy_size_,
        bool use_native_copy_,
        size_t max_unexpected_write_error_retries_,
        size_t max_inflight_parts_for_one_file_,
        size_t strict_upload_part_size_,
        size_t upload_part_size_multiply_factor_,
        size_t upload_part_size_multiply_parts_count_threshold_)
        : max_single_part_upload_size(max_single_part_upload_size_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
        , list_object_keys_size(list_object_keys_size_)
        , min_upload_part_size(min_upload_part_size_)
        , max_upload_part_size(max_upload_part_size_)
        , max_single_part_copy_size(max_single_part_copy_size_)
        , use_native_copy(use_native_copy_)
        , max_unexpected_write_error_retries(max_unexpected_write_error_retries_)
        , max_inflight_parts_for_one_file(max_inflight_parts_for_one_file_)
        , strict_upload_part_size(strict_upload_part_size_)
        , upload_part_size_multiply_factor(upload_part_size_multiply_factor_)
        , upload_part_size_multiply_parts_count_threshold(upload_part_size_multiply_parts_count_threshold_)
    {
    }

    AzureObjectStorageSettings() = default;

    size_t max_single_part_upload_size = 100 * 1024 * 1024; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    uint64_t min_bytes_for_seek = 1024 * 1024;
    size_t max_single_read_retries = 3;
    size_t max_single_download_retries = 3;
    int list_object_keys_size = 1000;
    size_t min_upload_part_size = 16 * 1024 * 1024;
    size_t max_upload_part_size = 5ULL * 1024 * 1024 * 1024;
    size_t max_single_part_copy_size = 256 * 1024 * 1024;
    bool use_native_copy = false;
    size_t max_unexpected_write_error_retries = 4;
    size_t max_inflight_parts_for_one_file = 20;
    size_t max_blocks_in_multipart_upload = 50000;
    size_t strict_upload_part_size = 0;
    size_t upload_part_size_multiply_factor = 2;
    size_t upload_part_size_multiply_parts_count_threshold = 500;
};

using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

class AzureObjectStorage : public IObjectStorage
{
public:

    using SettingsPtr = std::unique_ptr<AzureObjectStorageSettings>;

    AzureObjectStorage(
        const String & name_,
        AzureClientPtr && client_,
        SettingsPtr && settings_,
        const String & object_namespace_,
        const String & description_);

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectStorageIteratorPtr iterate(const std::string & path_prefix, size_t max_keys) const override;

    std::string getName() const override { return "AzureObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Azure; }

    std::string getCommonKeyPrefix() const override { return ""; }

    std::string getDescription() const override { return description; }

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

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override {}

    void startup() override {}

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override;

    String getObjectsNamespace() const override { return object_namespace ; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override;

    bool isRemote() const override { return true; }

    std::shared_ptr<const AzureObjectStorageSettings> getSettings() { return settings.get(); }

    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> getAzureBlobStorageClient() override
    {
        return client.get();
    }

    bool supportParallelWrite() const override { return true; }

private:
    using SharedAzureClientPtr = std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient>;
    void removeObjectImpl(const StoredObject & object, const SharedAzureClientPtr & client_ptr, bool if_exists);

    const String name;
    /// client used to access the files in the Blob Storage cloud
    MultiVersion<Azure::Storage::Blobs::BlobContainerClient> client;
    MultiVersion<AzureObjectStorageSettings> settings;
    const String object_namespace; /// container + prefix

    /// We use source url without container and prefix as description, because in Azure there are no limitations for operations between different containers.
    const String description;

    LoggerPtr log;
};

}

#endif
