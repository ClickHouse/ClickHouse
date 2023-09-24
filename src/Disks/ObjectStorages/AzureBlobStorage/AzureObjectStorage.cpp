#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include "Common/Exception.h"

#if USE_AZURE_BLOB_STORAGE

#include <Common/getRandomASCIIString.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;

}

namespace DB
{


namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int UNSUPPORTED_METHOD;
}


namespace
{

class AzureIteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    AzureIteratorAsync(
        const std::string & path_prefix,
        std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client_,
        size_t max_list_size)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageAzureThreads,
            CurrentMetrics::ObjectStorageAzureThreadsActive,
            "ListObjectAzure")
        , client(client_)
    {

        options.Prefix = path_prefix;
        options.PageSizeHint = static_cast<int>(max_list_size);
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        batch.clear();
        auto outcome = client->ListBlobs(options);
        auto blob_list_response = client->ListBlobs(options);
        auto blobs_list = blob_list_response.Blobs;

        for (const auto & blob : blobs_list)
        {
            batch.emplace_back(
                blob.Name,
                ObjectMetadata{
                    static_cast<uint64_t>(blob.BlobSize),
                    Poco::Timestamp::fromEpochTime(
                        std::chrono::duration_cast<std::chrono::seconds>(
                            static_cast<std::chrono::system_clock::time_point>(blob.Details.LastModified).time_since_epoch()).count()),
                    {}});
        }

        if (!blob_list_response.NextPageToken.HasValue() || blob_list_response.NextPageToken.Value().empty())
            return false;

        options.ContinuationToken = blob_list_response.NextPageToken;
        return true;
    }

    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client;
    Azure::Storage::Blobs::ListBlobsOptions options;
};

}


AzureObjectStorage::AzureObjectStorage(
    const String & name_,
    AzureClientPtr && client_,
    SettingsPtr && settings_)
    : name(name_)
    , client(std::move(client_))
    , settings(std::move(settings_))
    , log(&Poco::Logger::get("AzureObjectStorage"))
{
    data_source_description.type = DataSourceType::AzureBlobStorage;
    data_source_description.description = client.get()->GetUrl();
    data_source_description.is_cached = false;
    data_source_description.is_encrypted = false;
}

std::string AzureObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    return getRandomASCIIString(32);
}

bool AzureObjectStorage::exists(const StoredObject & object) const
{
    auto client_ptr = client.get();

    /// What a shame, no Exists method...
    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = object.remote_path;
    options.PageSizeHint = 1;

    auto blobs_list_response = client_ptr->ListBlobs(options);
    auto blobs_list = blobs_list_response.Blobs;

    for (const auto & blob : blobs_list)
    {
        if (object.remote_path == blob.Name)
            return true;
    }

    return false;
}

ObjectStorageIteratorPtr AzureObjectStorage::iterate(const std::string & path_prefix) const
{
    auto settings_ptr = settings.get();
    auto client_ptr = client.get();

    return std::make_shared<AzureIteratorAsync>(path_prefix, client_ptr, settings_ptr->list_object_keys_size);
}

void AzureObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const
{
    auto client_ptr = client.get();

    /// What a shame, no Exists method...
    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = path;
    if (max_keys)
        options.PageSizeHint = max_keys;
    else
        options.PageSizeHint = settings.get()->list_object_keys_size;
    Azure::Storage::Blobs::ListBlobsPagedResponse blob_list_response;

    while (true)
    {
        blob_list_response = client_ptr->ListBlobs(options);
        auto blobs_list = blob_list_response.Blobs;

        for (const auto & blob : blobs_list)
        {
            children.emplace_back(
                blob.Name,
                ObjectMetadata{
                    static_cast<uint64_t>(blob.BlobSize),
                    Poco::Timestamp::fromEpochTime(
                        std::chrono::duration_cast<std::chrono::seconds>(
                            static_cast<std::chrono::system_clock::time_point>(blob.Details.LastModified).time_since_epoch()).count()),
                    {}});
        }

        if (max_keys)
        {
            int keys_left = max_keys - static_cast<int>(children.size());
            if (keys_left <= 0)
                break;
            options.PageSizeHint = keys_left;
        }

        if (blob_list_response.HasPage())
            options.ContinuationToken = blob_list_response.NextPageToken;
        else
            break;
    }
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = settings.get();

    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client.get(), object.remote_path, patchSettings(read_settings), settings_ptr->max_single_read_retries,
        settings_ptr->max_single_download_retries);
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    ReadSettings disk_read_settings = patchSettings(read_settings);
    auto settings_ptr = settings.get();
    auto global_context = Context::getGlobalContextInstance();

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromAzureBlobStorage>(
            client.get(),
            path,
            disk_read_settings,
            settings_ptr->max_single_read_retries,
            settings_ptr->max_single_download_retries,
            /* use_external_buffer */true,
            /* restricted_seek */true,
            read_until_position);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */false);

        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, disk_read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

/// Open the file for write and return WriteBufferFromFileBase object.
std::unique_ptr<WriteBufferFromFileBase> AzureObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes>,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Azure storage doesn't support append");

    LOG_TEST(log, "Writing file: {}", object.remote_path);

    return std::make_unique<WriteBufferFromAzureBlobStorage>(
        client.get(),
        object.remote_path,
        settings.get()->max_single_part_upload_size,
        buf_size,
        patchSettings(write_settings));
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void AzureObjectStorage::removeObject(const StoredObject & object)
{
    const auto & path = object.remote_path;
    LOG_TEST(log, "Removing single object: {}", path);
    auto client_ptr = client.get();
    auto delete_info = client_ptr->DeleteBlob(path);
    if (!delete_info.Value.Deleted)
        throw Exception(
            ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file (path: {}) in AzureBlob Storage, reason: {}",
            path, delete_info.RawResponse ? delete_info.RawResponse->GetReasonPhrase() : "Unknown");
}

void AzureObjectStorage::removeObjects(const StoredObjects & objects)
{
    auto client_ptr = client.get();
    for (const auto & object : objects)
    {
        LOG_TEST(log, "Removing object: {} (total: {})", object.remote_path, objects.size());
        auto delete_info = client_ptr->DeleteBlob(object.remote_path);
        if (!delete_info.Value.Deleted)
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file (path: {}) in AzureBlob Storage, reason: {}",
                object.remote_path, delete_info.RawResponse ? delete_info.RawResponse->GetReasonPhrase() : "Unknown");
    }
}

void AzureObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto client_ptr = client.get();
    try
    {
        LOG_TEST(log, "Removing single object: {}", object.remote_path);
        auto delete_info = client_ptr->DeleteBlob(object.remote_path);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        /// If object doesn't exist...
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return;
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

void AzureObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    auto client_ptr = client.get();
    for (const auto & object : objects)
    {
        try
        {
            auto delete_info = client_ptr->DeleteBlob(object.remote_path);
        }
        catch (const Azure::Storage::StorageException & e)
        {
            /// If object doesn't exist...
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                return;
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

}


ObjectMetadata AzureObjectStorage::getObjectMetadata(const std::string & path) const
{
    auto client_ptr = client.get();
    auto blob_client = client_ptr->GetBlobClient(path);
    auto properties = blob_client.GetProperties().Value;
    ObjectMetadata result;
    result.size_bytes = properties.BlobSize;
    if (!properties.Metadata.empty())
    {
        result.attributes.emplace();
        for (const auto & [key, value] : properties.Metadata)
            (*result.attributes)[key] = value;
    }
    result.last_modified.emplace(static_cast<std::chrono::system_clock::time_point>(properties.LastModified).time_since_epoch().count());
    return result;
}

void AzureObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = client.get();
    auto dest_blob_client = client_ptr->GetBlobClient(object_to.remote_path);
    auto source_blob_client = client_ptr->GetBlobClient(object_from.remote_path);

    Azure::Storage::Blobs::CopyBlobFromUriOptions copy_options;
    if (object_to_attributes.has_value())
    {
        for (const auto & [key, value] : *object_to_attributes)
            copy_options.Metadata[key] = value;
    }

    dest_blob_client.CopyFromUri(source_blob_client.GetUrl(), copy_options);
}

void AzureObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_settings = getAzureBlobStorageSettings(config, config_prefix, context);
    settings.set(std::move(new_settings));
    /// We don't update client
}


std::unique_ptr<IObjectStorage> AzureObjectStorage::cloneObjectStorage(const std::string &, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    return std::make_unique<AzureObjectStorage>(
        name,
        getAzureBlobContainerClient(config, config_prefix),
        getAzureBlobStorageSettings(config, config_prefix, context)
    );
}

}

#endif
