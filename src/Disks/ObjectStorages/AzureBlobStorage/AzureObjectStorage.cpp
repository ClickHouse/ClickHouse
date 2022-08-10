#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

#if USE_AZURE_BLOB_STORAGE

#include <IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/WriteBufferFromAzureBlobStorage.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int UNSUPPORTED_METHOD;
}


AzureObjectStorage::AzureObjectStorage(
    FileCachePtr && cache_,
    const String & name_,
    AzureClientPtr && client_,
    SettingsPtr && settings_)
    : IObjectStorage(std::move(cache_))
    , name(name_)
    , client(std::move(client_))
    , settings(std::move(settings_))
{
}

bool AzureObjectStorage::exists(const std::string & uri) const
{
    auto client_ptr = client.get();

    /// What a shame, no Exists method...
    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = uri;
    options.PageSizeHint = 1;

    auto blobs_list_response = client_ptr->ListBlobs(options);
    auto blobs_list = blobs_list_response.Blobs;

    for (const auto & blob : blobs_list)
    {
        if (uri == blob.Name)
            return true;
    }

    return false;
}

std::unique_ptr<SeekableReadBuffer> AzureObjectStorage::readObject( /// NOLINT
    const std::string & path,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = settings.get();

    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client.get(), path, settings_ptr->max_single_read_retries,
        settings_ptr->max_single_download_retries, read_settings.remote_fs_buffer_size);
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorage::readObjects( /// NOLINT
    const std::string & common_path_prefix,
    const BlobsPathToSize & blobs_to_read,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = settings.get();
    auto reader_impl = std::make_unique<ReadBufferFromAzureBlobStorageGather>(
        client.get(), common_path_prefix, blobs_to_read,
        settings_ptr->max_single_read_retries, settings_ptr->max_single_download_retries, read_settings);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, read_settings, std::move(reader_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(reader_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings_ptr->min_bytes_for_seek);
    }
}

/// Open the file for write and return WriteBufferFromFileBase object.
std::unique_ptr<WriteBufferFromFileBase> AzureObjectStorage::writeObject( /// NOLINT
    const std::string & path,
    WriteMode mode,
    std::optional<ObjectAttributes>,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings &)
{
    if (mode != WriteMode::Rewrite)
        throw Exception("Azure storage doesn't support append", ErrorCodes::UNSUPPORTED_METHOD);

    auto buffer = std::make_unique<WriteBufferFromAzureBlobStorage>(
        client.get(),
        path,
        settings.get()->max_single_part_upload_size,
        buf_size);

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(buffer), std::move(finalize_callback), path);
}

void AzureObjectStorage::listPrefix(const std::string & path, BlobsPathToSize & children) const
{
    auto client_ptr = client.get();

    Azure::Storage::Blobs::ListBlobsOptions blobs_list_options;
    blobs_list_options.Prefix = path;

    auto blobs_list_response = client_ptr->ListBlobs(blobs_list_options);
    auto blobs_list = blobs_list_response.Blobs;

    for (const auto & blob : blobs_list)
        children.emplace_back(blob.Name, blob.BlobSize);
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void AzureObjectStorage::removeObject(const std::string & path)
{
    auto client_ptr = client.get();
    auto delete_info = client_ptr->DeleteBlob(path);
    if (!delete_info.Value.Deleted)
        throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file in AzureBlob Storage: {}", path);
}

void AzureObjectStorage::removeObjects(const std::vector<std::string> & paths)
{
    auto client_ptr = client.get();
    for (const auto & path : paths)
    {
        auto delete_info = client_ptr->DeleteBlob(path);
        if (!delete_info.Value.Deleted)
            throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file in AzureBlob Storage: {}", path);
    }
}

void AzureObjectStorage::removeObjectIfExists(const std::string & path)
{
    auto client_ptr = client.get();
    auto delete_info = client_ptr->DeleteBlob(path);
}

void AzureObjectStorage::removeObjectsIfExist(const std::vector<std::string> & paths)
{
    auto client_ptr = client.get();
    for (const auto & path : paths)
        auto delete_info = client_ptr->DeleteBlob(path);
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
    result.last_modified.emplace(properties.LastModified.time_since_epoch().count());
    return result;
}

void AzureObjectStorage::copyObject( /// NOLINT
    const std::string & object_from,
    const std::string & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = client.get();
    auto dest_blob_client = client_ptr->GetBlobClient(object_to);
    auto source_blob_client = client_ptr->GetBlobClient(object_from);
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
        nullptr,
        name,
        getAzureBlobContainerClient(config, config_prefix),
        getAzureBlobStorageSettings(config, config_prefix, context)
    );
}

}

#endif
