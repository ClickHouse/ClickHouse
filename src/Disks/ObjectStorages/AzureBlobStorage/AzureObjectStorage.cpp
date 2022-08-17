#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/getRandomASCIIString.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
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
    const String & name_,
    AzureClientPtr && client_,
    SettingsPtr && settings_)
    : name(name_)
    , client(std::move(client_))
    , settings(std::move(settings_))
{
}

std::string AzureObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    return getRandomASCIIString();
}

bool AzureObjectStorage::exists(const StoredObject & object) const
{
    auto client_ptr = client.get();

    /// What a shame, no Exists method...
    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = object.absolute_path;
    options.PageSizeHint = 1;

    auto blobs_list_response = client_ptr->ListBlobs(options);
    auto blobs_list = blobs_list_response.Blobs;

    for (const auto & blob : blobs_list)
    {
        if (object.absolute_path == blob.Name)
            return true;
    }

    return false;
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = settings.get();

    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client.get(), object.absolute_path, patchSettings(read_settings), settings_ptr->max_single_read_retries,
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

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromAzureBlobStorage>(
            client.get(),
            path,
            disk_read_settings,
            settings_ptr->max_single_read_retries,
            settings_ptr->max_single_download_retries,
            /* use_external_buffer */true,
            read_until_position);
    };

    auto reader_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        objects,
        disk_read_settings);

    if (disk_read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, disk_read_settings, std::move(reader_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(reader_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings_ptr->min_bytes_for_seek);
    }
}

/// Open the file for write and return WriteBufferFromFileBase object.
std::unique_ptr<WriteBufferFromFileBase> AzureObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes>,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (mode != WriteMode::Rewrite)
        throw Exception("Azure storage doesn't support append", ErrorCodes::UNSUPPORTED_METHOD);

    auto buffer = std::make_unique<WriteBufferFromAzureBlobStorage>(
        client.get(),
        object.absolute_path,
        settings.get()->max_single_part_upload_size,
        buf_size,
        patchSettings(write_settings));

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(buffer), std::move(finalize_callback), object.absolute_path);
}

void AzureObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
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
void AzureObjectStorage::removeObject(const StoredObject & object)
{
    const auto & path = object.absolute_path;
    auto client_ptr = client.get();
    auto delete_info = client_ptr->DeleteBlob(path);
    if (!delete_info.Value.Deleted)
        throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file in AzureBlob Storage: {}", path);
}

void AzureObjectStorage::removeObjects(const StoredObjects & objects)
{
    auto client_ptr = client.get();
    for (const auto & object : objects)
    {
        auto delete_info = client_ptr->DeleteBlob(object.absolute_path);
        if (!delete_info.Value.Deleted)
            throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file in AzureBlob Storage: {}", object.absolute_path);
    }
}

void AzureObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto client_ptr = client.get();
    auto delete_info = client_ptr->DeleteBlob(object.absolute_path);
}

void AzureObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    auto client_ptr = client.get();
    for (const auto & object : objects)
        auto delete_info = client_ptr->DeleteBlob(object.absolute_path);
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
    const StoredObject & object_from,
    const StoredObject & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = client.get();
    auto dest_blob_client = client_ptr->GetBlobClient(object_to.absolute_path);
    auto source_blob_client = client_ptr->GetBlobClient(object_from.absolute_path);

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
    applyRemoteThrottlingSettings(context);
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
