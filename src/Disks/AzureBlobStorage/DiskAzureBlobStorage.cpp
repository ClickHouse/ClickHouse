#include <Disks/AzureBlobStorage/DiskAzureBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/RemoteDisksCommon.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Common/getRandomASCIIString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
}


DiskAzureBlobStorageSettings::DiskAzureBlobStorageSettings(
    UInt64 max_single_part_upload_size_,
    UInt64 min_bytes_for_seek_,
    int max_single_read_retries_,
    int max_single_download_retries_,
    int thread_pool_size_) :
    max_single_part_upload_size(max_single_part_upload_size_),
    min_bytes_for_seek(min_bytes_for_seek_),
    max_single_read_retries(max_single_read_retries_),
    max_single_download_retries(max_single_download_retries_),
    thread_pool_size(thread_pool_size_) {}


class AzureBlobStoragePathKeeper : public RemoteFSPathKeeper
{
public:
    /// RemoteFSPathKeeper constructed with a placeholder argument for chunk_limit, it is unused in this class
    AzureBlobStoragePathKeeper() : RemoteFSPathKeeper(1000) {}

    void addPath(const String & path) override
    {
        paths.push_back(path);
    }

    std::vector<String> paths;
};


DiskAzureBlobStorage::DiskAzureBlobStorage(
    const String & name_,
    DiskPtr metadata_disk_,
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    SettingsPtr settings_,
    GetDiskSettings settings_getter_) :
    IDiskRemote(name_, "", metadata_disk_, nullptr, "DiskAzureBlobStorage", settings_->thread_pool_size),
    blob_container_client(blob_container_client_),
    current_settings(std::move(settings_)),
    settings_getter(settings_getter_) {}


std::unique_ptr<ReadBufferFromFileBase> DiskAzureBlobStorage::readFile(
    const String & path,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings = current_settings.get();
    auto metadata = readMetadata(path);

    LOG_TEST(log, "Read from file by path: {}", backQuote(metadata_disk->getPath() + path));

    auto reader_impl = std::make_unique<ReadBufferFromAzureBlobStorageGather>(
        path, blob_container_client, metadata, settings->max_single_read_retries,
        settings->max_single_download_retries, read_settings);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, read_settings, std::move(reader_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(reader_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), current_settings.get()->min_bytes_for_seek);
    }
}


std::unique_ptr<WriteBufferFromFileBase> DiskAzureBlobStorage::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode)
{
    auto blob_path = path + "_" + getRandomASCIIString(8); /// NOTE: path contains the tmp_* prefix in the blob name

    LOG_TRACE(log, "{} to file by path: {}. AzureBlob Storage path: {}",
        mode == WriteMode::Rewrite ? "Write" : "Append", backQuote(metadata_disk->getPath() + path), blob_path);

    auto buffer = std::make_unique<WriteBufferFromAzureBlobStorage>(
        blob_container_client,
        blob_path,
        current_settings.get()->max_single_part_upload_size,
        buf_size);

    auto create_metadata_callback = [this, path, mode, blob_path] (size_t count)
    {
        readOrCreateUpdateAndStoreMetadata(path, mode, false, [blob_path, count] (Metadata & metadata) { metadata.addObject(blob_path, count); return true; });
    };

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(std::move(buffer), std::move(create_metadata_callback), path);
}


DiskType DiskAzureBlobStorage::getType() const
{
    return DiskType::AzureBlobStorage;
}


bool DiskAzureBlobStorage::isRemote() const
{
    return true;
}


bool DiskAzureBlobStorage::supportZeroCopyReplication() const
{
    return true;
}


bool DiskAzureBlobStorage::checkUniqueId(const String & id) const
{
    Azure::Storage::Blobs::ListBlobsOptions blobs_list_options;
    blobs_list_options.Prefix = id;
    blobs_list_options.PageSizeHint = 1;

    auto blobs_list_response = blob_container_client->ListBlobs(blobs_list_options);
    auto blobs_list = blobs_list_response.Blobs;

    for (const auto & blob : blobs_list)
    {
        if (id == blob.Name)
            return true;
    }

    return false;
}


void DiskAzureBlobStorage::removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper)
{
    auto * paths_keeper = dynamic_cast<AzureBlobStoragePathKeeper *>(fs_paths_keeper.get());

    if (paths_keeper)
    {
        for (const auto & path : paths_keeper->paths)
        {
            try
            {
                auto delete_info = blob_container_client->DeleteBlob(path);
                if (!delete_info.Value.Deleted)
                    throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file in AzureBlob Storage: {}", path);
            }
            catch (const Azure::Storage::StorageException & e)
            {
                LOG_INFO(log, "Caught an error while deleting file {} : {}", path, e.Message);
                throw;
            }
        }
    }
}


RemoteFSPathKeeperPtr DiskAzureBlobStorage::createFSPathKeeper() const
{
    return std::make_shared<AzureBlobStoragePathKeeper>();
}


void DiskAzureBlobStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String &, const DisksMap &)
{
    auto new_settings = settings_getter(config, "storage_configuration.disks." + name, context);

    current_settings.set(std::move(new_settings));

    if (AsyncExecutor * exec = dynamic_cast<AsyncExecutor*>(&getExecutor()))
        exec->setMaxThreads(current_settings.get()->thread_pool_size);
}

}

#endif
