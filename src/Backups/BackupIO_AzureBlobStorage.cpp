#include <Backups/BackupIO_AzureBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE
#include <Common/quoteString.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <IO/HTTPHeaderEntries.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <Disks/IDisk.h>
#include <Disks/DiskType.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <azure/storage/blobs/blob_options.hpp>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int LOGICAL_ERROR;
}

BackupReaderAzureBlobStorage::BackupReaderAzureBlobStorage(
    const AzureBlobStorage::ConnectionParams & connection_params_,
    const String & blob_path_,
    bool allow_azure_native_copy,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderAzureBlobStorage"))
    , data_source_description{DataSourceType::ObjectStorage, ObjectStorageType::Azure, MetadataStorageType::None, connection_params_.getConnectionURL(), false, false}
    , connection_params(connection_params_)
    , blob_path(blob_path_)
{
    auto client_ptr = AzureBlobStorage::getContainerClient(connection_params, /*readonly=*/ false);
    auto settings_ptr = AzureBlobStorage::getRequestSettingsForBackup(context_->getSettingsRef(), allow_azure_native_copy);

    object_storage = std::make_unique<AzureObjectStorage>(
        "BackupReaderAzureBlobStorage",
        std::move(client_ptr),
        std::move(settings_ptr),
        connection_params.getContainer(),
        connection_params.getConnectionURL());

    client = object_storage->getAzureBlobStorageClient();
    settings = object_storage->getSettings();
}

BackupReaderAzureBlobStorage::~BackupReaderAzureBlobStorage() = default;

bool BackupReaderAzureBlobStorage::fileExists(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    return object_storage->exists(StoredObject(key));
}

UInt64 BackupReaderAzureBlobStorage::getFileSize(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    ObjectMetadata object_metadata = object_storage->getObjectMetadata(key);
    return object_metadata.size_bytes;
}

std::unique_ptr<SeekableReadBuffer> BackupReaderAzureBlobStorage::readFile(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client, key, read_settings, settings->max_single_read_retries,
        settings->max_single_download_retries);
}

void BackupReaderAzureBlobStorage::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                    DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    LOG_TRACE(log, "Source description {}, destination description {}", data_source_description.description, destination_data_source_description.description);
    if (destination_data_source_description.object_storage_type == ObjectStorageType::Azure
        && destination_data_source_description.is_encrypted == encrypted_in_backup)
    {
        LOG_TRACE(log, "Copying {} from AzureBlobStorage to disk {}", path_in_backup, destination_disk->getName());
        auto write_blob_function = [&](const Strings & dst_blob_path, WriteMode mode, const std::optional<ObjectAttributes> &) -> size_t
        {
            /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
            if (dst_blob_path.size() != 2 || mode != WriteMode::Rewrite)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                dst_blob_path.size(), mode);

            copyAzureBlobStorageFile(
                client,
                destination_disk->getObjectStorage()->getAzureBlobStorageClient(),
                connection_params.getContainer(),
                fs::path(blob_path) / path_in_backup,
                0,
                file_size,
                /* dest_container */ dst_blob_path[1],
                /* dest_path */ dst_blob_path[0],
                settings,
                read_settings,
                threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), "BackupRDAzure"));

            return file_size;
        };

        destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
        return; /// copied!
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterAzureBlobStorage::BackupWriterAzureBlobStorage(
    const AzureBlobStorage::ConnectionParams & connection_params_,
    const String & blob_path_,
    bool allow_azure_native_copy,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_,
    bool attempt_to_create_container)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterAzureBlobStorage"))
    , data_source_description{DataSourceType::ObjectStorage, ObjectStorageType::Azure, MetadataStorageType::None, connection_params_.getConnectionURL(), false, false}
    , connection_params(connection_params_)
    , blob_path(blob_path_)
{
    if (!attempt_to_create_container)
        connection_params.endpoint.container_already_exists = true;

    auto client_ptr = AzureBlobStorage::getContainerClient(connection_params, /*readonly=*/ false);
    auto settings_ptr = AzureBlobStorage::getRequestSettingsForBackup(context_->getSettingsRef(), allow_azure_native_copy);

    object_storage = std::make_unique<AzureObjectStorage>(
        "BackupWriterAzureBlobStorage",
        std::move(client_ptr),
        std::move(settings_ptr),
        connection_params.getContainer(),
        connection_params.getConnectionURL());

    client = object_storage->getAzureBlobStorageClient();
    settings = object_storage->getSettings();
}

void BackupWriterAzureBlobStorage::copyFileFromDisk(
    const String & path_in_backup,
    DiskPtr src_disk,
    const String & src_path,
    bool copy_encrypted,
    UInt64 start_pos,
    UInt64 length)
{
    /// Use the native copy as a more optimal way to copy a file from AzureBlobStorage to AzureBlobStorage if it's possible.
    auto source_data_source_description = src_disk->getDataSourceDescription();
    LOG_TRACE(log, "Source description {}, destination description {}", source_data_source_description.description, data_source_description.description);
    if (source_data_source_description.object_storage_type == ObjectStorageType::Azure
        && source_data_source_description.is_encrypted == copy_encrypted)
    {
        /// getBlobPath() can return more than 3 elements if the file is stored as multiple objects in AzureBlobStorage container.
        /// In this case we can't use the native copy.
        if (auto src_blob_path = src_disk->getBlobPath(src_path); src_blob_path.size() == 2)
        {
            LOG_TRACE(log, "Copying file {} from disk {} to AzureBlobStorag", src_path, src_disk->getName());
            copyAzureBlobStorageFile(
                src_disk->getObjectStorage()->getAzureBlobStorageClient(),
                client,
                /* src_container */ src_blob_path[1],
                /* src_path */ src_blob_path[0],
                start_pos,
                length,
                connection_params.getContainer(),
                fs::path(blob_path) / path_in_backup,
                settings,
                read_settings,
                threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), "BackupWRAzure"));
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterAzureBlobStorage::copyFile(const String & destination, const String & source, size_t size)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);
    copyAzureBlobStorageFile(
       client,
       client,
       connection_params.getContainer(),
       fs::path(blob_path)/ source,
       0,
       size,
       /* dest_container */ connection_params.getContainer(),
       /* dest_path */ destination,
       settings,
       read_settings,
       threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), "BackupWRAzure"));
}

void BackupWriterAzureBlobStorage::copyDataToFile(
    const String & path_in_backup,
    const CreateReadBufferFunction & create_read_buffer,
    UInt64 start_pos,
    UInt64 length)
{
    copyDataToAzureBlobStorageFile(
        create_read_buffer,
        start_pos,
        length,
        client,
        connection_params.getContainer(),
        fs::path(blob_path) / path_in_backup,
        settings,
        threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(),
        "BackupWRAzure"));
}

BackupWriterAzureBlobStorage::~BackupWriterAzureBlobStorage() = default;

bool BackupWriterAzureBlobStorage::fileExists(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    return object_storage->exists(StoredObject(key));
}

UInt64 BackupWriterAzureBlobStorage::getFileSize(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    RelativePathsWithMetadata children;
    object_storage->listObjects(key,children,/*max_keys*/0);
    if (children.empty())
        throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Object must exist");
    return children[0]->metadata->size_bytes;
}

std::unique_ptr<ReadBuffer> BackupWriterAzureBlobStorage::readFile(const String & file_name, size_t /*expected_file_size*/)
{
    String key = fs::path(blob_path) / file_name;
    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client, key, read_settings, settings->max_single_read_retries,
        settings->max_single_download_retries);
}

std::unique_ptr<WriteBuffer> BackupWriterAzureBlobStorage::writeFile(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    return std::make_unique<WriteBufferFromAzureBlobStorage>(
        client,
        key,
        DBMS_DEFAULT_BUFFER_SIZE,
        write_settings,
        settings,
        threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), "BackupWRAzure"));
}

void BackupWriterAzureBlobStorage::removeFile(const String & file_name)
{
    String key = fs::path(blob_path) / file_name;
    StoredObject object(key);
    object_storage->removeObjectIfExists(object);
}

void BackupWriterAzureBlobStorage::removeFiles(const Strings & file_names)
{
    StoredObjects objects;
    for (const auto & file_name : file_names)
        objects.emplace_back(fs::path(blob_path) / file_name);

    object_storage->removeObjectsIfExist(objects);

}

void BackupWriterAzureBlobStorage::removeFilesBatch(const Strings & file_names)
{
    StoredObjects objects;
    for (const auto & file_name : file_names)
        objects.emplace_back(fs::path(blob_path) / file_name);

    object_storage->removeObjectsIfExist(objects);
}

}

#endif
