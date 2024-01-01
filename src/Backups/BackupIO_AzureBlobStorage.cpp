#include <Backups/BackupIO_AzureBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE
#include <Common/quoteString.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <IO/HTTPHeaderEntries.h>
#include <Storages/StorageAzureBlobCluster.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int LOGICAL_ERROR;
}

//using AzureClientPtr = std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient>;

BackupReaderAzureBlobStorage::BackupReaderAzureBlobStorage(
    StorageAzureBlob::Configuration configuration_,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupReaderDefault(read_settings_, write_settings_, &Poco::Logger::get("BackupReaderAzureBlobStorage"))
    , data_source_description{DataSourceType::AzureBlobStorage, configuration_.container, false, false}
    , configuration(configuration_)
{
    client = StorageAzureBlob::createClient(configuration, /* is_read_only */ false);
    settings = StorageAzureBlob::createSettingsAsSharedPtr(context_);
    auto settings_as_unique_ptr = StorageAzureBlob::createSettings(context_);
    object_storage = std::make_unique<AzureObjectStorage>("BackupReaderAzureBlobStorage",
                                                          std::make_unique<Azure::Storage::Blobs::BlobContainerClient>(*client.get()),
                                                          std::move(settings_as_unique_ptr));
}

BackupReaderAzureBlobStorage::~BackupReaderAzureBlobStorage() = default;

bool BackupReaderAzureBlobStorage::fileExists(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    return object_storage->exists(StoredObject(key));
}

UInt64 BackupReaderAzureBlobStorage::getFileSize(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    ObjectMetadata object_metadata = object_storage->getObjectMetadata(key);
    return object_metadata.size_bytes;
}

std::unique_ptr<SeekableReadBuffer> BackupReaderAzureBlobStorage::readFile(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client, key, read_settings, settings->max_single_read_retries,
        settings->max_single_download_retries);
}

void BackupReaderAzureBlobStorage::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                    DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    LOG_INFO(&Poco::Logger::get("BackupReaderAzureBlobStorage"), "Enter copyFileToDisk");

    /// Use the native copy as a more optimal way to copy a file from AzureBlobStorage to AzureBlobStorage if it's possible.
    /// We don't check for `has_throttling` here because the native copy almost doesn't use network.
    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    if (destination_data_source_description.sameKind(data_source_description)
        && (destination_data_source_description.is_encrypted == encrypted_in_backup))
    {
        LOG_TRACE(log, "Copying {} from AzureBlobStorage to disk {}", path_in_backup, destination_disk->getName());
        auto write_blob_function = [&](const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes) -> size_t
        {
            /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
            if (blob_path.size() != 2 || mode != WriteMode::Rewrite)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                blob_path.size(), mode);

            std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> dest_client;
            if (configuration.container == blob_path[1])
            {
                dest_client = client;
            }
            else
            {
                StorageAzureBlob::Configuration dest_configuration = configuration;
                dest_configuration.container = blob_path[1];
                dest_configuration.blob_path = blob_path[0];
                dest_client = StorageAzureBlob::createClient(dest_configuration, /* is_read_only */ false);
            }


            copyAzureBlobStorageFile(
                client,
                dest_client,
                configuration.container,
                fs::path(configuration.blob_path) / path_in_backup,
                0,
                file_size,
                /* dest_bucket= */ blob_path[1],
                /* dest_key= */ blob_path[0],
                settings,
                read_settings,
                object_attributes,
                threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupReaderAzureBlobStorage"),
                /* for_disk_azure_blob_storage= */ true);

            return file_size;
        };

        destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
        return; /// copied!
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterAzureBlobStorage::BackupWriterAzureBlobStorage(
    StorageAzureBlob::Configuration configuration_,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupWriterDefault(read_settings_, write_settings_, &Poco::Logger::get("BackupWriterAzureBlobStorage"))
    , data_source_description{DataSourceType::AzureBlobStorage,configuration_.container, false, false}
    , configuration(configuration_)
{
    client = StorageAzureBlob::createClient(configuration, /* is_read_only */ false);
    settings = StorageAzureBlob::createSettingsAsSharedPtr(context_);
    auto settings_as_unique_ptr = StorageAzureBlob::createSettings(context_);
    object_storage = std::make_unique<AzureObjectStorage>("BackupWriterAzureBlobStorage",
                                                          std::make_unique<Azure::Storage::Blobs::BlobContainerClient>(*client.get()),
                                                                  std::move(settings_as_unique_ptr));
}

void BackupWriterAzureBlobStorage::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                      bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Use the native copy as a more optimal way to copy a file from AzureBlobStorage to AzureBlobStorage if it's possible.
    auto source_data_source_description = src_disk->getDataSourceDescription();
    if (source_data_source_description.sameKind(data_source_description) && (source_data_source_description.is_encrypted == copy_encrypted))
    {
        /// getBlobPath() can return more than 3 elements if the file is stored as multiple objects in AzureBlobStorage bucket.
        /// In this case we can't use the native copy.
        if (auto blob_path = src_disk->getBlobPath(src_path); blob_path.size() == 2)
        {

            std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> src_client;
            if (configuration.container == blob_path[1])
            {
                src_client = client;
            }
            else
            {
                StorageAzureBlob::Configuration src_configuration = configuration;
                src_configuration.container = blob_path[1];
                src_configuration.blob_path = blob_path[0];
                src_client = StorageAzureBlob::createClient(src_configuration, /* is_read_only */ false);
            }

            LOG_TRACE(log, "Copying file {} from disk {} to AzureBlobStorag", src_path, src_disk->getName());
            copyAzureBlobStorageFile(
                src_client,
                client,
                /* src_bucket */ blob_path[1],
                /* src_key= */ blob_path[0],
                start_pos,
                length,
                configuration.container,
                fs::path(configuration.blob_path) / path_in_backup,
                settings,
                read_settings,
                {},
                threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"));
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterAzureBlobStorage::copyFile(const String & destination, const String & source, size_t size)
{
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> src_client;
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> dest_client;
    StorageAzureBlob::Configuration src_configuration = configuration;
    src_configuration.container = source;
    src_client = StorageAzureBlob::createClient(src_configuration, /* is_read_only */ false);

    StorageAzureBlob::Configuration dest_configuration = configuration;
    dest_configuration.container = destination;
    dest_client = StorageAzureBlob::createClient(dest_configuration, /* is_read_only */ false);

    LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);
    copyAzureBlobStorageFile(
       src_client,
       dest_client,
       configuration.container,
       fs::path(configuration.blob_path),
       0,
       size,
       /* dest_bucket= */ destination,
       /* dest_key= */ configuration.blob_path,
       settings,
       read_settings,
       {},
       threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupRDAzure"),
       /* for_disk_azure_blob_storage= */ true);
}

void BackupWriterAzureBlobStorage::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    copyDataToAzureBlobStorageFile(create_read_buffer, start_pos, length, client, configuration.container, path_in_backup, settings, {},
                     threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWRAzure"));
}

BackupWriterAzureBlobStorage::~BackupWriterAzureBlobStorage() = default;

bool BackupWriterAzureBlobStorage::fileExists(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    LOG_INFO(&Poco::Logger::get("BackupWriterAzureBlobStorage"), "Result fileExists   {} ", object_storage->exists(StoredObject(key)));

    return object_storage->exists(StoredObject(key));
}

UInt64 BackupWriterAzureBlobStorage::getFileSize(const String & file_name)
{
    LOG_INFO(&Poco::Logger::get("BackupWriterAzureBlobStorage"), "Enter getFileSize");
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    RelativePathsWithMetadata children;
    object_storage->listObjects(key,children,/*max_keys*/0);
    if (children.empty())
        throw Exception(ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Object must exist");
    return children[0].metadata.size_bytes;
}

std::unique_ptr<ReadBuffer> BackupWriterAzureBlobStorage::readFile(const String & file_name, size_t /*expected_file_size*/)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }

    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client, key, read_settings, settings->max_single_read_retries,
        settings->max_single_download_retries);
}

std::unique_ptr<WriteBuffer> BackupWriterAzureBlobStorage::writeFile(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    return std::make_unique<WriteBufferFromAzureBlobStorage>(
        client,
        key,
        settings->max_single_part_upload_size,
        DBMS_DEFAULT_BUFFER_SIZE,
        write_settings);
}

void BackupWriterAzureBlobStorage::removeFile(const String & file_name)
{
    String key;
    if (startsWith(file_name, "."))
    {
        key= configuration.blob_path + file_name;
    }
    else
    {
        key = file_name;
    }
    StoredObject object(key);
    object_storage->removeObjectIfExists(object);
}

void BackupWriterAzureBlobStorage::removeFiles(const Strings & keys)
{
    StoredObjects objects;
    for (const auto & key : keys)
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);

}

void BackupWriterAzureBlobStorage::removeFilesBatch(const Strings & keys)
{
    StoredObjects objects;
    for (const auto & key : keys)
        objects.emplace_back(key);

    object_storage->removeObjectsIfExist(objects);
}

}

#endif
