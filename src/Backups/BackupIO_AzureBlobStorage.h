#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>


namespace DB
{

/// Represents a backup stored to Azure
class BackupReaderAzureBlobStorage : public BackupReaderDefault
{
public:
    BackupReaderAzureBlobStorage(
        const AzureBlobStorage::ConnectionParams & connection_params_,
        const String & blob_path_,
        bool allow_azure_native_copy,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_);

    ~BackupReaderAzureBlobStorage() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;

    void copyFileToDisk(
        const String & path_in_backup,
        size_t file_size,
        bool encrypted_in_backup,
        DiskPtr destination_disk,
        const String & destination_path,
        WriteMode write_mode) override;

private:
    const DataSourceDescription data_source_description;
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client;
    AzureBlobStorage::ConnectionParams connection_params;
    String blob_path;
    std::unique_ptr<AzureObjectStorage> object_storage;
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings;
};

class BackupWriterAzureBlobStorage : public BackupWriterDefault
{
public:
    BackupWriterAzureBlobStorage(
        const AzureBlobStorage::ConnectionParams & connection_params_,
        const String & blob_path_,
        bool allow_azure_native_copy,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_,
        bool attempt_to_create_container);

    ~BackupWriterAzureBlobStorage() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyDataToFile(
        const String & path_in_backup,
        const CreateReadBufferFunction & create_read_buffer,
        UInt64 start_pos,
        UInt64 length) override;

    void copyFileFromDisk(
        const String & path_in_backup,
        DiskPtr src_disk,
        const String & src_path,
        bool copy_encrypted,
        UInt64 start_pos,
        UInt64 length) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;
    void removeEmptyDirectories() override {}

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
    void removeFilesBatch(const Strings & file_names);

    const DataSourceDescription data_source_description;
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client;
    AzureBlobStorage::ConnectionParams connection_params;
    String blob_path;
    std::unique_ptr<AzureObjectStorage> object_storage;
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings;
};

}

#endif
