#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

#if CLICKHOUSE_CLOUD
/// Connection params for reading the objects a lightweight snapshot references, built from the endpoint
/// and object namespace its manifest records for the source disk. For Azure that namespace is `<container>`
/// or `<container>/<prefix>` when the disk endpoint has a path below the container
/// (`AzureObjectStorage::getObjectsNamespace`), while the recorded object keys are relative to the prefix, so
/// it is split back into a container and a blob prefix: used as a container name, `<container>/<prefix>`
/// gives every container-level request extra path segments, which Azure rejects with `400 InvalidUri`.
/// The objects are read from the recorded endpoint with the backup's credential, as the S3 reader does. A
/// connection string is pointed there by replacing its `BlobEndpoint`, since `CreateFromConnectionString()`
/// takes the endpoint from the connection string alone; its key or SAS then authorises the reads, or Azure
/// refuses them if it belongs to another account. Query parameters of the recorded endpoint (a SAS of the
/// source disk) are dropped, and an endpoint recorded as a connection string is reduced to its service URL.
AzureBlobStorage::ConnectionParams makeSnapshotSourceConnectionParams(
    const AzureBlobStorage::ConnectionParams & backup_connection_params, const String & endpoint, const String & blob_namespace);
#endif
  
/// The generation (`ETag`) of the blob `blob_path` of `src_object_storage`, which is about to be copied
/// into a backup, whole or in part, as a file whose blob is `expected_size` bytes long. The size and
/// the generation are taken with one `HEAD`, so they describe the same generation of the blob: the size the disk reported was measured
/// earlier, and a blob replaced in between could be of another size, which a copy pinned to the new
/// generation would then copy in full under the old size. A blob of another size is refused with
/// `FILE_CHANGED_DURING_READ`, and a blob whose generation the endpoint does not report with
/// `AZURE_BLOB_STORAGE_ERROR`: a copy that cannot be pinned to a generation is not made.
String headSourceBlobOfBackupCopy(const IObjectStorage & src_object_storage, const String & blob_path, size_t expected_size);

/// A read buffer over the blob `blob_path` of `container`, pinned to the generation `etag` that
/// `headSourceBlobOfBackupCopy` selected and bounded by the size `source_size` the same `HEAD`
/// measured. A copy of a part of a single Azure blob into a backup cannot be made by the
/// Azure-to-Azure copy and is read through a buffer; that read goes to the blob itself rather than
/// through the generic disk read, because the `StoredObject`s of a `plain` or `plain_rewritable`
/// disk carry no `ETag`, so a generic read takes whatever generation each request happens to be
/// answered with, and an incremental backup could copy the bytes of two generations of one key.
std::unique_ptr<ReadBufferFromFileBase> readSourceBlobOfBackupCopy(
    std::shared_ptr<const AzureBlobStorage::ContainerClient> client,
    const String & container,
    const String & blob_path,
    size_t source_size,
    const String & etag,
    const ReadSettings & read_settings,
    const AzureBlobStorage::RequestSettings & request_settings);

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
    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name, std::optional<size_t> expected_file_size) override;
    String getFileGeneration(const String & file_name) override;
    std::unique_ptr<ReadBufferFromFileBase> readFilePinnedToGeneration(
        const String & file_name, std::optional<size_t> expected_file_size, const String & generation) override;

    void copyFileToDisk(
        const String & path_in_backup,
        size_t file_size,
        bool encrypted_in_backup,
        DiskPtr destination_disk,
        const String & destination_path,
        WriteMode write_mode) override;

    std::map<String, String> getSerializedSettings() const override;

private:
    const DataSourceDescription data_source_description;
    std::shared_ptr<const AzureBlobStorage::ContainerClient> client;
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
    std::unique_ptr<WriteBuffer> writeFileIfNotExists(const String & file_name) override;

    void copyDataToFile(
        const String & path_in_backup,
        const CreateReadBufferFunction & create_read_buffer,
        UInt64 start_pos,
        UInt64 length) override;

    void copyFileFromDisk(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

    std::map<String, String> getSerializedSettings() const override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;

    const DataSourceDescription data_source_description;
    std::shared_ptr<const AzureBlobStorage::ContainerClient> client;
    AzureBlobStorage::ConnectionParams connection_params;
    String blob_path;
    std::unique_ptr<AzureObjectStorage> object_storage;
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings;
};

}

#endif
