#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>
#include <optional>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>

namespace DB
{

/// Writes an object to a Microsoft Fabric / OneLake (ADLS Gen2) endpoint.
///
/// The final target path is never created, truncated or removed by this buffer except by a single
/// conditional atomic rename that publishes complete content: all data goes to a staging sibling
/// object first. So the target is never left partial, and a failed write leaves whatever it held before
/// untouched, with one exception: when the rename itself reports an unknown outcome, the target holds one
/// complete object, either its previous contents or the new ones.
class WriteBufferFromAzureDataLakeStorage : public WriteBufferFromFileBase
{
public:
    WriteBufferFromAzureDataLakeStorage(
        const AzureBlobStorage::Endpoint & endpoint_,
        const AzureBlobStorage::AuthMethod & auth_method_,
        const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
        const String & blob_path_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
        std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
        const String & container_for_logging_ = {},
        BlobStorageLogWriterPtr blob_log_ = {});

    ~WriteBufferFromAzureDataLakeStorage() override;

    void nextImpl() override;
    void finalizeImpl() override;
    void preFinalize() override;
    void cancelImpl() noexcept override;
    std::string getFileName() const override { return blob_path; }
    void sync() override { next(); }

private:
    void ensureCreated();
    void appendBufferedData();
    void flushStaging();
    /// Rename staging onto the final path. Emits `commit status unknown` when the outcome is ambiguous.
    void publish();
    /// Best-effort removal of the staging object. Safe to call from a destructor.
    void cleanupStaging() noexcept;
    void runWithRetries(
        const std::function<void()> & op,
        const char * what,
        BlobStorageLogElement::EventType event_type,
        size_t data_size);
    /// Retries only a 403 while provisioning access, for operations that must not be repeated blindly.
    void runOnceRetryingForbidden(
        const std::function<void()> & op,
        const char * what,
        BlobStorageLogElement::EventType event_type);

    /// Point file_client and flush_client at the current staging_path.
    void buildStagingClients();

    LoggerPtr log;

    /// Container-scoped clients. RenameFile is not on DataLakeFileClient, and all staging paths are
    /// composed off these so create, flush, rename and delete provably address the same object.
    /// `staging_fs_client` keeps the caller's SDK retries; `publish_client` has them disabled
    /// because neither the guarded flush nor the rename may be re-sent behind our back.
    Azure::Storage::Files::DataLake::DataLakeFileSystemClient staging_fs_client;
    Azure::Storage::Files::DataLake::DataLakeFileSystemClient publish_client;
    /// Path of the container within the endpoint. RenameFile otherwise defaults the destination
    /// file system to the first path segment, which is wrong when the URL also carries the account.
    const std::string publish_filesystem_path;
    /// Path of the final target and of its staging sibling, both prefixed as the endpoint requires.
    /// The staging path is regenerated if the name turns out to be taken.
    const std::string blob_path;
    const std::string prefixed_blob_path;
    std::string staging_path;
    /// Targets the staging object; the final path is written only by the rename in `publish`.
    std::optional<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client;
    /// The same staging object without SDK retries, for the single-shot guarded flush.
    std::optional<Azure::Storage::Files::DataLake::DataLakeFileClient> flush_client;
    /// ETag of the staging object: from its creation, then refreshed by the flush. Guards the flush,
    /// the rename source and the cleanup delete, so only the object we created can be published.
    Azure::ETag staging_etag;
    const WriteSettings write_settings;
    const size_t max_unexpected_write_error_retries;

    bool file_created = false;
    bool is_prefinalized = false;
    bool published = false;
    /// The rename was issued but its outcome is unknown, so the target may hold either object.
    bool publish_outcome_unknown = false;
    int64_t bytes_appended = 0;

    char fake_buffer_when_prefinalized[1] = {};

    String container_for_logging;
    BlobStorageLogWriterPtr blob_log;
};

Azure::Storage::Files::DataLake::DataLakeFileClient makeAdlsGen2FileClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    const String & blob_path);

/// Container-scoped client used for staging and for the publishing rename. Paths passed to it must be
/// composed the same way as for makeAdlsGen2FileClient, i.e. prefixed with the endpoint prefix.
Azure::Storage::Files::DataLake::DataLakeFileSystemClient makeAdlsGen2FileSystemClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    bool disable_sdk_retries);

bool isAdlsGen2Endpoint(const AzureBlobStorage::Endpoint & endpoint);

/// Path of the staging sibling for `blob_path`, or an empty string if no suffix fits the key limit.
/// Exposed for unit testing.
String makeAdlsGen2StagingPath(const String & blob_path, const String & random_suffix);

/// Build the ADLS Gen2 (DFS) write URL for a blob path. Exposed for unit testing.
String buildAdlsGen2FileUrl(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path);

}

#endif
