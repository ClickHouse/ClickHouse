#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <base/sleep.h>

#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>

#include <azure/core/io/body_stream.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>


namespace ProfileEvents
{
    extern const Event AzureUpload;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

namespace
{

Azure::Storage::Files::DataLake::DataLakeClientOptions toDataLakeOptions(
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options)
{
    Azure::Storage::Files::DataLake::DataLakeClientOptions out;
    static_cast<Azure::Core::_internal::ClientOptions &>(out)
        = static_cast<const Azure::Core::_internal::ClientOptions &>(blob_client_options);
    return out;
}

String prefixedBlobPath(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path_)
{
    String full = endpoint.prefix;
    if (!full.empty() && !full.ends_with('/'))
        full += '/';
    full += blob_path_;
    return full;
}

String buildAdlsGen2FileUrl(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path_)
{
    Poco::URI uri(endpoint.getContainerEndpoint());
    String path = uri.getPath();
    if (!path.empty() && path.back() != '/')
        path += '/';
    path += prefixedBlobPath(endpoint, blob_path_);
    uri.setPath(path);
    return uri.toString();
}

}

constexpr size_t ADLFS_MAX_RETRIES = 10;

bool isAdlsGen2Endpoint(const AzureBlobStorage::Endpoint & endpoint)
{
    return endpoint.storage_account_url.contains("dfs.fabric.microsoft.com");
}

Azure::Storage::Files::DataLake::DataLakeFileClient makeAdlsGen2FileClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    const String & blob_path)
{
    using namespace Azure::Storage::Files::DataLake;
    auto datalake_options = toDataLakeOptions(blob_client_options);

    return std::visit(
        [&]<typename T>(const T & auth) -> DataLakeFileClient
        {
            if constexpr (std::is_same_v<T, AzureBlobStorage::ConnectionString>)
            {
                return DataLakeFileClient::CreateFromConnectionString(
                    auth.toUnderType(),
                    endpoint.container_name,
                    prefixedBlobPath(endpoint, blob_path),
                    datalake_options);
            }
            else
            {
                return DataLakeFileClient(buildAdlsGen2FileUrl(endpoint, blob_path), auth, datalake_options);
            }
        },
        auth_method);
}

WriteBufferFromAzureDataLakeStorage::WriteBufferFromAzureDataLakeStorage(
    const AzureBlobStorage::Endpoint & endpoint_,
    const AzureBlobStorage::AuthMethod & auth_method_,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
    const String & blob_path_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
    const String & container_for_logging_,
    BlobStorageLogWriterPtr blob_log_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(getLogger("WriteBufferFromAzureDataLakeStorage"))
    , file_client(makeAdlsGen2FileClient(endpoint_, auth_method_, blob_client_options_, blob_path_))
    , blob_path(blob_path_)
    , write_settings(write_settings_)
    , max_unexpected_write_error_retries(settings_->max_unexpected_write_error_retries)
    , container_for_logging(container_for_logging_)
    , blob_log(std::move(blob_log_))
{
}

WriteBufferFromAzureDataLakeStorage::~WriteBufferFromAzureDataLakeStorage()
{
    if (canceled)
    {
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage was canceled. File `{}` may be left in an incomplete state.", blob_path);
    }
    else if (!finalized)
    {
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage is not finalized in destructor. File `{}` may not be written to ADLS Gen2.", blob_path);
    }
}

void WriteBufferFromAzureDataLakeStorage::runWithRetries(
    const std::function<void()> & op,
    const char * what,
    BlobStorageLogElement::EventType event_type,
    size_t data_size)
{
    auto log_event = [&](Int32 error_code, const String & error_message, size_t elapsed_us)
    {
        if (blob_log)
            blob_log->addEvent(
                event_type,
                /* bucket */ container_for_logging,
                /* remote_path */ blob_path,
                /* local_path */ {},
                /* data_size */ data_size,
                /* elapsed_microseconds */ elapsed_us,
                error_code,
                error_message);
    };

    Stopwatch watch;
    size_t backoff_ms = 100;
    for (size_t attempt = 1; attempt < ADLFS_MAX_RETRIES; ++attempt)
    {
        LOG_INFO(log, "ADLS Gen2 {} attempt {} for `{}`", what, attempt, blob_path);
        try
        {
            op();
            log_event(/*error_code=*/ 0, /*error_message=*/ {}, watch.elapsedMicroseconds());
            LOG_INFO(log, "ADLS Gen2 {} attempt {} for `{}` succeeded", what, attempt, blob_path);
            return;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            const bool retryable = isRetryableAzureException(e, write_settings.is_initial_access_check);
            if (!retryable || attempt >= max_unexpected_write_error_retries)
            {
                log_event(static_cast<Int32>(e.StatusCode), e.Message, watch.elapsedMicroseconds());
                throw Exception(
                    ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                    "ADLS Gen2 {} failed for `{}`: HTTP {}: {}",
                    what,
                    blob_path,
                    static_cast<int>(e.StatusCode),
                    e.Message);
            }

            LOG_WARNING(log, "ADLS Gen2 {} attempt {} for `{}` failed: HTTP {}: {}. Retrying after {} ms.",
                what, attempt, blob_path, static_cast<int>(e.StatusCode), e.Message, backoff_ms);

            sleepForMilliseconds(backoff_ms);
            backoff_ms *= 2;
        }
    }
    log_event(static_cast<Int32>(ErrorCodes::AZURE_BLOB_STORAGE_ERROR), "retries exhausted", watch.elapsedMicroseconds());
    throw Exception(
        ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
        "ADLS Gen2 {} failed for `{}`",
        what,
        blob_path);
}

void WriteBufferFromAzureDataLakeStorage::ensureCreated()
{
    if (file_created)
        return;

    Azure::Storage::Files::DataLake::CreateFileOptions create_options;
    if (!write_settings.object_storage_write_if_none_match.empty())
        create_options.AccessConditions.IfNoneMatch = Azure::ETag(write_settings.object_storage_write_if_none_match);
    if (!write_settings.object_storage_write_if_match.empty())
        create_options.AccessConditions.IfMatch = Azure::ETag(write_settings.object_storage_write_if_match);

    LOG_INFO(log, "Entering Create for ADLS Gen2 file `{}` (url={})", blob_path, file_client.GetUrl());
    runWithRetries(
        [&]() { file_client.Create(create_options); },
        "Create",
        BlobStorageLogElement::EventType::MultiPartUploadCreate,
        /*data_size=*/ 0);
    file_created = true;
    LOG_INFO(log, "Created ADLS Gen2 file `{}`", blob_path);
}

void WriteBufferFromAzureDataLakeStorage::appendBufferedData()
{
    const size_t to_append = offset();
    if (to_append == 0)
        return;

    ensureCreated();

    const auto * data_ptr = reinterpret_cast<const uint8_t *>(working_buffer.begin());
    const int64_t offset_for_append = bytes_appended;

    ProfileEvents::increment(ProfileEvents::AzureUpload);

    LOG_INFO(log, "Entering Append for `{}`: offset={}, len={}", blob_path, offset_for_append, to_append);
    runWithRetries(
        [&]()
        {
            Azure::Core::IO::MemoryBodyStream stream(data_ptr, to_append);
            file_client.Append(stream, offset_for_append);
        },
        "Append",
        BlobStorageLogElement::EventType::MultiPartUploadWrite,
        to_append);

    bytes_appended += static_cast<int64_t>(to_append);
    LOG_INFO(log, "Appended for `{}`: bytes_appended={}", blob_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::nextImpl()
{
    if (is_prefinalized)
        throw Exception(
            ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
            "Cannot write to prefinalized buffer for ADLS Gen2, the file `{}` has already been flushed",
            blob_path);

    appendBufferedData();
}

void WriteBufferFromAzureDataLakeStorage::preFinalize()
{
    if (is_prefinalized)
        return;
    is_prefinalized = true;

    LOG_INFO(log, "Entering preFinalize for ADLS Gen2 file `{}`", blob_path);
    appendBufferedData();
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
    ensureCreated();
    runWithRetries(
        [&]() { file_client.Flush(bytes_appended); },
        "Flush",
        BlobStorageLogElement::EventType::MultiPartUploadComplete,
        /*data_size=*/ 0);
    LOG_INFO(log, "Flushed ADLS Gen2 file `{}` ({} bytes)", blob_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::finalizeImpl()
{
    if (!is_prefinalized)
        preFinalize();
}

}

#endif
