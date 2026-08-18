#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <base/sleep.h>

#include <Poco/URI.h>

#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>

#include <azure/core/io/body_stream.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <azure/core/credentials/credentials.hpp>


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

}

String buildAdlsGen2FileUrl(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path_)
{
    Poco::URI uri(endpoint.getContainerEndpoint());

    /// OneLake serves reads from the Blob host but writes only from the DFS host,
    /// so retarget the Fabric Blob host to DFS for the write client.
    const String blob_suffix = ".blob.fabric.microsoft.com";
    const String dfs_suffix = ".dfs.fabric.microsoft.com";
    String host = uri.getHost();
    if (host.ends_with(blob_suffix))
        uri.setHost(host.substr(0, host.size() - blob_suffix.size()) + dfs_suffix);

    String path = uri.getPath();
    if (!path.empty() && path.back() != '/')
        path += '/';
    path += prefixedBlobPath(endpoint, blob_path_);
    uri.setPath(path);
    return uri.toString();
}

constexpr size_t ADLFS_MAX_RETRIES = 10;

bool isAdlsGen2Endpoint(const AzureBlobStorage::Endpoint & endpoint)
{
    /// Only real Microsoft Fabric / OneLake hosts, matched at a label boundary.
    const String host = Poco::URI(endpoint.storage_account_url).getHost();
    return host.ends_with(".dfs.fabric.microsoft.com") || host.ends_with(".blob.fabric.microsoft.com");
}

Azure::Storage::Files::DataLake::DataLakeFileClient makeAdlsGen2FileClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    const String & blob_path)
{
    using namespace Azure::Storage::Files::DataLake;
    auto datalake_options = toDataLakeOptions(blob_client_options);

    if (!endpoint.sas_auth.empty() || !endpoint.additional_params.empty())
        return DataLakeFileClient(buildAdlsGen2FileUrl(endpoint, blob_path), datalake_options);

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
    size_t attempt = 1;

    /// Shared budget/backoff tail for both failure kinds: give up (log + throw) once the retry
    /// budget is spent, otherwise warn and sleep before the next attempt.
    auto retry_or_throw = [&](Int32 code, const String & msg, const char * kind)
    {
        if (attempt >= max_unexpected_write_error_retries)
        {
            log_event(code, msg, watch.elapsedMicroseconds());
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "ADLS Gen2 {} failed for `{}`: {}: {}", what, blob_path, kind, msg);
        }

        LOG_WARNING(log, "ADLS Gen2 {} attempt {} for `{}` failed: {}: {}. Retrying after {} ms.",
            what, attempt, blob_path, kind, msg, backoff_ms);

        sleepForMilliseconds(backoff_ms);
        backoff_ms *= 2;
    };

    for (; attempt < ADLFS_MAX_RETRIES; ++attempt)
    {
        LOG_TRACE(log, "ADLS Gen2 {} attempt {} for `{}`", what, attempt, blob_path);
        try
        {
            op();
            log_event(/*error_code=*/ 0, /*error_message=*/ {}, watch.elapsedMicroseconds());
            LOG_TRACE(log, "ADLS Gen2 {} attempt {} for `{}` succeeded", what, attempt, blob_path);
            return;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            /// A non-retryable HTTP error fails immediately (keeping the numeric status in the
            /// message); a retryable one goes through the shared budget/backoff tail.
            if (!isRetryableAzureException(e))
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

            retry_or_throw(static_cast<Int32>(e.StatusCode), e.Message, "HTTP error");
        }
        catch (const Azure::Core::Credentials::AuthenticationException & e)
        {
            /// Credential/RBAC token-acquisition failures are transient during the auth-propagation
            /// window (same rationale as 403 in isRetryableAzureException), so retry within budget.
            retry_or_throw(static_cast<Int32>(ErrorCodes::AZURE_BLOB_STORAGE_ERROR), e.what(), "authentication error");
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

    LOG_TRACE(log, "Entering Create for ADLS Gen2 file `{}`", blob_path);
    runWithRetries(
        [&]() { file_client.Create(create_options); },
        "Create",
        BlobStorageLogElement::EventType::MultiPartUploadCreate,
        /*data_size=*/ 0);
    file_created = true;
    LOG_DEBUG(log, "Created ADLS Gen2 file `{}`", blob_path);
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

    LOG_TRACE(log, "Entering Append for `{}`: offset={}, len={}", blob_path, offset_for_append, to_append);
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
    LOG_TRACE(log, "Appended for `{}`: bytes_appended={}", blob_path, bytes_appended);
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

    LOG_DEBUG(log, "Entering preFinalize for ADLS Gen2 file `{}`", blob_path);
    next();

    is_prefinalized = true;
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
    ensureCreated();
    runWithRetries(
        [&]() { file_client.Flush(bytes_appended); },
        "Flush",
        BlobStorageLogElement::EventType::MultiPartUploadComplete,
        /*data_size=*/ 0);
    LOG_DEBUG(log, "Flushed ADLS Gen2 file `{}` ({} bytes)", blob_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::finalizeImpl()
{
    if (!is_prefinalized)
        preFinalize();
}

void WriteBufferFromAzureDataLakeStorage::cancelImpl() noexcept
{
    WriteBufferFromFileBase::cancelImpl();

    if (file_created && !finalized)
    {
        try
        {
            LOG_INFO(log, "Deleting incomplete ADLS Gen2 file `{}` after cancel", blob_path);
            file_client.DeleteIfExists();
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to delete incomplete ADLS Gen2 file `{}`", blob_path));
        }
    }
}

}

#endif
