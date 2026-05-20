#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>

#include <azure/core/io/body_stream.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>

#include <thread>

namespace ProfileEvents
{
    extern const Event AzureUpload;
    extern const Event DiskAzureUpload;
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

}

Azure::Storage::Files::DataLake::DataLakeFileClient WriteBufferFromAzureDataLakeStorage::buildClient(
    const String & file_url,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options)
{
    using namespace Azure::Storage::Files::DataLake;
    auto datalake_options = toDataLakeOptions(blob_client_options);

    return std::visit(
        [&]<typename T>(const T & auth) -> DataLakeFileClient
        {
            if constexpr (std::is_same_v<T, AzureBlobStorage::ConnectionString>)
                return DataLakeFileClient(file_url, datalake_options);
            else
                return DataLakeFileClient(file_url, auth, datalake_options);
        },
        auth_method);
}

WriteBufferFromAzureDataLakeStorage::WriteBufferFromAzureDataLakeStorage(
    const String & file_url_,
    const AzureBlobStorage::AuthMethod & auth_method_,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
    const String & blob_path_,
    size_t buf_size_,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
    const String & container_for_logging_,
    BlobStorageLogWriterPtr blob_log_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(getLogger("WriteBufferFromAzureDataLakeStorage"))
    , file_client(buildClient(file_url_, auth_method_, blob_client_options_))
    , blob_path(blob_path_)
    , max_unexpected_write_error_retries(settings_->max_unexpected_write_error_retries)
    , sdk_retry_initial_backoff_ms(settings_->sdk_retry_initial_backoff_ms)
    , sdk_retry_max_backoff_ms(settings_->sdk_retry_max_backoff_ms)
    , container_for_logging(container_for_logging_)
    , blob_log(std::move(blob_log_))
{
}

WriteBufferFromAzureDataLakeStorage::~WriteBufferFromAzureDataLakeStorage()
{
    if (canceled)
    {
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage was canceled. File `{}` may be left in an incomplete state.", blob_path);
        return;
    }

    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to finalize ADLS Gen2 file `{}`", blob_path));
    }
}

void WriteBufferFromAzureDataLakeStorage::runWithRetries(const std::function<void()> & op, const char * what)
{
    size_t backoff_ms = sdk_retry_initial_backoff_ms;
    for (size_t attempt = 1;; ++attempt)
    {
        try
        {
            op();
            return;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (attempt >= max_unexpected_write_error_retries)
            {
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

            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, sdk_retry_max_backoff_ms);
        }
    }
}

void WriteBufferFromAzureDataLakeStorage::ensureCreated()
{
    if (file_created)
        return;

    runWithRetries([&]() { file_client.Create(); }, "Create");
    file_created = true;
    LOG_TRACE(log, "Created ADLS Gen2 file `{}`", blob_path);
}

void WriteBufferFromAzureDataLakeStorage::appendBufferedData()
{
    const size_t to_append = offset();
    if (to_append == 0)
        return;

    ensureCreated();

    auto * data_ptr = reinterpret_cast<const uint8_t *>(working_buffer.begin());
    const int64_t offset_for_append = bytes_appended;

    ProfileEvents::increment(ProfileEvents::AzureUpload);

    runWithRetries(
        [&]()
        {
            Azure::Core::IO::MemoryBodyStream stream(data_ptr, to_append);
            file_client.Append(stream, offset_for_append);
        },
        "Append");

    bytes_appended += static_cast<int64_t>(to_append);
}

void WriteBufferFromAzureDataLakeStorage::nextImpl()
{
    appendBufferedData();
}

void WriteBufferFromAzureDataLakeStorage::preFinalize()
{
    if (is_prefinalized)
        return;
    is_prefinalized = true;

    appendBufferedData();
    ensureCreated();
    runWithRetries([&]() { file_client.Flush(bytes_appended); }, "Flush");
    LOG_TRACE(log, "Flushed ADLS Gen2 file `{}` ({} bytes)", blob_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::finalizeImpl()
{
    if (!is_prefinalized)
        preFinalize();
}

}

#endif
