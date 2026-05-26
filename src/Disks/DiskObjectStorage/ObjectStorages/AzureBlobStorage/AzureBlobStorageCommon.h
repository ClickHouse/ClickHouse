#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <azure/core/response.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>

#endif

#include <chrono>
#include <functional>
#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Common/BlobStorageLogWriter.h>
#include <IO/BufferWithOwnMemory.h>     // for Memory<>
#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#endif

namespace DB
{

struct Settings;
struct WriteSettings;

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace AzureBlobStorage
{

struct RequestSettings
{
    RequestSettings() = default;

    size_t max_single_part_upload_size = 100 * 1024 * 1024; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    size_t min_bytes_for_seek = 1024 * 1024;
    size_t max_single_read_retries = 3;
    size_t max_single_download_retries = 3;
    size_t list_object_keys_size = 1000;
    size_t min_upload_part_size = 16 * 1024 * 1024;
    size_t max_upload_part_size = 5ULL * 1024 * 1024 * 1024;
    size_t max_single_part_copy_size = 256 * 1024 * 1024;
    size_t max_unexpected_write_error_retries = 4;
    size_t rbac_warmup_interval_sec = 0;
    size_t rbac_warmup_retry_multiplier = 3;
    size_t max_inflight_parts_for_one_file = 20;
    size_t max_blocks_in_multipart_upload = 50000;
    size_t strict_upload_part_size = 0;
    size_t upload_part_size_multiply_factor = 2;
    size_t upload_part_size_multiply_parts_count_threshold = 500;
    size_t sdk_max_retries = 10;
    size_t sdk_retry_initial_backoff_ms = 10;
    size_t sdk_retry_max_backoff_ms = 1000;
    bool use_native_copy = false;
    bool check_objects_after_upload = false;
    bool read_only = false;
    size_t http_keep_alive_timeout = DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT;
    size_t http_keep_alive_max_requests = DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST;
};

struct Endpoint
{
    String storage_account_url;
    String account_name;
    String account_key;
    String container_name;
    String prefix;
    String sas_auth;
    String additional_params;
    std::optional<bool> container_already_exists;
    std::optional<bool> add_account_name_to_url;

    String getContainerEndpoint() const
    {
        String url = storage_account_url;
        if (url.ends_with('/'))
          url.pop_back();

        if (!account_name.empty() && add_account_name_to_url.value_or(true))
            url += "/" + account_name;

        if (!container_name.empty())
            url += "/" + container_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        if (!additional_params.empty())
            url += "?" + additional_params;

        return url;
    }

    String getServiceEndpoint() const
    {
        String url = storage_account_url;

        if (!account_name.empty() && add_account_name_to_url.value_or(true))
            url += "/" + account_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        if (!additional_params.empty())
            url += "?" + additional_params;

        return url;
    }
};

#if USE_AZURE_BLOB_STORAGE

using BlobClient = Azure::Storage::Blobs::BlobClient;
using BlockBlobClient = Azure::Storage::Blobs::BlockBlobClient;
using RawContainerClient = Azure::Storage::Blobs::BlobContainerClient;

using Azure::Storage::Blobs::ListBlobsOptions;
using Azure::Storage::Blobs::ListBlobsPagedResponse;
using Azure::Storage::Blobs::BlobContainerBatch;
using BlobPropertiesResponse = Azure::Response<Azure::Storage::Blobs::Models::BlobProperties>;
using BlobContainerPropertiesResponse = Azure::Response<Azure::Storage::Blobs::Models::BlobContainerProperties>;
using BlobBatchResultResponse = Azure::Response<Azure::Storage::Blobs::Models::SubmitBlobBatchResult>;
using DeleteBlobResultDeferredResponse = Azure::Storage::DeferredResponse<Azure::Storage::Blobs::Models::DeleteBlobResult>;

/// A single upload payload: an owning `Memory<>` buffer plus the number of
/// bytes actually populated. Used by the single-part upload and StageBlock
/// wrapper methods. Default-construct an empty one for the zero-byte path.
struct UploadPartData
{
    Memory<> memory;
    size_t data_size = 0;
};

/// This wrapper is the single entry point to Azure SDK.
/// Callers should NOT acquire raw `BlobClient`/`BlockBlobClient` objects —
/// use the named data-plane methods below or add new ones.
class ContainerClientWrapper
{
public:
    ContainerClientWrapper(
        RawContainerClient client_,
        String blob_prefix_,
        size_t rbac_warmup_interval_sec_ = 0,
        size_t rbac_warmup_retry_multiplier_ = 3);

    bool isInWarmupPeriod() const;

    size_t getEffectiveRetries(size_t base_retries) const;

    /// Check if client is configured for disk. Doesn't call remote Azure.
    bool IsClientForDisk() const;

    /// Create an empty batch for delete operations. Doesn't call remote Azure.
    BlobContainerBatch CreateBatch() const;

    /// Prepend blob prefix to blob name. Doesn't call remote Azure.
    String GetBlobPath(const String & blob_name) const;

    /// Get full URL for a blob. Doesn't call remote Azure.
    String GetBlobUrl(const String & blob_name) const;

    /// Add a delete operation to a batch. Doesn't call remote Azure.
    DeleteBlobResultDeferredResponse addDeleteBlobToBatch(
        BlobContainerBatch & batch,
        const String & blob_name) const;

    /// Trace counter for batch delete operations. Doesn't call remote Azure.
    void traceAzureDeleteObjects(size_t count = 1) const;

    /// Fetch container-level properties. Calls remote Azure.
    BlobContainerPropertiesResponse GetProperties() const;

    /// Fetch properties for a single blob. Calls remote Azure.
    BlobPropertiesResponse GetBlobProperties(const String & blob_name) const;

    /// List blobs matching a prefix. Calls remote Azure.
    ListBlobsPagedResponse ListBlobs(const ListBlobsOptions & options) const;

    /// Submit a batch of operations. Calls remote Azure.
    BlobBatchResultResponse SubmitBatch(const BlobContainerBatch & batch) const;

    /// Read-modify-write a single blob tag. Calls remote Azure.
    bool UpdateBlobTag(
        const String & blob_name,
        const String & tag_key,
        const String & tag_value,
        LoggerPtr log) const;

    using DownloadResponseHandler = std::function<void(Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult> &)>;

    /// Download a byte range from a blob. Calls remote Azure.
    Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult>
        downloadRange(
            const String & blob_name,
            const String & container_name,
            size_t range_offset,
            size_t range_length,
            size_t num_tries,
            LoggerPtr log,
            const BlobStorageLogWriterPtr & blob_log,
            DownloadResponseHandler response_handler = {}) const;

    /// Upload blob content in a single request. Calls remote Azure.
    void uploadSinglePart(
        const String & blob_name,
        const String & container_name,
        const uint8_t * data,
        size_t data_size,
        const WriteSettings * write_settings,
        size_t num_tries,
        LoggerPtr log,
        const BlobStorageLogWriterPtr & blob_log) const;

    /// Stage a block for multipart upload. Calls remote Azure.
    void stageBlock(
        const String & blob_name,
        const String & container_name,
        const String & block_id,
        const uint8_t * data,
        size_t data_size,
        const WriteSettings * write_settings,
        size_t num_tries,
        LoggerPtr log,
        const BlobStorageLogWriterPtr & blob_log) const;

    /// Commit staged blocks to finalize upload. Calls remote Azure.
    void commitBlockList(
        const String & blob_name,
        const String & container_name,
        const std::vector<std::string> & block_ids,
        const WriteSettings * write_settings,
        size_t num_tries,
        LoggerPtr log,
        const BlobStorageLogWriterPtr & blob_log) const;

    /// Delete a single blob. Calls remote Azure.
    void deleteBlobSingleWithBlobStorageLog(
        const String & blob_name,
        const String & container_name,
        const String & local_path,
        bool if_exists,
        const BlobStorageLogWriterPtr & blob_log,
        size_t bytes_size_for_logging) const;

    /// Synchronous server-side copy from a source URI. Calls remote Azure.
    Azure::Response<Azure::Storage::Blobs::Models::CopyBlobFromUriResult> copyBlobFromUri(
        const String & dest_blob_name,
        const String & source_uri,
        const Azure::Storage::Blobs::CopyBlobFromUriOptions & options) const
    {
        return executeWithRetry(nullptr, dest_blob_name, 1, [&](size_t)
        {
            traceAzureCopyObject();
            return client.GetBlockBlobClient(blob_prefix + dest_blob_name).CopyFromUri(source_uri, options);
        });
    }

    /// Asynchronous server-side copy: starts copy, polls until done
    Azure::Storage::Blobs::Models::BlobProperties copyBlobFromUriAsync(
        const String & dest_blob_name,
        const String & source_uri,
        const Azure::Storage::Blobs::StartBlobCopyFromUriOptions & options) const
    {
        return executeWithRetry(nullptr, dest_blob_name, 1, [&](size_t)
        {
            traceAzureCopyObject();
            auto operation = client.GetBlockBlobClient(blob_prefix + dest_blob_name)
                .StartCopyFromUri(source_uri, options);
            auto copy_response = operation.PollUntilDone(std::chrono::milliseconds(100));
            return copy_response.Value;
        });
    }

    /// Unified retry wrapper for Azure SDK network calls.
    /// Retries transient failures with exponential backoff.
    /// During RBAC warmup period, automatically boosts the retry budget.
    template <typename F>
    auto executeWithRetry(
        LoggerPtr log,
        std::string_view resource_for_logging,
        size_t num_tries,
        F && func) const -> decltype(func(size_t{0}))
    {
        if (!log)
            log = default_log;

        const size_t effective_tries = getEffectiveRetries(num_tries);

        size_t sleep_ms = 100;
        for (size_t i = 0; i <= effective_tries; ++i)
        {
            try
            {
                return func(i);
            }
            catch (const Azure::Core::RequestFailedException & e)
            {
                if (i + 1 == effective_tries || !isRetryableAzureException(e))
                    throw;
                /// TODO: should it be DEBUG at failed attempts? E.g. 403 is being logged, we want to soft-alert on it but keep retrying
                LOG_DEBUG(log, "Azure call for `{}` failed at attempt {}/{}{}: {} {}",
                          resource_for_logging, i + 1, effective_tries,
                          (effective_tries != num_tries ? fmt::format(" (warmup, base {})", num_tries) : ""),
                          e.what(), e.Message);
                sleepForMilliseconds(sleep_ms);
                sleep_ms *= 2;
            }
            catch (...)
            {
                if (getCurrentExceptionCode() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                    throw;
                if (i + 1 >= effective_tries)
                    throw;
                LOG_DEBUG(log, "Azure call for `{}` failed at attempt {}/{}{}: {}",
                          resource_for_logging, i + 1, effective_tries,
                          (effective_tries != num_tries ? fmt::format(" (warmup, base {})", num_tries) : ""),
                          getCurrentExceptionMessage(false));
                sleepForMilliseconds(sleep_ms);
                sleep_ms *= 2;
            }
        }
        UNREACHABLE();
    }

private:

    /// Get a raw BlobClient handle. Doesn't call remote Azure.
    BlobClient GetBlobClient(const String & blob_name) const;

    /// Get a raw BlockBlobClient handle. Doesn't call remote Azure.
    BlockBlobClient GetBlockBlobClient(const String & blob_name) const;

    void traceAzureListObjects(size_t count = 1) const;
    void traceAzureCopyObject() const;
    void traceAzureGetProperties() const;
    void traceAzureUpload() const;
    void traceAzureStageBlock() const;
    void traceAzureCommitBlockList() const;
    void traceAzureGetObject() const;

    static void applyAccessConditions(
        Azure::Storage::Blobs::BlobAccessConditions & access_conditions,
        const WriteSettings & write_settings);

    /// Fetch blob tags for update. Calls remote Azure.
    std::map<std::string, std::string> getBlobTagsForUpdate(const String & blob_name) const;

    /// Set blob tags. Calls remote Azure.
    void setBlobTags(const String & blob_name, const std::map<std::string, std::string> & tags) const;

    static inline const LoggerPtr default_log = getLogger("ContainerClientWrapper");

    RawContainerClient client;
    String blob_prefix;
    std::chrono::steady_clock::time_point created_at;
    size_t rbac_warmup_interval_sec;
    size_t rbac_warmup_retry_multiplier;
};

using ContainerClient = ContainerClientWrapper;
using ServiceClient = Azure::Storage::Blobs::BlobServiceClient;
using BlobClientOptions = Azure::Storage::Blobs::BlobClientOptions;

struct ConnectionParams
{
    Endpoint endpoint;
    AuthMethod auth_method;
    BlobClientOptions client_options;

    String getContainer() const { return endpoint.container_name; }
    String getConnectionURL() const;

    std::unique_ptr<ServiceClient> createForService() const;
    std::unique_ptr<ContainerClient> createForContainer(
        size_t rbac_warmup_interval_sec = 0,
        size_t rbac_warmup_retry_multiplier = 3) const;
};

void processURL(const String & url, const String & container_name, Endpoint & endpoint, AuthMethod & auth_method);

std::unique_ptr<ContainerClient> getContainerClient(
    const ConnectionParams & params,
    bool readonly,
    size_t rbac_warmup_interval_sec = 0,
    size_t rbac_warmup_retry_multiplier = 3);

BlobClientOptions getClientOptions(
    const ContextPtr & context,
    const Settings & settings,
    const RequestSettings & request_settings,
    bool for_disk);

AuthMethod getAuthMethod(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

#endif

Endpoint processEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

std::unique_ptr<RequestSettings> getRequestSettings(const Settings & query_settings);
std::unique_ptr<RequestSettings> getRequestSettingsForBackup(ContextPtr context, String endpoint, bool use_native_copy);
std::unique_ptr<RequestSettings> getRequestSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Settings & settings_ref);

}


/// AzureSettingsByEndpoint contains a map of AzureBlobStorage endpoints and their settings, used in Context level
/// When any endpoint is used, the settings are looked up in this map and applied
class AzureSettingsByEndpoint
{
public:
    void loadFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DB::Settings & settings);

    std::optional<AzureBlobStorage::RequestSettings> getSettings(
        const std::string & endpoint) const;

private:
    mutable std::mutex mutex;
    std::map<const String, const AzureBlobStorage::RequestSettings> azure_settings;
};


}
