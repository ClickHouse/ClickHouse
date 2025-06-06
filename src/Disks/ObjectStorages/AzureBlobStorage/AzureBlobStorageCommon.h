#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <azure/core/response.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/core/http/curl_transport.hpp>

#endif


#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <base/strong_typedef.h>
#include <filesystem>
#include <variant>

namespace fs = std::filesystem;

namespace DB
{

struct Settings;

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

#if USE_AZURE_BLOB_STORAGE
    using CurlOptions = Azure::Core::Http::CurlTransportOptions;
    CurlOptions::CurlOptIPResolve curl_ip_resolve = CurlOptions::CURL_IPRESOLVE_WHATEVER;
#endif
};

struct Endpoint
{
    String storage_account_url;
    String account_name;
    String container_name;
    String prefix;
    String sas_auth;
    std::optional<bool> container_already_exists;

    String getContainerEndpoint() const
    {
        String url = storage_account_url;
        if (url.ends_with('/'))
          url.pop_back();

        if (!account_name.empty())
            url += "/" + account_name;

        if (!container_name.empty())
            url += "/" + container_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        return url;
    }

    String getServiceEndpoint() const
    {
        String url = storage_account_url;

        if (!account_name.empty())
            url += "/" + account_name;

        if (!sas_auth.empty())
            url += "?" + sas_auth;

        return url;
    }
};

#if USE_AZURE_BLOB_STORAGE

using BlobClient = Azure::Storage::Blobs::BlobClient;
using BlockBlobClient = Azure::Storage::Blobs::BlockBlobClient;
using RawContainerClient = Azure::Storage::Blobs::BlobContainerClient;

using Azure::Storage::Blobs::ListBlobsOptions;
using Azure::Storage::Blobs::ListBlobsPagedResponse;
using BlobContainerPropertiesRespones = Azure::Response<Azure::Storage::Blobs::Models::BlobContainerProperties>;

/// A wrapper for ContainerClient that correctly handles the prefix of blobs.
/// See AzureBlobStorageEndpoint and processAzureBlobStorageEndpoint for details.
class ContainerClientWrapper
{
public:
    ContainerClientWrapper(RawContainerClient client_, String blob_prefix_);

    bool IsClientForDisk() const;
    BlobClient GetBlobClient(const String & blob_name) const;
    BlockBlobClient GetBlockBlobClient(const String & blob_name) const;
    BlobContainerPropertiesRespones GetProperties() const;
    ListBlobsPagedResponse ListBlobs(const ListBlobsOptions & options) const;

private:
    RawContainerClient client;
    fs::path blob_prefix;
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
    std::unique_ptr<ContainerClient> createForContainer() const;
};

Endpoint processEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
void processURL(const String & url, const String & container_name, Endpoint & endpoint, AuthMethod & auth_method);

std::unique_ptr<ContainerClient> getContainerClient(const ConnectionParams & params, bool readonly);

BlobClientOptions getClientOptions(const RequestSettings & settings, bool for_disk);
AuthMethod getAuthMethod(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

#endif

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
