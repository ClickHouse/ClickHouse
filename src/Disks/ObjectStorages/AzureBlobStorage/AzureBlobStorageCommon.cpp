#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/re2.h>
#include <Core/Settings.h>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event AzureGetProperties;
    extern const Event DiskAzureGetProperties;
    extern const Event AzureCreateContainer;
    extern const Event DiskAzureCreateContainer;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 azure_max_single_part_upload_size;
    extern const SettingsUInt64 azure_max_single_read_retries;
    extern const SettingsUInt64 azure_list_object_keys_size;
    extern const SettingsUInt64 azure_min_upload_part_size;
    extern const SettingsUInt64 azure_max_upload_part_size;
    extern const SettingsUInt64 azure_max_single_part_copy_size;
    extern const SettingsUInt64 azure_max_blocks_in_multipart_upload;
    extern const SettingsUInt64 azure_max_unexpected_write_error_retries;
    extern const SettingsUInt64 azure_max_inflight_parts_for_one_file;
    extern const SettingsUInt64 azure_strict_upload_part_size;
    extern const SettingsUInt64 azure_upload_part_size_multiply_factor;
    extern const SettingsUInt64 azure_upload_part_size_multiply_parts_count_threshold;
    extern const SettingsUInt64 azure_sdk_max_retries;
    extern const SettingsUInt64 azure_sdk_retry_initial_backoff_ms;
    extern const SettingsUInt64 azure_sdk_retry_max_backoff_ms;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace AzureBlobStorage
{

static void validateStorageAccountUrl(const String & storage_account_url)
{
    const auto * storage_account_url_pattern_str = R"(http(()|s)://[a-z0-9-.:]+(()|/)[a-z0-9]*(()|/))";
    static const RE2 storage_account_url_pattern(storage_account_url_pattern_str);

    if (!re2::RE2::FullMatch(storage_account_url, storage_account_url_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Blob Storage URL is not valid, should follow the format: {}, got: {}", storage_account_url_pattern_str, storage_account_url);
}

static void validateContainerName(const String & container_name)
{
    auto len = container_name.length();
    if (len < 3 || len > 64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "AzureBlob Storage container name is not valid, should have length between 3 and 64, but has length: {}", len);

    const auto * container_name_pattern_str = R"([a-z][a-z0-9-]+)";
    static const RE2 container_name_pattern(container_name_pattern_str);

    if (!re2::RE2::FullMatch(container_name, container_name_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "AzureBlob Storage container name is not valid, should follow the format: {}, got: {}",
                        container_name_pattern_str, container_name);
}

static bool isConnectionString(const std::string & candidate)
{
    return !candidate.starts_with("http");
}

String ConnectionParams::getConnectionURL() const
{
    if (std::holds_alternative<ConnectionString>(auth_method))
    {
        auto parsed_connection_string = Azure::Storage::_internal::ParseConnectionString(endpoint.storage_account_url);
        return parsed_connection_string.BlobServiceUrl.GetAbsoluteUrl();
    }

    return endpoint.storage_account_url;
}

std::unique_ptr<ServiceClient> ConnectionParams::createForService() const
{
    return std::visit([this]<typename T>(const T & auth)
    {
        if constexpr (std::is_same_v<T, ConnectionString>)
            return std::make_unique<ServiceClient>(ServiceClient::CreateFromConnectionString(auth.toUnderType(), client_options));
        else
            return std::make_unique<ServiceClient>(endpoint.getEndpointWithoutContainer(), auth, client_options);
    }, auth_method);
}

std::unique_ptr<ContainerClient> ConnectionParams::createForContainer() const
{
    return std::visit([this]<typename T>(const T & auth)
    {
        if constexpr (std::is_same_v<T, ConnectionString>)
            return std::make_unique<ContainerClient>(ContainerClient::CreateFromConnectionString(auth.toUnderType(), endpoint.container_name, client_options));
        else
            return std::make_unique<ContainerClient>(endpoint.getEndpoint(), auth, client_options);
    }, auth_method);
}

Endpoint processEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String storage_url;
    String account_name;
    String container_name;
    String prefix;

    auto get_container_name = [&]
    {
        if (config.has(config_prefix + ".container_name"))
            return config.getString(config_prefix + ".container_name");

        if (config.has(config_prefix + ".container"))
            return config.getString(config_prefix + ".container");

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected either `container` or `container_name` parameter in config");
    };

    if (config.has(config_prefix + ".endpoint"))
    {
        String endpoint = config.getString(config_prefix + ".endpoint");

        /// For some authentication methods account name is not present in the endpoint
        /// 'endpoint_contains_account_name' bool is used to understand how to split the endpoint (default : true)
        bool endpoint_contains_account_name = config.getBool(config_prefix + ".endpoint_contains_account_name", true);

        size_t pos = endpoint.find("//");
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected '//' in endpoint");

        if (endpoint_contains_account_name)
        {
            size_t acc_pos_begin = endpoint.find('/', pos + 2);
            if (acc_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected account_name in endpoint");

            storage_url = endpoint.substr(0, acc_pos_begin);
            size_t acc_pos_end = endpoint.find('/', acc_pos_begin + 1);

            if (acc_pos_end == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            account_name = endpoint.substr(acc_pos_begin + 1, acc_pos_end - acc_pos_begin - 1);

            size_t cont_pos_end = endpoint.find('/', acc_pos_end + 1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(acc_pos_end + 1, cont_pos_end - acc_pos_end - 1);
                prefix = endpoint.substr(cont_pos_end + 1);
            }
            else
            {
                container_name = endpoint.substr(acc_pos_end + 1);
            }
        }
        else
        {
            size_t cont_pos_begin = endpoint.find('/', pos + 2);

            if (cont_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            storage_url = endpoint.substr(0, cont_pos_begin);
            size_t cont_pos_end = endpoint.find('/', cont_pos_begin + 1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(cont_pos_begin + 1,cont_pos_end - cont_pos_begin - 1);
                prefix = endpoint.substr(cont_pos_end + 1);
            }
            else
            {
                container_name = endpoint.substr(cont_pos_begin + 1);
            }
        }
    }
    else if (config.has(config_prefix + ".connection_string"))
    {
        storage_url = config.getString(config_prefix + ".connection_string");
        container_name = get_container_name();
    }
    else if (config.has(config_prefix + ".storage_account_url"))
    {
        storage_url = config.getString(config_prefix + ".storage_account_url");
        validateStorageAccountUrl(storage_url);
        container_name = get_container_name();
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected either `storage_account_url` or `connection_string` or `endpoint` in config");

    if (!container_name.empty())
        validateContainerName(container_name);

    std::optional<bool> container_already_exists {};
    if (config.has(config_prefix + ".container_already_exists"))
        container_already_exists = {config.getBool(config_prefix + ".container_already_exists")};

    return {storage_url, account_name, container_name, prefix, "", container_already_exists};
}

void processURL(const String & url, const String & container_name, Endpoint & endpoint, AuthMethod & auth_method)
{
    endpoint.container_name = container_name;

    if (isConnectionString(url))
    {
        endpoint.storage_account_url = url;
        auth_method = ConnectionString{url};
        return;
    }

    auto pos = url.find('?');

    /// If conneciton_url does not have '?', then its not SAS
    if (pos == std::string::npos)
    {
        endpoint.storage_account_url = url;
        auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
    }
    else
    {
        endpoint.storage_account_url = url.substr(0, pos);
        endpoint.sas_auth = url.substr(pos + 1);
        auth_method = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    }
}

static bool containerExists(const ContainerClient & client)
{
    ProfileEvents::increment(ProfileEvents::AzureGetProperties);
    if (client.GetClickhouseOptions().IsClientForDisk)
        ProfileEvents::increment(ProfileEvents::DiskAzureGetProperties);

    try
    {
        client.GetProperties();
        return true;
    }
    catch (const Azure::Storage::StorageException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return false;
        throw;
    }
}

std::unique_ptr<ContainerClient> getContainerClient(const ConnectionParams & params, bool readonly)
{
    if (params.endpoint.container_already_exists.value_or(false) || readonly)
    {
        return params.createForContainer();
    }

    if (!params.endpoint.container_already_exists.has_value())
    {
        auto container_client = params.createForContainer();
        if (containerExists(*container_client))
            return container_client;
    }

    try
    {
        auto service_client = params.createForService();

        ProfileEvents::increment(ProfileEvents::AzureCreateContainer);
        if (params.client_options.ClickhouseOptions.IsClientForDisk)
            ProfileEvents::increment(ProfileEvents::DiskAzureCreateContainer);

        return std::make_unique<ContainerClient>(service_client->CreateBlobContainer(params.endpoint.container_name).Value);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        /// If container_already_exists is not set (in config), ignore already exists error. Conflict - The specified container already exists.
        /// To avoid race with creation of container, handle this error despite that we have already checked the existence of container.
        if (!params.endpoint.container_already_exists.has_value() && e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict)
            return params.createForContainer();
        throw;
    }
}

AuthMethod getAuthMethod(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    if (config.has(config_prefix + ".account_key") && config.has(config_prefix + ".account_name"))
    {
        return std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            config.getString(config_prefix + ".account_name"),
            config.getString(config_prefix + ".account_key")
        );
    }

    if (config.has(config_prefix + ".connection_string"))
        return ConnectionString{config.getString(config_prefix + ".connection_string")};

    if (config.getBool(config_prefix + ".use_workload_identity", false))
        return std::make_shared<Azure::Identity::WorkloadIdentityCredential>();

    return std::make_shared<Azure::Identity::ManagedIdentityCredential>();
}

BlobClientOptions getClientOptions(const RequestSettings & settings, bool for_disk)
{
    Azure::Core::Http::Policies::RetryOptions retry_options;
    retry_options.MaxRetries = static_cast<Int32>(settings.sdk_max_retries);
    retry_options.RetryDelay = std::chrono::milliseconds(settings.sdk_retry_initial_backoff_ms);
    retry_options.MaxRetryDelay = std::chrono::milliseconds(settings.sdk_retry_max_backoff_ms);

    Azure::Core::Http::CurlTransportOptions curl_options;
    curl_options.NoSignal = true;
    curl_options.IPResolve = settings.curl_ip_resolve;

    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry = retry_options;
    client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>(curl_options);
    client_options.ClickhouseOptions = Azure::Storage::Blobs::ClickhouseClientOptions{.IsClientForDisk=for_disk};

    return client_options;
}

std::unique_ptr<RequestSettings> getRequestSettings(const Settings & query_settings)
{
    auto settings = std::make_unique<RequestSettings>();

    settings->max_single_part_upload_size = query_settings[Setting::azure_max_single_part_upload_size];
    settings->max_single_read_retries = query_settings[Setting::azure_max_single_read_retries];
    settings->max_single_download_retries = query_settings[Setting::azure_max_single_read_retries];
    settings->list_object_keys_size = query_settings[Setting::azure_list_object_keys_size];
    settings->min_upload_part_size = query_settings[Setting::azure_min_upload_part_size];
    settings->max_upload_part_size = query_settings[Setting::azure_max_upload_part_size];
    settings->max_single_part_copy_size = query_settings[Setting::azure_max_single_part_copy_size];
    settings->max_blocks_in_multipart_upload = query_settings[Setting::azure_max_blocks_in_multipart_upload];
    settings->max_unexpected_write_error_retries = query_settings[Setting::azure_max_unexpected_write_error_retries];
    settings->max_inflight_parts_for_one_file = query_settings[Setting::azure_max_inflight_parts_for_one_file];
    settings->strict_upload_part_size = query_settings[Setting::azure_strict_upload_part_size];
    settings->upload_part_size_multiply_factor = query_settings[Setting::azure_upload_part_size_multiply_factor];
    settings->upload_part_size_multiply_parts_count_threshold = query_settings[Setting::azure_upload_part_size_multiply_parts_count_threshold];
    settings->sdk_max_retries = query_settings[Setting::azure_sdk_max_retries];
    settings->sdk_retry_initial_backoff_ms = query_settings[Setting::azure_sdk_retry_initial_backoff_ms];
    settings->sdk_retry_max_backoff_ms = query_settings[Setting::azure_sdk_retry_max_backoff_ms];

    return settings;
}

std::unique_ptr<RequestSettings> getRequestSettingsForBackup(const Settings & query_settings, bool use_native_copy)
{
    auto settings = getRequestSettings(query_settings);
    settings->use_native_copy = use_native_copy;
    return settings;
}

std::unique_ptr<RequestSettings> getRequestSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    auto settings = std::make_unique<RequestSettings>();
    const auto & settings_ref = context->getSettingsRef();

    settings->min_bytes_for_seek = config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024);
    settings->use_native_copy = config.getBool(config_prefix + ".use_native_copy", false);

    settings->max_single_part_upload_size = config.getUInt64(config_prefix + ".max_single_part_upload_size", settings_ref[Setting::azure_max_single_part_upload_size]);
    settings->max_single_read_retries = config.getUInt64(config_prefix + ".max_single_read_retries", settings_ref[Setting::azure_max_single_read_retries]);
    settings->max_single_download_retries = config.getUInt64(config_prefix + ".max_single_download_retries", settings_ref[Setting::azure_max_single_read_retries]);
    settings->list_object_keys_size = config.getUInt64(config_prefix + ".list_object_keys_size", settings_ref[Setting::azure_list_object_keys_size]);
    settings->min_upload_part_size = config.getUInt64(config_prefix + ".min_upload_part_size", settings_ref[Setting::azure_min_upload_part_size]);
    settings->max_upload_part_size = config.getUInt64(config_prefix + ".max_upload_part_size", settings_ref[Setting::azure_max_upload_part_size]);
    settings->max_single_part_copy_size = config.getUInt64(config_prefix + ".max_single_part_copy_size", settings_ref[Setting::azure_max_single_part_copy_size]);
    settings->max_blocks_in_multipart_upload = config.getUInt64(config_prefix + ".max_blocks_in_multipart_upload", settings_ref[Setting::azure_max_blocks_in_multipart_upload]);
    settings->max_unexpected_write_error_retries = config.getUInt64(config_prefix + ".max_unexpected_write_error_retries", settings_ref[Setting::azure_max_unexpected_write_error_retries]);
    settings->max_inflight_parts_for_one_file = config.getUInt64(config_prefix + ".max_inflight_parts_for_one_file", settings_ref[Setting::azure_max_inflight_parts_for_one_file]);
    settings->strict_upload_part_size = config.getUInt64(config_prefix + ".strict_upload_part_size", settings_ref[Setting::azure_strict_upload_part_size]);
    settings->upload_part_size_multiply_factor = config.getUInt64(config_prefix + ".upload_part_size_multiply_factor", settings_ref[Setting::azure_upload_part_size_multiply_factor]);
    settings->upload_part_size_multiply_parts_count_threshold = config.getUInt64(config_prefix + ".upload_part_size_multiply_parts_count_threshold", settings_ref[Setting::azure_upload_part_size_multiply_parts_count_threshold]);

    settings->sdk_max_retries = config.getUInt64(config_prefix + ".max_tries", settings_ref[Setting::azure_sdk_max_retries]);
    settings->sdk_retry_initial_backoff_ms = config.getUInt64(config_prefix + ".retry_initial_backoff_ms", settings_ref[Setting::azure_sdk_retry_initial_backoff_ms]);
    settings->sdk_retry_max_backoff_ms = config.getUInt64(config_prefix + ".retry_max_backoff_ms", settings_ref[Setting::azure_sdk_retry_max_backoff_ms]);

    if (config.has(config_prefix + ".curl_ip_resolve"))
    {
        using CurlOptions = Azure::Core::Http::CurlTransportOptions;

        auto value = config.getString(config_prefix + ".curl_ip_resolve");
        if (value == "ipv4")
            settings->curl_ip_resolve = CurlOptions::CURL_IPRESOLVE_V4;
        else if (value == "ipv6")
            settings->curl_ip_resolve = CurlOptions::CURL_IPRESOLVE_V6;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected value for option 'curl_ip_resolve': {}. Expected one of 'ipv4' or 'ipv6'", value);
    }

    return settings;
}

}

}

#endif
