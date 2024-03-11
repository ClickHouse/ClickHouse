#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/Exception.h>
#include <Common/re2.h>
#include <optional>
#include <azure/identity/managed_identity_credential.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

using namespace Azure::Storage::Blobs;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


void validateStorageAccountUrl(const String & storage_account_url)
{
    const auto * storage_account_url_pattern_str = R"(http(()|s)://[a-z0-9-.:]+(()|/)[a-z0-9]*(()|/))";
    static const RE2 storage_account_url_pattern(storage_account_url_pattern_str);

    if (!re2::RE2::FullMatch(storage_account_url, storage_account_url_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Blob Storage URL is not valid, should follow the format: {}, got: {}", storage_account_url_pattern_str, storage_account_url);
}


void validateContainerName(const String & container_name)
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


AzureBlobStorageEndpoint processAzureBlobStorageEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String storage_url;
    String account_name;
    String container_name;
    String prefix;
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
            size_t acc_pos_begin = endpoint.find('/', pos+2);
            if (acc_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected account_name in endpoint");

            storage_url = endpoint.substr(0,acc_pos_begin);
            size_t acc_pos_end = endpoint.find('/',acc_pos_begin+1);

            if (acc_pos_end == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            account_name = endpoint.substr(acc_pos_begin+1,(acc_pos_end-acc_pos_begin)-1);

            size_t cont_pos_end = endpoint.find('/', acc_pos_end+1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(acc_pos_end+1,(cont_pos_end-acc_pos_end)-1);
                prefix = endpoint.substr(cont_pos_end+1);
            }
            else
            {
                container_name = endpoint.substr(acc_pos_end+1);
            }
        }
        else
        {
            size_t cont_pos_begin = endpoint.find('/', pos+2);

            if (cont_pos_begin == std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected container_name in endpoint");

            storage_url = endpoint.substr(0,cont_pos_begin);
            size_t cont_pos_end = endpoint.find('/',cont_pos_begin+1);

            if (cont_pos_end != std::string::npos)
            {
                container_name = endpoint.substr(cont_pos_begin+1,(cont_pos_end-cont_pos_begin)-1);
                prefix = endpoint.substr(cont_pos_end+1);
            }
            else
            {
                container_name = endpoint.substr(cont_pos_begin+1);
            }
        }
    }
    else if (config.has(config_prefix + ".connection_string"))
    {
        storage_url = config.getString(config_prefix + ".connection_string");
        container_name = config.getString(config_prefix + ".container_name");
    }
    else if (config.has(config_prefix + ".storage_account_url"))
    {
        storage_url = config.getString(config_prefix + ".storage_account_url");
        validateStorageAccountUrl(storage_url);
        container_name = config.getString(config_prefix + ".container_name");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected either `storage_account_url` or `connection_string` or `endpoint` in config");

    if (!container_name.empty())
        validateContainerName(container_name);
    std::optional<bool> container_already_exists {};
    if (config.has(config_prefix + ".container_already_exists"))
        container_already_exists = {config.getBool(config_prefix + ".container_already_exists")};
    return {storage_url, account_name, container_name, prefix, container_already_exists};
}


template <class T>
std::unique_ptr<T> getClientWithConnectionString(const String & connection_str, const String & container_name) = delete;


template<>
std::unique_ptr<BlobServiceClient> getClientWithConnectionString(
    const String & connection_str, const String & /*container_name*/)
{
    return std::make_unique<BlobServiceClient>(BlobServiceClient::CreateFromConnectionString(connection_str));
}


template<>
std::unique_ptr<BlobContainerClient> getClientWithConnectionString(
    const String & connection_str, const String & container_name)
{
    return std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(connection_str, container_name));
}


template <class T>
std::unique_ptr<T> getAzureBlobStorageClientWithAuth(
    const String & url, const String & container_name, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    std::string connection_str;
    if (config.has(config_prefix + ".connection_string"))
        connection_str = config.getString(config_prefix + ".connection_string");

    if (!connection_str.empty())
        return getClientWithConnectionString<T>(connection_str, container_name);

    if (config.has(config_prefix + ".account_key") && config.has(config_prefix + ".account_name"))
    {
        auto storage_shared_key_credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            config.getString(config_prefix + ".account_name"),
            config.getString(config_prefix + ".account_key")
        );
        return std::make_unique<T>(url, storage_shared_key_credential);
    }

    auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    return std::make_unique<T>(url, managed_identity_credential);
}


std::unique_ptr<BlobContainerClient> getAzureBlobContainerClient(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    auto endpoint = processAzureBlobStorageEndpoint(config, config_prefix);
    auto container_name = endpoint.container_name;
    auto final_url = endpoint.getEndpoint();

    if (endpoint.container_already_exists.value_or(false))
        return getAzureBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);

    auto blob_service_client = getAzureBlobStorageClientWithAuth<BlobServiceClient>(
        endpoint.getEndpointWithoutContainer(), container_name, config, config_prefix);

    try
    {
        return std::make_unique<BlobContainerClient>(
            blob_service_client->CreateBlobContainer(container_name).Value);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        /// If container_already_exists is not set (in config), ignore already exists error.
        /// (Conflict - The specified container already exists)
        if (!endpoint.container_already_exists.has_value() && e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict)
            return getAzureBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);
        throw;
    }
}

std::unique_ptr<AzureObjectStorageSettings> getAzureBlobStorageSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    return std::make_unique<AzureObjectStorageSettings>(
        config.getUInt64(config_prefix + ".max_single_part_upload_size", 100 * 1024 * 1024),
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".max_single_read_retries", 3),
        config.getInt(config_prefix + ".max_single_download_retries", 3),
        config.getInt(config_prefix + ".list_object_keys_size", 1000),
        config.getUInt64(config_prefix + ".max_upload_part_size", 5ULL * 1024 * 1024 * 1024),
        config.getUInt64(config_prefix + ".max_single_part_copy_size", context->getSettings().azure_max_single_part_copy_size),
        config.getBool(config_prefix + ".use_native_copy", false),
        config.getUInt64(config_prefix + ".max_unexpected_write_error_retries", context->getSettings().azure_max_unexpected_write_error_retries)
    );
}

}

#endif
