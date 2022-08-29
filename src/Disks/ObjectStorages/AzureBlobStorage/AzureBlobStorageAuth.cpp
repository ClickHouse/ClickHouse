#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>

#if USE_AZURE_BLOB_STORAGE

#include <optional>
#include <re2/re2.h>
#include <azure/identity/managed_identity_credential.hpp>
#include <Poco/Util/AbstractConfiguration.h>

using namespace Azure::Storage::Blobs;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct AzureBlobStorageEndpoint
{
    const String storage_account_url;
    const String container_name;
    const std::optional<bool> container_already_exists;
};


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
            "AzureBlob Storage container name is not valid, should follow the format: {}, got: {}", container_name_pattern_str, container_name);
}


AzureBlobStorageEndpoint processAzureBlobStorageEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String storage_account_url = config.getString(config_prefix + ".storage_account_url");
    validateStorageAccountUrl(storage_account_url);
    String container_name = config.getString(config_prefix + ".container_name", "default-container");
    validateContainerName(container_name);
    std::optional<bool> container_already_exists {};
    if (config.has(config_prefix + ".container_already_exists"))
        container_already_exists = {config.getBool(config_prefix + ".container_already_exists")};
    return {storage_account_url, container_name, container_already_exists};
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
    if (config.has(config_prefix + ".connection_string"))
    {
        String connection_str = config.getString(config_prefix + ".connection_string");
        return getClientWithConnectionString<T>(connection_str, container_name);
    }

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
    auto final_url = endpoint.storage_account_url
        + (endpoint.storage_account_url.back() == '/' ? "" : "/")
        + container_name;

    if (endpoint.container_already_exists.value_or(false))
        return getAzureBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);

    auto blob_service_client = getAzureBlobStorageClientWithAuth<BlobServiceClient>(endpoint.storage_account_url, container_name, config, config_prefix);

    if (!endpoint.container_already_exists.has_value())
    {
        ListBlobContainersOptions blob_containers_list_options;
        blob_containers_list_options.Prefix = container_name;
        blob_containers_list_options.PageSizeHint = 1;
        auto blob_containers = blob_service_client->ListBlobContainers().BlobContainers;
        for (const auto & blob_container : blob_containers)
        {
            if (blob_container.Name == endpoint.container_name)
                return getAzureBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);
        }
    }

    return std::make_unique<BlobContainerClient>(
        blob_service_client->CreateBlobContainer(container_name).Value);
}

std::unique_ptr<AzureObjectStorageSettings> getAzureBlobStorageSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr /*context*/)
{
    return std::make_unique<AzureObjectStorageSettings>(
        config.getUInt64(config_prefix + ".max_single_part_upload_size", 100 * 1024 * 1024),
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".max_single_read_retries", 3),
        config.getInt(config_prefix + ".max_single_download_retries", 3)
    );
}

}

#endif
