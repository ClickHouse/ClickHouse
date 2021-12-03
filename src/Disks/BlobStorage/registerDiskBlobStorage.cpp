#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <optional>

#include <re2/re2.h>
#include <azure/identity/managed_identity_credential.hpp>

#include <Disks/BlobStorage/DiskBlobStorage.h>
#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskCacheWrapper.h>
#include <Disks/RemoteDisksCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
}

constexpr char test_file[] = "test.txt";
constexpr char test_str[] = "test";
constexpr size_t test_str_size = 4;


void checkWriteAccess(IDisk & disk)
{
    auto file = disk.writeFile(test_file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    file->write(test_str, test_str_size);
}


void checkReadAccess(IDisk & disk)
{
    auto file = disk.readFile(test_file);
    String buf(test_str_size, '0');
    file->readStrict(buf.data(), test_str_size);
    if (buf != test_str)
        throw Exception("No read access to disk", ErrorCodes::PATH_ACCESS_DENIED);
}


void checkReadWithOffset(IDisk & disk)
{
    auto file = disk.readFile(test_file);
    auto offset = 2;
    auto test_size = test_str_size - offset;
    String buf(test_size, '0');
    file->seek(offset, 0);
    file->readStrict(buf.data(), test_size);
    if (buf != test_str + offset)
        throw Exception("Failed to read file with offset", ErrorCodes::PATH_ACCESS_DENIED);
}


void checkRemoveAccess(IDisk & disk)
{
    disk.removeFile(test_file);
}


std::unique_ptr<DiskBlobStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr /*context*/)
{
    return std::make_unique<DiskBlobStorageSettings>(
        config.getUInt64(config_prefix + ".max_single_part_upload_size", 100 * 1024 * 1024),
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".max_single_read_retries", 3),
        config.getInt(config_prefix + ".max_single_download_retries", 3),
        config.getInt(config_prefix + ".thread_pool_size", 16)
        // TODO: maybe use context for global settings from Settings.h
    );
}


struct BlobStorageEndpoint
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
            "Blob Storage container name is not valid, should have length between 3 and 64, but has length: {}", len);

    const auto * container_name_pattern_str = R"([a-z][a-z0-9-]+)";
    static const RE2 container_name_pattern(container_name_pattern_str);

    if (!re2::RE2::FullMatch(container_name, container_name_pattern))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Blob Storage container name is not valid, should follow the format: {}, got: {}", container_name_pattern_str, container_name);
}


BlobStorageEndpoint processBlobStorageEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
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
std::shared_ptr<T> getClientWithConnectionString(const String & connection_str, const String & container_name) = delete;


template<>
std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> getClientWithConnectionString(
    const String & connection_str, const String & /*container_name*/)
{
    return std::make_shared<Azure::Storage::Blobs::BlobServiceClient>(
        Azure::Storage::Blobs::BlobServiceClient::CreateFromConnectionString(connection_str));
}


template<>
std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getClientWithConnectionString(
    const String & connection_str, const String & container_name)
{
    return std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(connection_str, container_name));
}


template <class T>
std::shared_ptr<T> getBlobStorageClientWithAuth(
    const String & url, const String & container_name, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    if (config.has(config_prefix + ".connection_string")) {
        String connection_str = config.getString(config_prefix + ".connection_string");
        return getClientWithConnectionString<T>(connection_str, container_name);
    }

    // TODO: untested so far
    if (config.has(config_prefix + ".account_key") && config.has(config_prefix + ".account_name")) {
        auto storage_shared_key_credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            config.getString(config_prefix + ".account_name"),
            config.getString(config_prefix + ".account_key")
        );
        return std::make_shared<T>(url, storage_shared_key_credential);
    }

    auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    return std::make_shared<T>(url, managed_identity_credential);
}


std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getBlobContainerClient(
    const BlobStorageEndpoint & endpoint, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    using namespace Azure::Storage::Blobs;

    auto container_name = endpoint.container_name;
    auto final_url = endpoint.storage_account_url
        + (endpoint.storage_account_url.back() == '/' ? "" : "/")
        + container_name;

    if (endpoint.container_already_exists.value_or(false))
        return getBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);

    auto blob_service_client = getBlobStorageClientWithAuth<BlobServiceClient>(final_url, container_name, config, config_prefix);

    if (!endpoint.container_already_exists.has_value())
    {
        ListBlobContainersOptions blob_containers_list_options;
        blob_containers_list_options.Prefix = endpoint.container_name;
        blob_containers_list_options.PageSizeHint = 1;
        auto blob_containers = blob_service_client->ListBlobContainers().BlobContainers;
        for (const auto & blob_container : blob_containers)
        {
            if (blob_container.Name == endpoint.container_name)
                return getBlobStorageClientWithAuth<BlobContainerClient>(final_url, container_name, config, config_prefix);
        }
    }

    return std::make_shared<BlobContainerClient>(
        blob_service_client->CreateBlobContainer(endpoint.container_name).Value);
}


void registerDiskBlobStorage(DiskFactory & factory)
{
    auto creator = [](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/)
    {
        auto endpoint_details = processBlobStorageEndpoint(config, config_prefix);
        auto blob_container_client = getBlobContainerClient(endpoint_details, config, config_prefix);

        /// where the metadata files are stored locally
        auto metadata_path = config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
        fs::create_directories(metadata_path);
        auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0);

        std::shared_ptr<IDisk> blob_storage_disk = std::make_shared<DiskBlobStorage>(
            name,
            metadata_disk,
            blob_container_client,
            getSettings(config, config_prefix, context),
            getSettings
        );

        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*blob_storage_disk);
            checkReadAccess(*blob_storage_disk);
            checkReadWithOffset(*blob_storage_disk);
            checkRemoveAccess(*blob_storage_disk);
        }

        if (config.getBool(config_prefix + ".cache_enabled", true))
        {
            String cache_path = config.getString(config_prefix + ".cache_path", context->getPath() + "disks/" + name + "/cache/");
            blob_storage_disk = wrapWithCache(blob_storage_disk, "blob-storage-cache", cache_path, metadata_path);
        }

        return std::make_shared<DiskRestartProxy>(blob_storage_disk);
    };
    factory.registerDiskType("blob_storage", creator);
}

}

#else

namespace DB
{

void registerDiskBlobStorage(DiskFactory &) {}

}

#endif
