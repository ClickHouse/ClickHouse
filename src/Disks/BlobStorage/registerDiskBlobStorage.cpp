#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/BlobStorage/DiskBlobStorage.h>
#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskCacheWrapper.h>
#include <azure/identity/managed_identity_credential.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
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
    auto file = disk.readFile(test_file, DBMS_DEFAULT_BUFFER_SIZE);
    String buf(test_str_size, '0');
    file->readStrict(buf.data(), test_str_size);

#ifdef VERBOSE_DEBUG_MODE
    std::cout << "buf: ";
    for (size_t i = 0; i < test_str_size; i++)
    {
        std::cout << static_cast<uint8_t>(buf[i]) << " ";
    }
    std::cout << "\n";
#endif

    if (buf != test_str)
        throw Exception("No read access to disk", ErrorCodes::PATH_ACCESS_DENIED);
}


void checkRemoveAccess(IDisk & disk) {
    // TODO: implement actually removing the file from Blob Storage cloud, now it seems only the metadata file is removed
    disk.removeFile(test_file);
}


std::unique_ptr<DiskBlobStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr /* context */)
{
    return std::make_unique<DiskBlobStorageSettings>(
        config.getInt(config_prefix + ".thread_pool_size", 16)
        // TODO: use context for global settings from Settings.h
        );
}


void registerDiskBlobStorage(DiskFactory & factory)
{
    auto creator = [](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap &)
    {
        auto endpoint_url = config.getString(config_prefix + ".endpoint", "https://sadttmpstgeus.blob.core.windows.net/data"); // TODO: remove default url
        auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
        auto blob_container_client = Azure::Storage::Blobs::BlobContainerClient(endpoint_url, managed_identity_credential);

        /// where the metadata files are stored locally
        auto metadata_path = config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
        fs::create_directories(metadata_path);

        std::shared_ptr<IDisk> blob_storage_disk = std::make_shared<DiskBlobStorage>(
            name,
            metadata_path,
            blob_container_client,
            getSettings(config, config_prefix, context),
            getSettings
        );

        // NOTE: test - almost direct copy-paste from registerDiskS3
        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*blob_storage_disk);
            checkReadAccess(*blob_storage_disk);
            checkRemoveAccess(*blob_storage_disk);
        }

        // NOTE: cache - direct copy-paste from registerDiskS3
        if (config.getBool(config_prefix + ".cache_enabled", true))
        {
            String cache_path = config.getString(config_prefix + ".cache_path", context->getPath() + "disks/" + name + "/cache/");

            if (metadata_path == cache_path)
                throw Exception("Metadata and cache path should be different: " + metadata_path, ErrorCodes::BAD_ARGUMENTS);

            auto cache_disk = std::make_shared<DiskLocal>("blob-storage-cache", cache_path, 0);
            auto cache_file_predicate = [] (const String & path)
            {
                return path.ends_with("idx") // index files.
                       || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") // mark files.
                       || path.ends_with("txt") || path.ends_with("dat");
            };

            blob_storage_disk = std::make_shared<DiskCacheWrapper>(blob_storage_disk, cache_disk, cache_file_predicate);
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
