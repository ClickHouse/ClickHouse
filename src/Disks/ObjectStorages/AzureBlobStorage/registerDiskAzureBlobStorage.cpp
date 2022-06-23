#include <Common/config.h>

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskCacheWrapper.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

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

}

void registerDiskAzureBlobStorage(DiskFactory & factory)
{
    auto creator = [](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/)
    {
        auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);

        /// FIXME Cache currently unsupported :(
        ObjectStoragePtr azure_object_storage = std::make_unique<AzureObjectStorage>(
            nullptr,
            name,
            getAzureBlobContainerClient(config, config_prefix),
            getAzureBlobStorageSettings(config, config_prefix, context));

        uint64_t copy_thread_pool_size = config.getUInt(config_prefix + ".thread_pool_size", 16);
        bool send_metadata = config.getBool(config_prefix + ".send_metadata", false);

        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, "");

        std::shared_ptr<IDisk> azure_blob_storage_disk = std::make_shared<DiskObjectStorage>(
            name,
            /* no namespaces */"",
            "DiskAzureBlobStorage",
            std::move(metadata_storage),
            std::move(azure_object_storage),
            DiskType::AzureBlobStorage,
            send_metadata,
            copy_thread_pool_size
        );

        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*azure_blob_storage_disk);
            checkReadAccess(*azure_blob_storage_disk);
            checkReadWithOffset(*azure_blob_storage_disk);
            checkRemoveAccess(*azure_blob_storage_disk);
        }

#ifdef NDEBUG
        bool use_cache = true;
#else
        /// Current cache implementation lead to allocations in destructor of
        /// read buffer.
        bool use_cache = false;
#endif

        azure_blob_storage_disk->startup(context);

        if (config.getBool(config_prefix + ".cache_enabled", use_cache))
        {
            String cache_path = config.getString(config_prefix + ".cache_path", context->getPath() + "disks/" + name + "/cache/");
            azure_blob_storage_disk = wrapWithCache(azure_blob_storage_disk, "azure-blob-storage-cache", cache_path, metadata_path);
        }

        return std::make_shared<DiskRestartProxy>(azure_blob_storage_disk);
    };
    factory.registerDiskType("azure_blob_storage", creator);
}

}

#else

namespace DB
{

void registerDiskAzureBlobStorage(DiskFactory &) {}

}

#endif
