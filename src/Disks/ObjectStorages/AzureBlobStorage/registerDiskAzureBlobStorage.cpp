#include "config.h"

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/DiskRestartProxy.h>

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Interpreters/Context.h>

namespace DB
{

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

        ObjectStoragePtr azure_object_storage = std::make_unique<AzureObjectStorage>(
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
            send_metadata,
            copy_thread_pool_size
        );

        bool skip_access_check = config.getBool(config_prefix + ".skip_access_check", false);
        azure_blob_storage_disk->startup(context, skip_access_check);

        return std::make_shared<DiskRestartProxy>(azure_blob_storage_disk, skip_access_check);
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
