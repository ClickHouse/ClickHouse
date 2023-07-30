#include "config.h"

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Interpreters/Context.h>

namespace DB
{

void registerDiskAzureBlobStorage(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
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

        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, "");

        std::shared_ptr<IDisk> azure_blob_storage_disk = std::make_shared<DiskObjectStorage>(
            name,
            /* no namespaces */"",
            "DiskAzureBlobStorage",
            std::move(metadata_storage),
            std::move(azure_object_storage),
            config,
            config_prefix
        );

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        azure_blob_storage_disk->startup(context, skip_access_check);

        return azure_blob_storage_disk;
    };

    factory.registerDiskType("azure_blob_storage", creator);
}

}

#else

namespace DB
{

void registerDiskAzureBlobStorage(DiskFactory &, bool /* global_skip_access_check */) {}

}

#endif
