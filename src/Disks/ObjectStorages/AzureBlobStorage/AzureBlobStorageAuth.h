#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

namespace DB
{

struct AzureBlobStorageEndpoint
{
    const String storage_account_url;
    const String container_name;
    const std::optional<bool> container_already_exists;
};

std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient> getAzureBlobContainerClient(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

std::unique_ptr<AzureObjectStorageSettings> getAzureBlobStorageSettings(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr /*context*/);

}

#endif
