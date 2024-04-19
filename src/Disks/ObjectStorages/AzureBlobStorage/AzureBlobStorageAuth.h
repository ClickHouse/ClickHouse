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
    const String account_name;
    const String container_name;
    const String prefix;
    const std::optional<bool> container_already_exists;

    String getEndpoint()
    {
        String url = storage_account_url;

        if (!account_name.empty())
            url += "/" + account_name;

        if (!container_name.empty())
            url += "/" + container_name;

        if (!prefix.empty())
            url += "/" + prefix;

        return url;
    }

    String getEndpointWithoutContainer()
    {
        String url = storage_account_url;

        if (!account_name.empty())
            url += "/" + account_name;

        return url;
    }
};

std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient> getAzureBlobContainerClient(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

AzureBlobStorageEndpoint processAzureBlobStorageEndpoint(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

std::unique_ptr<AzureObjectStorageSettings> getAzureBlobStorageSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);

}

#endif
