#pragma once

#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>

namespace DB
{

std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient> getAzureBlobContainerClient(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

std::unique_ptr<AzureObjectStorageSettings> getAzureBlobStorageSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr /*context*/);

}

#endif
