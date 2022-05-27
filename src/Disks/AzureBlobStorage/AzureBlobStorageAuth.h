#pragma once

#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>
#include <azure/storage/blobs.hpp>

namespace DB
{

std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getAzureBlobContainerClient(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

}

#endif
