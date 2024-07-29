#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{
class SeekableReadBuffer;

using CreateReadBuffer = std::function<std::unique_ptr<SeekableReadBuffer>()>;

/// Copies a file from AzureBlobStorage to AzureBlobStorage.
/// The parameters `src_offset` and `src_size` specify a part in the source to copy.
void copyAzureBlobStorageFile(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> src_client,
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> dest_client,
    const String & src_container_for_logging,
    const String & src_blob,
    size_t src_offset,
    size_t src_size,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    const ReadSettings & read_settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});


/// Copies data from any seekable source to AzureBlobStorage.
/// The same functionality can be done by using the function copyData() and the class WriteBufferFromS3
/// however copyDataToS3File() is faster and spends less memory.
/// The callback `create_read_buffer` can be called from multiple threads in parallel, so that should be thread-safe.
/// The parameters `offset` and `size` specify a part in the source to copy.
void copyDataToAzureBlobStorageFile(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> client,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {});

}

#endif
