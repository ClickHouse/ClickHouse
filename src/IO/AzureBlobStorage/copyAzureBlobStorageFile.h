#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{
class SeekableReadBuffer;

using CreateReadBuffer = std::function<std::unique_ptr<SeekableReadBuffer>()>;

/// Copies a whole blob from AzureBlobStorage to AzureBlobStorage. `src_size` is the size of the source blob.
/// `src_etag` is the `ETag` of the generation of the source blob that the caller has decided to
/// copy (from the listing or the `HEAD` that produced `src_size`), or empty when it is not known.
/// The native copy (`CopyFromUri` / `StartCopyFromUri`) is pinned to that generation with a
/// source-side `If-Match`, so a source blob overwritten after the caller looked at it is not copied
/// as its newer generation: the copy throws `FILE_CHANGED_DURING_READ` instead. When the copy falls
/// back to reading and writing, every read of the source is pinned to the same generation and bounded
/// by `src_size`, so that a source blob overwritten during the copy cannot end up as a destination
/// stitched together from two generations, and an endpoint that answers with more or fewer bytes
/// than requested cannot corrupt the destination.
///
/// There is deliberately no way to ask for a part of the source: the native copy (`CopyFromUri` /
/// `StartCopyFromUri`) carries no byte range and always transfers the entire source blob, so a range
/// argument could only be honored by the read-write fallback and would be silently ignored whenever
/// the native copy is enabled. Callers that need a range read the source themselves and use
/// `copyDataToAzureBlobStorageFile()`.
void copyAzureBlobStorageFile(
    std::shared_ptr<const AzureBlobStorage::ContainerClient> src_client,
    std::shared_ptr<const AzureBlobStorage::ContainerClient> dest_client,
    const String & src_container_for_logging,
    const String & src_blob,
    size_t src_size,
    const String & src_etag,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    const ReadSettings & read_settings,
    const std::optional<ObjectAttributes> & object_to_attributes,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
    BlobStorageLogWriterPtr blob_storage_log = {});


/// Copies data from any seekable source to AzureBlobStorage.
/// The same functionality can be done by using the function copyData() and the class WriteBufferFromS3
/// however copyDataToS3File() is faster and spends less memory.
/// The callback `create_read_buffer` can be called from multiple threads in parallel, so that should be thread-safe.
/// The parameters `offset` and `size` specify a part in the source to copy.
void copyDataToAzureBlobStorageFile(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    std::shared_ptr<const AzureBlobStorage::ContainerClient> client,
    const String & dest_container_for_logging,
    const String & dest_blob,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
    BlobStorageLogWriterPtr blob_storage_log = {});

}

#endif
