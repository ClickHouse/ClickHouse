#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Common/ProfileEvents.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>
#include <memory>


namespace DB
{

namespace S3
{
    class Client;
}

class S3Capabilities;
class BlobStorageLogWriter;
using BlobStorageLogWriterPtr = std::shared_ptr<BlobStorageLogWriter>;


/// Deletes one file from S3.
///
/// A non-empty `etag_to_match` pins the delete to one generation of the object: it is sent as
/// `If-Match`, and an endpoint that evaluates it (S3 does, on general purpose and directory buckets
/// alike) refuses to delete an object that was written over since that generation was named. The
/// refusal (`412 Precondition Failed`, or the `409 Conflict` S3 answers when a concurrent write got
/// in first) is reported as `FILE_CHANGED_DURING_READ`, which is not a "does not exist" and is not
/// swallowed by `if_exists`. An endpoint that ignores `If-Match` on a `DELETE` (some S3-compatible
/// ones do) deletes by key, as it did before the header was sent.
void deleteFileFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const String & key,
    bool if_exists = false,
    BlobStorageLogWriterPtr blob_storage_log = nullptr,
    const String & local_path_for_blob_storage_log = {},
    size_t file_size_for_blob_storage_log = 0,
    std::optional<ProfileEvents::Event> profile_event = std::nullopt,
    const String & etag_to_match = {});

/// Deletes multiple files from S3 using batch requests when it's possible.
///
/// `etags_to_match`, when not empty, is parallel to `keys`: a non-empty entry pins the delete of that
/// key to one generation of the object (the `ETag` element of the `DeleteObjects` request, or
/// `If-Match` when the objects are deleted one by one), see `deleteFileFromS3`. Every object that can
/// be deleted is deleted before a refused one is reported with `FILE_CHANGED_DURING_READ`, and a
/// refused object is not among `successful_keys`.
void deleteFilesFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const Strings & keys,
    bool if_exists,
    S3Capabilities & s3_capabilities,
    size_t batch_size = 1000,
    BlobStorageLogWriterPtr blob_storage_log = nullptr,
    const Strings & local_paths_for_blob_storage_log = {},
    const VectorWithMemoryTracking<size_t> & file_sizes_for_blob_storage_log = {},
    std::optional<ProfileEvents::Event> profile_event = std::nullopt,
    Strings * successful_keys = nullptr,
    const Strings & etags_to_match = {});

}

#endif
