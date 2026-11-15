#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Common/ProfileEvents.h>
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
void deleteFileFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const String & key,
    bool if_exists = false,
    BlobStorageLogWriterPtr blob_storage_log = nullptr,
    const String & local_path_for_blob_storage_log = {},
    size_t file_size_for_blob_storage_log = 0,
    std::optional<ProfileEvents::Event> profile_event = std::nullopt);

/// Deletes multiple files from S3 using batch requests when it's possible.
void deleteFilesFromS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const Strings & keys,
    bool if_exists,
    S3Capabilities & s3_capabilities,
    size_t batch_size = 1000,
    BlobStorageLogWriterPtr blob_storage_log = nullptr,
    const Strings & local_paths_for_blob_storage_log = {},
    const std::vector<size_t> & file_sizes_for_blob_storage_log = {},
    std::optional<ProfileEvents::Event> profile_event = std::nullopt);

}

#endif
