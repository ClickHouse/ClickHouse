#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{
struct ReadSettings;
class SeekableReadBuffer;

using CreateReadBuffer = std::function<std::unique_ptr<SeekableReadBuffer>()>;

/// Copies a file from S3 to S3.
/// The same functionality can be done by using the function copyData() and the classes ReadBufferFromS3 and WriteBufferFromS3
/// however copyS3File() is faster and spends less network traffic and memory.
/// The parameters `src_offset` and `src_size` specify a part in the source to copy.
///
/// Note, that it tries to copy file using native copy (CopyObject), but if it
/// has been disabled (with settings.allow_native_copy) or request failed
/// because it is a known issue, it is fallbacks to read-write copy
/// (copyDataToS3File()).
///
/// read_settings - is used for throttling in case of native copy is not possible
void copyS3File(
    const std::shared_ptr<const S3::Client> & src_s3_client,
    const String & src_bucket,
    const String & src_key,
    size_t src_offset,
    size_t src_size,
    std::shared_ptr<const S3::Client> dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const S3::S3RequestSettings & settings,
    const ReadSettings & read_settings,
    BlobStorageLogWriterPtr blob_storage_log,
    const std::optional<std::map<String, String>> & object_metadata = std::nullopt,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
    bool for_disk_s3 = false);

/// Copies data from any seekable source to S3.
/// The same functionality can be done by using the function copyData() and the class WriteBufferFromS3
/// however copyDataToS3File() is faster and spends less memory.
/// The callback `create_read_buffer` can be called from multiple threads in parallel, so that should be thread-safe.
/// The parameters `offset` and `size` specify a part in the source to copy.
void copyDataToS3File(
    const CreateReadBuffer & create_read_buffer,
    size_t offset,
    size_t size,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const S3::S3RequestSettings & settings,
    BlobStorageLogWriterPtr blob_storage_log,
    const std::optional<std::map<String, String>> & object_metadata = std::nullopt,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_ = {},
    bool for_disk_s3 = false);

}

#endif
