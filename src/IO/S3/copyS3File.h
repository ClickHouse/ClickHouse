#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageS3Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/CoTask_fwd.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{
class SeekableReadBuffer;

struct CopyS3FileSettings
{
    S3Settings::RequestSettings request_settings;

    /// A part of the source to copy.
    size_t offset = 0;
    size_t size = static_cast<size_t>(-1);

    /// The entire source file must be copied. If set, `offset` must be zero and `size` can be either the size of the source file or `-1`.
    bool whole_file = false;

    /// Metadata to store on the target. If set the metadata on the target will be replaced with it.
    std::optional<std::map<String, String>> object_metadata;

    /// Used for profiling events only.
    bool for_disk_s3 = false;
};

/// Copies a file from S3 to S3.
/// The same functionality can be done by using the function copyData() and the classes ReadBufferFromS3 and WriteBufferFromS3
/// however copyS3File() is faster and spends less network traffic and memory.
/// The parameters `src_offset` and `src_size` specify a part in the source to copy.
void copyS3File(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & src_bucket,
    const String & src_key,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings,
    const ThreadPoolCallbackRunner<void> & scheduler = {});

Co::Task<> copyS3FileAsync(
    std::shared_ptr<const S3::Client> s3_client,
    const String src_bucket,
    const String src_key,
    const String dest_bucket,
    const String dest_key,
    const CopyS3FileSettings copy_settings);

/// Copies data from any seekable source to S3.
/// The same functionality can be done by using the function copyData() and the class WriteBufferFromS3
/// however copyDataToS3File() is faster and spends less memory.
/// The callback `create_read_buffer` can be called from multiple threads in parallel, so that should be thread-safe.
/// The parameters `offset` and `size` specify a part in the source to copy.
void copyDataToS3File(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings,
    const ThreadPoolCallbackRunner<void> & schedule_ = {});

Co::Task<> copyDataToS3FileAsync(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer,
    const std::shared_ptr<const S3::Client> dest_s3_client,
    const String dest_bucket,
    const String dest_key,
    const CopyS3FileSettings copy_settings);

}

#endif
