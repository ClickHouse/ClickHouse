#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Disks/IDisk.h>
#include <Storages/StorageS3Settings.h>
#include <Interpreters/threadPoolCallbackRunner.h>


namespace DB
{

/// Copies an object from S3 bucket to a disk of any type.
/// Depending on the disk the function can either do copying through buffers
/// (i.e. download the object by portions and then write those portions to the specified disk),
/// or perform a server-side copy.
void copyS3FileToDisk(
    const std::shared_ptr<const S3::Client> & src_s3_client,
    const String & src_bucket,
    const String & src_key,
    const std::optional<String> & version_id,
    std::optional<size_t> src_offset,
    std::optional<size_t> src_size,
    DiskPtr destination_disk,
    const String & destination_path,
    WriteMode write_mode = WriteMode::Rewrite,
    const ReadSettings & read_settings = {},
    const WriteSettings & write_settings = {},
    const S3Settings::RequestSettings & request_settings = {},
    ThreadPoolCallbackRunner<void> scheduler = {});

/// Copies an object from a disk of any type to S3 bucket.
/// Depending on the disk the function can either do copying through buffers
/// (i.e. read the object by portions and then upload those portions to the specified disk),
/// or perform a server-side copy.
void copyS3FileFromDisk(
    DiskPtr src_disk,
    const String & src_path,
    std::optional<size_t> src_offset,
    std::optional<size_t> src_size,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const ReadSettings & read_settings = {},
    const S3Settings::RequestSettings & request_settings = {},
    ThreadPoolCallbackRunner<void> scheduler = {});

}

#endif
