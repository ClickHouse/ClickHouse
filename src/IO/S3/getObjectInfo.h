#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Storages/StorageS3Settings.h>
#include <base/types.h>
#include <IO/S3/Client.h>


namespace DB::S3
{

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;

    std::map<String, String> metadata = {}; /// Set only if getObjectInfo() is called with `with_metadata = true`.
};

ObjectInfo getObjectInfo(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    const S3Settings::RequestSettings & request_settings = {},
    bool with_metadata = false,
    bool for_disk_s3 = false,
    bool throw_on_error = true);

size_t getObjectSize(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    const S3Settings::RequestSettings & request_settings = {},
    bool for_disk_s3 = false,
    bool throw_on_error = true);

bool objectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    const S3Settings::RequestSettings & request_settings = {},
    bool for_disk_s3 = false);

/// Throws an exception if a specified object doesn't exist. `description` is used as a part of the error message.
void checkObjectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    const S3Settings::RequestSettings & request_settings = {},
    bool for_disk_s3 = false,
    std::string_view description = {});

bool isNotFoundError(Aws::S3::S3Errors error);

}

#endif
