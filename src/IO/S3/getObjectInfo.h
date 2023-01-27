#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Storages/StorageS3Settings.h>
#include <base/types.h>
#include <aws/s3/S3Client.h>


namespace DB::S3
{

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;
};

ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, const S3Settings::RequestSettings & settings, bool for_disk_s3 = false, bool throw_on_error = true);

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, const S3Settings::RequestSettings & settings, bool for_disk_s3 = false, bool throw_on_error = true);

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, const S3Settings::RequestSettings & settings, bool for_disk_s3 = false);

/// Throws an exception if a specified object doesn't exist. `description` is used as a part of the error message.
void checkObjectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, const S3Settings::RequestSettings & settings, bool for_disk_s3 = false, std::string_view description = {});

/// Returns the object's metadata.
std::map<String, String> getObjectMetadata(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, const S3Settings::RequestSettings & settings, bool for_disk_s3 = false, bool throw_on_error = true);

bool isNotFoundError(Aws::S3::S3Errors error);

}

#endif
