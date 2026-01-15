#pragma once

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Settings.h>
#include <base/types.h>
#include <IO/S3/Client.h>

namespace DB
{
using ObjectAttributes = std::map<std::string, std::string>;
}

namespace DB::S3
{

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;
    String etag;
    ObjectAttributes tags; // Set only if getObjectInfo() is called with `with_tags = true`
    ObjectAttributes metadata = {}; /// Set only if getObjectInfo() is called with `with_metadata = true`.
};

/// Ignore if object does not exist
ObjectInfo getObjectInfoIfExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    bool with_metadata = false,
    bool with_tags = false);

ObjectInfo getObjectInfo(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    bool with_metadata = false,
    bool with_tags = false);

ObjectAttributes getObjectTags(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {});

size_t getObjectSize(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {});

bool objectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {});

/// Throws an exception if a specified object doesn't exist. `description` is used as a part of the error message.
void checkObjectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id = {},
    std::string_view description = {});

bool isNotFoundError(Aws::S3::S3Errors error);

}

#endif
