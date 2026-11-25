#pragma once

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Settings.h>
#include <base/types.h>
#include <IO/S3/Client.h>

namespace DB::S3
{

/// Headers to restate when a guarded copy uses a read-write upload.
struct ObjectHeaders
{
    String content_type;
    String content_encoding;
    String content_language;
    String content_disposition;
    String cache_control;
    Aws::Utils::DateTime expires;
    String website_redirect_location;
};

struct ObjectInfo
{
    size_t size = 0;
    /// Checks if Content-Length was present in the HEAD response and we can rely on its result
    bool is_size_known = true;
    time_t last_modification_time = 0;
    String etag;
    /// Set only on buckets with versioning enabled; empty otherwise.
    String version_id;
    ObjectAttributes tags; // Set only if getObjectInfo() is called with `with_tags = true`
    ObjectAttributes metadata = {}; /// Set only if getObjectInfo() is called with `with_metadata = true`.
    ObjectHeaders headers; /// Always read from the HEAD response; empty for whatever the object does not set.
};

/// Empty for `null`, the version id that every upload made while versioning is off or suspended shares.
inline String pinnableVersionId(std::string_view version_id) { return version_id == "null" ? String{} : String(version_id); }

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

/// If `tags_version_id` is not null, it receives the version whose tags were returned (empty if not reported).
ObjectAttributes getObjectTags(
    const S3::Client & client,
    std::string_view bucket,
    std::string_view key,
    const String & version_id = {},
    String * tags_version_id = nullptr);

/// The tags of the generation of an object that the caller has pinned everything else to, for a copy that restates
/// them on a request it sends after it has read the object. `GetObjectTagging` takes no `ETag` condition, so without
/// a `version_id` it answers for whatever the key points at now, which may be a newer generation than the bytes the
/// caller already holds. The key is therefore looked up again after the tags are read, and
/// `S3_OBJECT_CHANGED_DURING_READ` is thrown unless it still has `expected_etag`. Nothing is checked if `version_id`
/// names the generation or if `expected_etag` is empty (the caller has pinned nothing).
ObjectAttributes getObjectTagsOfGeneration(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const String & expected_etag);

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
bool isAuthenticationError(Aws::S3::S3Errors error);

}

#endif
