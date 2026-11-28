#include <optional>
#include <IO/S3/getObjectInfo.h>
#include <IO/Expect404ResponseScope.h>
#include <Common/FailPoint.h>

#if USE_AWS_S3

namespace ProfileEvents
{
    extern const Event S3GetObjectTagging;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObjectTagging;
    extern const Event DiskS3HeadObject;
}


namespace DB::FailPoints
{
    extern const char s3_pause_between_head_object_and_get_object_tagging[];
    extern const char s3_pause_before_get_object_tags_of_generation[];
}

namespace DB::ErrorCodes
{
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

namespace DB::S3
{

namespace
{
    Aws::S3::Model::HeadObjectOutcome headObject(
        const S3::Client & client,
        const String & bucket,
        const String & key,
        const String & version_id)
    {
        ProfileEvents::increment(ProfileEvents::S3HeadObject);
        if (client.isClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskS3HeadObject);

        S3::HeadObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        if (!version_id.empty())
            req.SetVersionId(version_id);

        return client.HeadObject(req);
    }

    Aws::S3::Model::GetObjectTaggingOutcome getObjectTagging(
        const S3::Client & client,
        std::string_view bucket,
        std::string_view key,
        const String & version_id)
    {
        ProfileEvents::increment(ProfileEvents::S3GetObjectTagging);
        if (client.isClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskS3GetObjectTagging);

        S3::GetObjectTaggingRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        if (!version_id.empty())
            req.SetVersionId(version_id);

        return client.GetObjectTagging(req);
    }

    /// Performs a request to get the size and last modification time of an object.
    std::pair<std::optional<ObjectInfo>, Aws::S3::S3Error> tryGetObjectInfo(
        const S3::Client & client,
        const String & bucket,
        const String & key,
        const String & version_id,
        bool with_metadata,
        bool with_tags)
    {
        auto outcome = headObject(client, bucket, key, version_id);
        if (!outcome.IsSuccess())
            return {std::nullopt, outcome.GetError()};

        const auto & result = outcome.GetResult();
        ObjectInfo object_info;
        object_info.size = static_cast<size_t>(result.GetContentLength());
        object_info.is_size_known = result.ContentLengthHasBeenSet();
        object_info.last_modification_time = result.GetLastModified().Seconds();
        object_info.etag = result.GetETag();
        object_info.version_id = pinnableVersionId(result.GetVersionId());

        object_info.headers.content_type = result.GetContentType();
        object_info.headers.content_encoding = result.GetContentEncoding();
        object_info.headers.content_language = result.GetContentLanguage();
        object_info.headers.content_disposition = result.GetContentDisposition();
        object_info.headers.cache_control = result.GetCacheControl();
        object_info.headers.expires = result.GetExpires();
        object_info.headers.website_redirect_location = result.GetWebsiteRedirectLocation();

        if (with_metadata)
            object_info.metadata = objectAttributesFromAwsMap(result.GetMetadata());

        /// `HeadObject` reports the tag count, so untagged objects cost no `GetObjectTagging` call; callers that
        /// need tags hidden from a restricted `HeadObject` ask via `getObjectTags`.
        /// Without a `versionId` the tags are of the version that is the latest at the time of `GetObjectTagging`. If it
        /// is not the version described by `HeadObject`, the object was rewritten in between, and the tags would not
        /// belong to the reported size, `ETag` and version.
        if (with_tags && result.GetTagCount() > 0)
        {
            FailPointInjection::pauseFailPoint(FailPoints::s3_pause_between_head_object_and_get_object_tagging);

            String tags_version_id;
            object_info.tags = getObjectTags(client, bucket, key, version_id, &tags_version_id);
            if (version_id.empty())
            {
                if (!object_info.version_id.empty() && !tags_version_id.empty())
                {
                    if (tags_version_id != object_info.version_id)
                        throw Exception(
                            ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                            "The object {} in bucket {} was rewritten while its tags were read (version {} by `HeadObject`, {} by `GetObjectTagging`)",
                            key, bucket, object_info.version_id, tags_version_id);
                }
                else
                {
                    /// Without versions `GetObjectTagging` names nothing that the tags could be checked against (it reports
                    /// no `ETag`). So `HeadObject` is repeated: if it still describes the same generation, nothing was
                    /// rewritten before the tags were read, and they are of that generation.
                    auto recheck = headObject(client, bucket, key, version_id);
                    if (!recheck.IsSuccess())
                    {
                        if (isNotFoundError(recheck.GetError().GetErrorType()))
                            throw Exception(
                                ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                                "The object {} in bucket {} was removed while its tags were read", key, bucket);
                        return {std::nullopt, recheck.GetError()};
                    }
                    const auto & recheck_result = recheck.GetResult();
                    if (recheck_result.GetETag() != result.GetETag()
                        || recheck_result.GetLastModified() != result.GetLastModified()
                        || pinnableVersionId(recheck_result.GetVersionId()) != object_info.version_id)
                        throw Exception(
                            ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                            "The object {} in bucket {} was rewritten while its tags were read (`ETag` {} before `GetObjectTagging`, {} after it)",
                            key, bucket, object_info.etag, recheck_result.GetETag());
                }
            }
        }

        return {object_info, {}};
    }
}

bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY
        || error == Aws::S3::S3Errors::NO_SUCH_BUCKET;
}

bool isAuthenticationError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::ACCESS_DENIED
        || error == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID
        || error == Aws::S3::S3Errors::INVALID_SIGNATURE;
}

static String getAuthenticationErrorHint(Aws::S3::S3Errors error)
{
    if (isAuthenticationError(error))
        return " Please check your AWS credentials and permissions.";
    return "";
}

ObjectAttributes getObjectTags(
    const S3::Client & client,
    std::string_view bucket,
    std::string_view key,
    const String & version_id,
    String * tags_version_id)
{
    ObjectAttributes tags;
    auto tag_outcome = getObjectTagging(client, bucket, key, version_id);
    if (!tag_outcome.IsSuccess())
    {
        const auto & error = tag_outcome.GetError();
        throw S3Exception(
            error.GetErrorType(),
            "Failed to get object tags of key {} in bucket {}: {}. HTTP response code: {}, error type: {}.{}",
            key,
            bucket,
            error.GetMessage(),
            error.GetResponseCode(),
            error.GetErrorType(),
            getAuthenticationErrorHint(error.GetErrorType()));
    }

    for (const auto & tag : tag_outcome.GetResult().GetTagSet())
        tags[String(tag.GetKey())] = tag.GetValue();

    if (tags_version_id)
        *tags_version_id = pinnableVersionId(tag_outcome.GetResult().GetVersionId());

    return tags;
}

ObjectAttributes getObjectTagsOfGeneration(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const String & expected_etag)
{
    FailPointInjection::pauseFailPoint(FailPoints::s3_pause_before_get_object_tags_of_generation);

    auto tags = getObjectTags(client, bucket, key, version_id);
    if (!version_id.empty() || expected_etag.empty())
        return tags;

    /// Looked up after the tags, so a replacement that the tags could have come from is still what the key points at.
    const auto current = getObjectInfo(client, bucket, key, /*version_id=*/{}, /*with_metadata=*/false, /*with_tags=*/false);
    if (current.etag != expected_etag)
        throw Exception(
            ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
            "The object {} in bucket {} was rewritten before its tags were read (its `ETag` is {} instead of {})",
            key, bucket, current.etag, expected_etag);
    return tags;
}

ObjectInfo getObjectInfoIfExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    bool with_metadata,
    bool with_tags)
{
    Expect404ResponseScope scope; // 404 is not an error

    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, with_metadata, with_tags);
    if (object_info)
        return *object_info;

    if (isNotFoundError(error.GetErrorType()))
        return {};

    throw S3Exception(
        error.GetErrorType(),
        "Failed to get object info for key {} in bucket {}: {}. HTTP response code: {}, error type: {}.{}",
        key,
        bucket,
        error.GetMessage(),
        error.GetResponseCode(),
        error.GetErrorType(),
        getAuthenticationErrorHint(error.GetErrorType()));
}

ObjectInfo getObjectInfo(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    bool with_metadata,
    bool with_tags)
{
    Expect404ResponseScope scope; // 404 is not an error

    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, with_metadata, with_tags);

    if (object_info)
        return *object_info;

    throw S3Exception(
        error.GetErrorType(),
        "Failed to get object info for key {} in bucket {}: {}. HTTP response code: {}, error type: {}.{}",
        key,
        bucket,
        error.GetMessage(),
        error.GetResponseCode(),
        error.GetErrorType(),
        getAuthenticationErrorHint(error.GetErrorType()));
}

size_t getObjectSize(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id)
{
    return getObjectInfo(client, bucket, key, version_id, /*with_metadata=*/ false, /*with_tags=*/ false).size;
}

bool objectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id)
{
    Expect404ResponseScope scope; // 404 is not an error

    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, {}, {});

    if (object_info)
        return true;

    if (isNotFoundError(error.GetErrorType()))
        return false;

    throw S3Exception(error.GetErrorType(),
        "Failed to check existence of key {} in bucket {}: {}. HTTP response code: {}, error type: {}.{}",
        key, bucket, error.GetMessage(), error.GetResponseCode(),
        error.GetErrorType(), getAuthenticationErrorHint(error.GetErrorType()));
}

void checkObjectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    std::string_view description)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, {}, {});
    if (object_info)
        return;
    throw S3Exception(error.GetErrorType(), "{}Object {} in bucket {} suddenly disappeared: {}",
                        (description.empty() ? "" : (String(description) + ": ")), key, bucket, error.GetMessage());
}
}

#endif
