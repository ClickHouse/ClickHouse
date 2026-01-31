#include <optional>
#include <cstdlib>
#include <filesystem>
#include <vector>
#include <IO/S3/getObjectInfo.h>
#include <IO/Expect404ResponseScope.h>

#if USE_AWS_S3

namespace ErrorCodes
{
    extern const int S3_ERROR;
}


namespace ProfileEvents
{
    extern const Event S3GetObject;
    extern const Event S3GetObjectTagging;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObject;
    extern const Event DiskS3GetObjectTagging;
    extern const Event DiskS3HeadObject;
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
        const String & bucket,
        const String & key,
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
        object_info.last_modification_time = result.GetLastModified().Seconds();
        object_info.etag = result.GetETag();

        if (with_metadata)
            object_info.metadata = result.GetMetadata();

        if (with_tags && result.GetTagCount() > 0)
            object_info.tags = getObjectTags(client, bucket, key, version_id);

        return {object_info, {}};
    }
}

ObjectAttributes getObjectTags(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id)
{
    ObjectAttributes tags;
    auto tag_outcome = getObjectTagging(client, bucket, key, version_id);
    if (!tag_outcome.IsSuccess())
    {
        const auto & error = tag_outcome.GetError();
        throw S3Exception(
            error.GetErrorType(),
            "Failed to get object tags: {}. HTTP response code: {}.{}",
            error.GetMessage(),
            static_cast<size_t>(error.GetResponseCode()),
            getAuthenticationErrorHint(error.GetErrorType()));
    }

    for (const auto & tag : tag_outcome.GetResult().GetTagSet())
        tags[tag.GetKey()] = tag.GetValue();

    return tags;
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

String getAuthenticationErrorHint(Aws::S3::S3Errors error)
{
    if (!isAuthenticationError(error))
        return "";

    std::vector<String> possible_sources;

    /// Check for environment variable credentials
    if (std::getenv("AWS_ACCESS_KEY_ID"))
        possible_sources.push_back("environment variables (AWS_ACCESS_KEY_ID)");

    /// Check for ECS container credentials
    if (std::getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") || std::getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"))
        possible_sources.push_back("ECS container credentials");

    /// Check for web identity / assume role credentials
    if (std::getenv("AWS_WEB_IDENTITY_TOKEN_FILE") || std::getenv("AWS_ROLE_ARN"))
        possible_sources.push_back("STS AssumeRole with web identity");

    /// Check for default credentials file
    const char * home = std::getenv("HOME");
    String credentials_file;
    if (const char * custom_file = std::getenv("AWS_SHARED_CREDENTIALS_FILE"))
        credentials_file = custom_file;
    else if (home)
        credentials_file = String(home) + "/.aws/credentials";

    if (!credentials_file.empty() && std::filesystem::exists(credentials_file))
        possible_sources.push_back(credentials_file);

    String hint = " Please check your AWS credentials and permissions.";
    if (!possible_sources.empty())
    {
        hint += " Possible credential sources: ";
        for (size_t i = 0; i < possible_sources.size(); ++i)
        {
            if (i > 0)
                hint += ", ";
            hint += possible_sources[i];
        }
        hint += ".";
    }
    return hint;
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
        "Failed to get object info: {}. HTTP response code: {}.{}",
        error.GetMessage(),
        static_cast<size_t>(error.GetResponseCode()),
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
        "Failed to get object info: {}. HTTP response code: {}.{}",
        error.GetMessage(),
        static_cast<size_t>(error.GetResponseCode()),
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
        key, bucket, error.GetMessage(), static_cast<size_t>(error.GetResponseCode()),
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
