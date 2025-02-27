#include <IO/S3/getObjectInfo.h>

#if USE_AWS_S3

namespace ErrorCodes
{
    extern const int S3_ERROR;
}


namespace ProfileEvents
{
    extern const Event S3GetObject;
    extern const Event S3GetObjectAttributes;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObject;
    extern const Event DiskS3GetObjectAttributes;
    extern const Event DiskS3HeadObject;
}


namespace DB::S3
{

namespace
{
    Aws::S3::Model::HeadObjectOutcome headObject(
        const S3::Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3)
    {
        ProfileEvents::increment(ProfileEvents::S3HeadObject);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3HeadObject);

        S3::HeadObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        if (!version_id.empty())
            req.SetVersionId(version_id);

        return client.HeadObject(req);
    }

    /// Performs a request to get the size and last modification time of an object.
    std::pair<std::optional<ObjectInfo>, Aws::S3::S3Error> tryGetObjectInfo(
        const S3::Client & client, const String & bucket, const String & key, const String & version_id,
        const S3Settings::RequestSettings & /*request_settings*/, bool with_metadata, bool for_disk_s3)
    {
        auto outcome = headObject(client, bucket, key, version_id, for_disk_s3);
        if (!outcome.IsSuccess())
            return {std::nullopt, outcome.GetError()};

        const auto & result = outcome.GetResult();
        ObjectInfo object_info;
        object_info.size = static_cast<size_t>(result.GetContentLength());
        object_info.last_modification_time = result.GetLastModified().Millis() / 1000;

        if (with_metadata)
            object_info.metadata = result.GetMetadata();

        return {object_info, {}};
    }
}


bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

ObjectInfo getObjectInfo(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const S3Settings::RequestSettings & request_settings,
    bool with_metadata,
    bool for_disk_s3,
    bool throw_on_error)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, request_settings, with_metadata, for_disk_s3);
    if (object_info)
    {
        return *object_info;
    }
    else if (throw_on_error)
    {
        throw S3Exception(error.GetErrorType(),
            "Failed to get object info: {}. HTTP response code: {}",
            error.GetMessage(), static_cast<size_t>(error.GetResponseCode()));
    }
    return {};
}

size_t getObjectSize(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const S3Settings::RequestSettings & request_settings,
    bool for_disk_s3,
    bool throw_on_error)
{
    return getObjectInfo(client, bucket, key, version_id, request_settings, {}, for_disk_s3, throw_on_error).size;
}

bool objectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const S3Settings::RequestSettings & request_settings,
    bool for_disk_s3)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, request_settings, {}, for_disk_s3);
    if (object_info)
        return true;

    if (isNotFoundError(error.GetErrorType()))
        return false;

    throw S3Exception(error.GetErrorType(),
        "Failed to check existence of key {} in bucket {}: {}",
        key, bucket, error.GetMessage());
}

void checkObjectExists(
    const S3::Client & client,
    const String & bucket,
    const String & key,
    const String & version_id,
    const S3Settings::RequestSettings & request_settings,
    bool for_disk_s3,
    std::string_view description)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, request_settings, {}, for_disk_s3);
    if (object_info)
        return;
    throw S3Exception(error.GetErrorType(), "{}Object {} in bucket {} suddenly disappeared: {}",
                        (description.empty() ? "" : (String(description) + ": ")), key, bucket, error.GetMessage());
}
}

#endif
