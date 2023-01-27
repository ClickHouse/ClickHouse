#include <IO/S3/getObjectInfo.h>

#if USE_AWS_S3
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectAttributesRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>


namespace ErrorCodes
{
    extern const int S3_ERROR;
}


namespace ProfileEvents
{
    extern const Event S3GetObjectAttributes;
    extern const Event S3GetObjectMetadata;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObjectAttributes;
    extern const Event DiskS3GetObjectMetadata;
    extern const Event DiskS3HeadObject;
}


namespace DB::S3
{

namespace
{
    /// Extracts the endpoint from a constructed S3 client.
    String getEndpoint(const Aws::S3::S3Client & client)
    {
        const auto * endpoint_provider = dynamic_cast<const Aws::S3::Endpoint::S3DefaultEpProviderBase *>(const_cast<Aws::S3::S3Client &>(client).accessEndpointProvider().get());
        if (!endpoint_provider)
            return {};
        String endpoint;
        endpoint_provider->GetBuiltInParameters().GetParameter("Endpoint").GetString(endpoint);
        return endpoint;
    }

    /// Performs a request to get the size and last modification time of an object.
    /// The function performs either HeadObject or GetObjectAttributes request depending on the endpoint.
    std::pair<std::optional<DB::S3::ObjectInfo>, Aws::S3::S3Error> tryGetObjectInfo(
        const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3)
    {
        auto endpoint = getEndpoint(client);
        bool use_get_object_attributes_request = (endpoint.find(".amazonaws.com") != String::npos);

        if (use_get_object_attributes_request)
        {
            /// It's better not to use `HeadObject` requests for AWS S3 because they don't work well with the global region.
            /// Details: `HeadObject` request never returns a response body (even if there is an error) however
            /// if the request was sent without specifying a region in the endpoint (i.e. for example "https://test.s3.amazonaws.com/mydata.csv"
            /// instead of "https://test.s3-us-west-2.amazonaws.com/mydata.csv") then that response body is one of the main ways
            /// to determine the correct region and try to repeat the request again with the correct region.
            /// For any other request type (`GetObject`, `ListObjects`, etc.) AWS SDK does that because they have response bodies,
            /// but for `HeadObject` there is no response body so this way doesn't work. That's why we use `GetObjectAttributes` request instead.
            /// See https://github.com/aws/aws-sdk-cpp/issues/1558 and also the function S3ErrorMarshaller::ExtractRegion() for more information.

            ProfileEvents::increment(ProfileEvents::S3GetObjectAttributes);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3GetObjectAttributes);

            Aws::S3::Model::GetObjectAttributesRequest req;
            req.SetBucket(bucket);
            req.SetKey(key);

            if (!version_id.empty())
                req.SetVersionId(version_id);

            req.SetObjectAttributes({Aws::S3::Model::ObjectAttributes::ObjectSize});

            auto outcome = client.GetObjectAttributes(req);
            if (outcome.IsSuccess())
            {
                const auto & result = outcome.GetResult();
                DB::S3::ObjectInfo object_info;
                object_info.size = static_cast<size_t>(result.GetObjectSize());
                object_info.last_modification_time = result.GetLastModified().Millis() / 1000;
                return {object_info, {}};
            }

            return {std::nullopt, outcome.GetError()};
        }
        else
        {
            /// By default we use `HeadObject` requests.
            /// We cannot just use `GetObjectAttributes` requests always because some S3 providers (e.g. Minio)
            /// don't support `GetObjectAttributes` requests.

            ProfileEvents::increment(ProfileEvents::S3HeadObject);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3HeadObject);

            Aws::S3::Model::HeadObjectRequest req;
            req.SetBucket(bucket);
            req.SetKey(key);

            if (!version_id.empty())
                req.SetVersionId(version_id);

            auto outcome = client.HeadObject(req);
            if (outcome.IsSuccess())
            {
                const auto & result = outcome.GetResult();
                DB::S3::ObjectInfo object_info;
                object_info.size = static_cast<size_t>(result.GetContentLength());
                object_info.last_modification_time = result.GetLastModified().Millis() / 1000;
                return {object_info, {}};
            }

            return {std::nullopt, outcome.GetError()};
        }
    }
}


bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3, bool throw_on_error)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, for_disk_s3);
    if (object_info)
    {
        return *object_info;
    }
    else if (throw_on_error)
    {
        throw DB::Exception(ErrorCodes::S3_ERROR,
            "Failed to get object attributes: {}. HTTP response code: {}",
            error.GetMessage(), static_cast<size_t>(error.GetResponseCode()));
    }
    return {};
}

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3, bool throw_on_error)
{
    return getObjectInfo(client, bucket, key, version_id, for_disk_s3, throw_on_error).size;
}

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, for_disk_s3);
    if (object_info)
        return true;

    if (isNotFoundError(error.GetErrorType()))
        return false;

    throw S3Exception(error.GetErrorType(),
        "Failed to check existence of key {} in bucket {}: {}",
        key, bucket, error.GetMessage());
}

void checkObjectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3, std::string_view description)
{
    auto [object_info, error] = tryGetObjectInfo(client, bucket, key, version_id, for_disk_s3);
    if (object_info)
        return;
    throw S3Exception(error.GetErrorType(), "{}Object {} in bucket {} suddenly disappeared: {}",
                        (description.empty() ? "" : (String(description) + ": ")), key, bucket, error.GetMessage());
}

std::map<String, String> getObjectMetadata(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3, bool throw_on_error)
{
    ProfileEvents::increment(ProfileEvents::S3GetObjectMetadata);
    if (for_disk_s3)
        ProfileEvents::increment(ProfileEvents::DiskS3GetObjectMetadata);

    /// We must not use the `HeadObject` request, see the comment about `HeadObjectRequest` in S3Common.h.

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    /// Only the first byte will be read.
    /// We don't need that first byte but the range should be set otherwise the entire object will be read.
    req.SetRange("bytes=0-0");

    if (!version_id.empty())
        req.SetVersionId(version_id);

    auto outcome = client.GetObject(req);

    if (outcome.IsSuccess())
        return outcome.GetResult().GetMetadata();

    if (!throw_on_error)
        return {};

    const auto & error = outcome.GetError();
    throw S3Exception(error.GetErrorType(),
        "Failed to get metadata of key {} in bucket {}: {}",
        key, bucket, error.GetMessage());
}

}

#endif
