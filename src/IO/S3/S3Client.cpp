#include <IO/S3/S3Client.h>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/core/client/AWSErrorMarshaller.h>
#include <aws/core/endpoint/EndpointParameter.h>

#include <IO/S3Common.h>
#include <IO/S3/Requests.h>

#include <Common/assert_cast.h>

#include <Common/logger_useful.h>

namespace DB::S3
{

bool S3Client::RetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const
{
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
        return false;

    return Aws::Client::DefaultRetryStrategy::ShouldRetry(error, attemptedRetries);
}

Model::HeadObjectOutcome S3Client::HeadObject(const Model::HeadObjectRequest & request) const
{
    if (const auto * casted_request = dynamic_cast<const HeadObjectRequest *>(&request); casted_request)
        casted_request->setClient(this);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid type of request, use derived request types and not the types from AWS SDK");

    auto bucket = request.GetBucket();

    if (detect_region)
    {
        std::lock_guard lock(region_cache_mutex);
        if (!getRegionForBucketAssumeLocked(bucket))
            updateRegionForBucketAssumeLocked(bucket);
    }

    auto result = Aws::S3::S3Client::HeadObject(request);
    if (!result.IsSuccess())
    {
        const auto & error = result.GetError();
        if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY && !getURIForBucket(bucket))
        {
            std::unique_lock lock(uri_cache_mutex);
            if (!getURIForBucketAssumeLocked(bucket))
            {
                lock.unlock();
                updateEndpointForBucketForHead(bucket);
                return Aws::S3::S3Client::HeadObject(request);
            }
        }
    }

    return result;
}

Model::ListObjectsV2Outcome S3Client::ListObjectsV2(const Model::ListObjectsV2Request & request) const
{
    return doRequest<ListObjectsV2Request>(
        request, [this](const Model::ListObjectsV2Request & req) { return Aws::S3::S3Client::ListObjectsV2(req); });
}

Model::GetObjectOutcome S3Client::GetObject(const Model::GetObjectRequest & request) const
{
    return doRequest<GetObjectRequest>(
        request, [this](const Model::GetObjectRequest & req) { return Aws::S3::S3Client::GetObject(req); });
}

Model::CreateMultipartUploadOutcome S3Client::CreateMultipartUpload(const Model::CreateMultipartUploadRequest & request) const
{
    return doRequest<CreateMultipartUploadRequest>(
        request, [this](const Model::CreateMultipartUploadRequest & req) { return Aws::S3::S3Client::CreateMultipartUpload(req); });
}

Model::CompleteMultipartUploadOutcome S3Client::CompleteMultipartUpload(const Model::CompleteMultipartUploadRequest & request) const
{
    return doRequest<CompleteMultipartUploadRequest>(
        request, [this](const Model::CompleteMultipartUploadRequest & req) { return Aws::S3::S3Client::CompleteMultipartUpload(req); });
}

Model::PutObjectOutcome S3Client::PutObject(const Model::PutObjectRequest & request) const
{
    return doRequest<PutObjectRequest>(
        request, [this](const Model::PutObjectRequest & req) { return Aws::S3::S3Client::PutObject(req); });
}

Model::UploadPartOutcome S3Client::UploadPart(const Model::UploadPartRequest & request) const
{
    return doRequest<UploadPartRequest>(
        request, [this](const Model::UploadPartRequest & req) { return Aws::S3::S3Client::UploadPart(req); });
}

Model::DeleteObjectOutcome S3Client::DeleteObject(const Model::DeleteObjectRequest & request) const
{
    return doRequest<DeleteObjectRequest>(
        request, [this](const Model::DeleteObjectRequest & req) { return Aws::S3::S3Client::DeleteObject(req); });
}

Model::DeleteObjectsOutcome S3Client::DeleteObjects(const Model::DeleteObjectsRequest & request) const
{
    return doRequest<DeleteObjectsRequest>(
        request, [this](const Model::DeleteObjectsRequest & req) { return Aws::S3::S3Client::DeleteObjects(req); });
}

void S3Client::updateRegionForBucketAssumeLocked(const std::string & bucket) const
{
    LOG_INFO(log, "Resolving region for bucket {}", bucket);
    Aws::S3::Model::HeadBucketRequest req;
    req.SetBucket(bucket);

    std::string region;
    auto outcome = HeadBucket(req);
    if (outcome.IsSuccess())
    {
        const auto & result = outcome.GetResult();
        region = result.GetRegion();
    }
    else
    {
        static const std::string region_header = "x-amz-bucket-region";
        const auto & headers = outcome.GetError().GetResponseHeaders();
        if (auto it = headers.find(region_header); it != headers.end())
            region = it->second;
    }

    if (region.empty())
    {
        LOG_INFO(log, "Failed resolving region for bucket {}", bucket);
        return;
    }

    LOG_INFO(log, "Found region {} for bucket {}", region, bucket);

    region_for_bucket_cache.emplace(bucket, region);
}

const std::string * S3Client::getRegionForBucket(const std::string & bucket) const
{
    std::lock_guard lock(region_cache_mutex);
    return getRegionForBucketAssumeLocked(bucket);
}

const std::string * S3Client::getRegionForBucketAssumeLocked(const std::string & bucket) const
{
    if (auto it = region_for_bucket_cache.find(bucket); it != region_for_bucket_cache.end())
        return &it->second;
    return nullptr;
}

bool S3Client::updateEndpointForBucketFromErrorAssumeLocked(const std::string & bucket, const Aws::S3::S3Error & error) const
{
    auto endpoint = GetErrorMarshaller()->ExtractEndpoint(error);
    if (endpoint.empty())
        return false;

    auto & s3_client = const_cast<S3Client &>(*this);
    const auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(s3_client.accessEndpointProvider().get());
    auto resolved_endpoint = endpoint_provider->ResolveEndpoint({});

    if (!resolved_endpoint.IsSuccess())
        return false;

    auto uri = resolved_endpoint.GetResult().GetURI();
    uri.SetAuthority(endpoint);

    S3::URI new_uri(uri.GetURIString());

    if (auto it = uri_for_bucket_cache.find(bucket); it != uri_for_bucket_cache.end())
    {
        if (it->second.uri == new_uri.uri)
        {
            LOG_INFO(log, "New endpoint found {}", uri.GetURIString());
            it->second = new_uri;
        }
    }
    else
    {
        LOG_INFO(log, "New endpoint found {}", uri.GetURIString());
        uri_for_bucket_cache.emplace(bucket, S3::URI(uri.GetURIString()));
    }

    return true;

}

void S3Client::updateEndpointForBucketForHead(const std::string & bucket) const
{
    LOG_INFO(log, "Resolving endpoint for bucket {}", bucket);
    ListObjectsV2Request req;
    req.SetMaxKeys(1);
    req.SetBucket(bucket);
    std::optional<S3::URI> last_uri;
    while (true)
    {
        auto result = ListObjectsV2(req);
        if (result.IsSuccess())
            return;

        if (result.GetError().GetResponseCode() != Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
            return;

        auto endpoint = GetErrorMarshaller()->ExtractEndpoint(result.GetError());
        if (endpoint.empty())
            return;

        std::unique_lock lock(uri_cache_mutex);
        if (!updateEndpointForBucketFromErrorAssumeLocked(bucket, result.GetError()))
            return;

        const auto * cached_uri_ptr = getURIForBucketAssumeLocked(bucket);

        if (!cached_uri_ptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "URI for bucket {} should be present in cache", bucket);

        auto cached_uri = *cached_uri_ptr;

        lock.unlock();

        // check if we already tried with this URI
        if (last_uri && last_uri->uri == cached_uri.uri)
            return;

        last_uri = std::move(cached_uri);
    }
}

const URI * S3Client::getURIForBucket(const std::string & bucket) const
{
    std::lock_guard lock(uri_cache_mutex);
    return getURIForBucketAssumeLocked(bucket);
}

const URI * S3Client::getURIForBucketAssumeLocked(const std::string & bucket) const
{
    if (auto it = uri_for_bucket_cache.find(bucket); it != uri_for_bucket_cache.end())
        return &it->second;

    return nullptr;
}

}
