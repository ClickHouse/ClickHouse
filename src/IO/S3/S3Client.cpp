#include <IO/S3/S3Client.h>

#if USE_AWS_S3

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

namespace DB
{

namespace S3
{

bool S3Client::RetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const
{
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
        return false;

    return Aws::Client::DefaultRetryStrategy::ShouldRetry(error, attemptedRetries);
}


bool S3Client::checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const
{
    if (detect_region)
        return false;

    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST && error.GetExceptionName() == "AuthorizationHeaderMalformed")
    {
        region = GetErrorMarshaller()->ExtractRegion(error);

        if (region.empty())
            region = getRegionForBucket(bucket, /*force_detect*/ true);

        assert(!explicit_region.empty());
        if (region == explicit_region)
            return false;

        insertRegionOverride(bucket, region);
        return true;
    }

    return false;
}

void S3Client::insertRegionOverride(const std::string & bucket, const std::string & region) const
{
    std::lock_guard lock(cache->region_cache_mutex);
    auto [it, inserted] = cache->region_for_bucket_cache.emplace(bucket, region);
    if (inserted)
        LOG_INFO(log, "Detected different region ('{}') for bucket {} than the one defined ('{}')", region, bucket, explicit_region);
}

Model::HeadObjectOutcome S3Client::HeadObject(const HeadObjectRequest & request) const
{
    const auto & bucket = request.GetBucket();

    if (auto region = getRegionForBucket(bucket); !region.empty())
    {
        if (!detect_region)
            LOG_INFO(log, "Using region override {} for bucket {}", region, bucket);
        request.overrideRegion(std::move(region));
    }

    if (auto uri = getURIForBucket(bucket); uri.has_value())
        request.overrideURI(std::move(*uri));

    auto result = Aws::S3::S3Client::HeadObject(request);
    if (result.IsSuccess())
        return result;

    const auto & error = result.GetError();

    std::string new_region;
    if (checkIfWrongRegionDefined(bucket, error, new_region))
    {
        request.overrideRegion(new_region);
        return HeadObject(request);
    }

    if (error.GetResponseCode() != Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
        return result;

    // maybe we detect a correct region
    if (!detect_region)
    {
        if (auto region = GetErrorMarshaller()->ExtractRegion(error); !region.empty() && region != explicit_region)
        {
            request.overrideRegion(region);
            insertRegionOverride(bucket, region);
        }
    }

    auto bucket_uri = getURIForBucket(bucket);
    if (!bucket_uri)
    {
        if (!updateURIForBucketForHead(bucket))
            return result;

        if (auto region = getRegionForBucket(bucket); !region.empty())
        {
            if (!detect_region)
                LOG_INFO(log, "Using region override {} for bucket {}", region, bucket);
            request.overrideRegion(std::move(region));
        }

        bucket_uri = getURIForBucket(bucket);
        if (!bucket_uri)
        {
            LOG_ERROR(log, "Missing resolved URI for bucket {}, maybe the cache was cleaned", bucket);
            return result;
        }
    }

    const auto & current_uri_override = request.getURIOverride();
    /// we already tried with this URI
    if (current_uri_override && current_uri_override->uri == bucket_uri->uri)
    {
        LOG_INFO(log, "Getting redirected to the same invalid location {}", bucket_uri->uri.toString());
        return result;
    }

    request.overrideURI(std::move(*bucket_uri));

    return Aws::S3::S3Client::HeadObject(request);
}

Model::ListObjectsV2Outcome S3Client::ListObjectsV2(const ListObjectsV2Request & request) const
{
    return doRequest(request, [this](const Model::ListObjectsV2Request & req) { return Aws::S3::S3Client::ListObjectsV2(req); });
}

Model::ListObjectsOutcome S3Client::ListObjects(const ListObjectsRequest & request) const
{
    return doRequest(request, [this](const Model::ListObjectsRequest & req) { return Aws::S3::S3Client::ListObjects(req); });
}

Model::GetObjectOutcome S3Client::GetObject(const GetObjectRequest & request) const
{
    return doRequest(request, [this](const Model::GetObjectRequest & req) { return Aws::S3::S3Client::GetObject(req); });
}

Model::AbortMultipartUploadOutcome S3Client::AbortMultipartUpload(const AbortMultipartUploadRequest & request) const
{
    return doRequest(
        request, [this](const Model::AbortMultipartUploadRequest & req) { return Aws::S3::S3Client::AbortMultipartUpload(req); });
}

Model::CreateMultipartUploadOutcome S3Client::CreateMultipartUpload(const CreateMultipartUploadRequest & request) const
{
    return doRequest(
        request, [this](const Model::CreateMultipartUploadRequest & req) { return Aws::S3::S3Client::CreateMultipartUpload(req); });
}

Model::CompleteMultipartUploadOutcome S3Client::CompleteMultipartUpload(const CompleteMultipartUploadRequest & request) const
{
    return doRequest(
        request, [this](const Model::CompleteMultipartUploadRequest & req) { return Aws::S3::S3Client::CompleteMultipartUpload(req); });
}

Model::CopyObjectOutcome S3Client::CopyObject(const CopyObjectRequest & request) const
{
    return doRequest(request, [this](const Model::CopyObjectRequest & req) { return Aws::S3::S3Client::CopyObject(req); });
}

Model::PutObjectOutcome S3Client::PutObject(const PutObjectRequest & request) const
{
    return doRequest(request, [this](const Model::PutObjectRequest & req) { return Aws::S3::S3Client::PutObject(req); });
}

Model::UploadPartOutcome S3Client::UploadPart(const UploadPartRequest & request) const
{
    return doRequest(request, [this](const Model::UploadPartRequest & req) { return Aws::S3::S3Client::UploadPart(req); });
}

Model::UploadPartCopyOutcome S3Client::UploadPartCopy(const UploadPartCopyRequest & request) const
{
    return doRequest(request, [this](const Model::UploadPartCopyRequest & req) { return Aws::S3::S3Client::UploadPartCopy(req); });
}

Model::DeleteObjectOutcome S3Client::DeleteObject(const DeleteObjectRequest & request) const
{
    return doRequest(request, [this](const Model::DeleteObjectRequest & req) { return Aws::S3::S3Client::DeleteObject(req); });
}

Model::DeleteObjectsOutcome S3Client::DeleteObjects(const DeleteObjectsRequest & request) const
{
    return doRequest(request, [this](const Model::DeleteObjectsRequest & req) { return Aws::S3::S3Client::DeleteObjects(req); });
}

std::string S3Client::getRegionForBucket(const std::string & bucket, bool force_detect) const
{
    std::lock_guard lock(cache->region_cache_mutex);
    if (auto it = cache->region_for_bucket_cache.find(bucket); it != cache->region_for_bucket_cache.end())
        return it->second;

    if (!force_detect && !detect_region)
        return "";


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
        return "";
    }

    LOG_INFO(log, "Found region {} for bucket {}", region, bucket);

    auto [it, _] = cache->region_for_bucket_cache.emplace(bucket, std::move(region));

    return it->second;
}

std::optional<S3::URI> S3Client::getURIFromError(const Aws::S3::S3Error & error) const
{
    auto endpoint = GetErrorMarshaller()->ExtractEndpoint(error);
    if (endpoint.empty())
        return std::nullopt;

    auto & s3_client = const_cast<S3Client &>(*this);
    const auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(s3_client.accessEndpointProvider().get());
    auto resolved_endpoint = endpoint_provider->ResolveEndpoint({});

    if (!resolved_endpoint.IsSuccess())
        return std::nullopt;

    auto uri = resolved_endpoint.GetResult().GetURI();
    uri.SetAuthority(endpoint);

    return S3::URI(uri.GetURIString());
}

// Do a list request because head requests don't have body in response
bool S3Client::updateURIForBucketForHead(const std::string & bucket) const
{
    ListObjectsV2Request req;
    req.SetBucket(bucket);
    req.SetMaxKeys(1);
    auto result = ListObjectsV2(req);
    return result.IsSuccess();
}

std::optional<S3::URI> S3Client::getURIForBucket(const std::string & bucket) const
{
    std::lock_guard lock(cache->uri_cache_mutex);
    if (auto it = cache->uri_for_bucket_cache.find(bucket); it != cache->uri_for_bucket_cache.end())
        return it->second;

    return std::nullopt;
}

void S3Client::updateURIForBucket(const std::string & bucket, S3::URI new_uri) const
{
    std::lock_guard lock(cache->uri_cache_mutex);
    if (auto it = cache->uri_for_bucket_cache.find(bucket); it != cache->uri_for_bucket_cache.end())
    {
        if (it->second.uri == new_uri.uri)
            return;

        LOG_INFO(log, "Updating URI for bucket {} to {}", bucket, new_uri.uri.toString());
        it->second = std::move(new_uri);

        return;
    }

    LOG_INFO(log, "Updating URI for bucket {} to {}", bucket, new_uri.uri.toString());
    cache->uri_for_bucket_cache.emplace(bucket, std::move(new_uri));
}

void S3ClientCacheRegistry::registerClient(const std::shared_ptr<S3ClientCache> & client_cache)
{
    std::lock_guard lock(clients_mutex);
    auto [it, inserted] = client_caches.emplace(client_cache.get(), client_cache);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Same S3 client registered twice");
}

void S3ClientCacheRegistry::unregisterClient(S3ClientCache * client)
{
    std::lock_guard lock(clients_mutex);
    auto erased = client_caches.erase(client);
    if (erased == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unregister S3 client, either it was already unregistered or not registered at all");
}

void S3ClientCacheRegistry::clearCacheForAll()
{
    std::lock_guard lock(clients_mutex);

    for (auto it = client_caches.begin(); it != client_caches.end();)
    {
        if (auto locked_client = it->second.lock(); locked_client)
        {
            locked_client->clearCache();
            ++it;
        }
        else
        {
            LOG_INFO(&Poco::Logger::get("S3ClientCacheRegistry"), "Deleting leftover S3 client cache");
            it = client_caches.erase(it);
        }
    }

}

}

}

#endif
