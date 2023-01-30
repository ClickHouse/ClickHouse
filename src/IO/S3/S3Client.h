#pragma once

#include "config.h"

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3ServiceClientModel.h>

#include <IO/S3Common.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace S3
{

namespace Model = Aws::S3::Model;

class S3Client : public Aws::S3::S3Client
{
public:
    template <typename... Args>
    explicit S3Client(Args &&... args)
        : Aws::S3::S3Client(std::forward<Args>(args)...)
        , log(&Poco::Logger::get("S3Client"))
    {
        auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(accessEndpointProvider().get());
        std::string region;
        endpoint_provider->GetBuiltInParameters().GetParameter("Region").GetString(region);
        detect_region = region == Aws::Region::AWS_GLOBAL;
    }

    class RetryStrategy : public Aws::Client::DefaultRetryStrategy
    {
    public:
        using Aws::Client::DefaultRetryStrategy::DefaultRetryStrategy;

        bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;
    };


    Model::HeadObjectOutcome HeadObject(const Model::HeadObjectRequest & request) const override;
    Model::ListObjectsV2Outcome ListObjectsV2(const Model::ListObjectsV2Request & request) const override;
    Model::GetObjectOutcome GetObject(const Model::GetObjectRequest & request) const override;

    Model::CreateMultipartUploadOutcome CreateMultipartUpload(const Model::CreateMultipartUploadRequest & request) const override;
    Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(const Model::CompleteMultipartUploadRequest & request) const override;
    Model::PutObjectOutcome PutObject(const Model::PutObjectRequest & request) const override;
    Model::UploadPartOutcome UploadPart(const Model::UploadPartRequest & request) const override;

    Model::DeleteObjectOutcome DeleteObject(const Model::DeleteObjectRequest & request) const override;
    Model::DeleteObjectsOutcome DeleteObjects(const Model::DeleteObjectsRequest & request) const override;

    const std::string * getRegionForBucket(const std::string & bucket) const;
    const URI * getURIForBucket(const std::string & bucket) const;

private:
    template <typename RequestType, typename RequestFn>
    std::invoke_result_t<RequestFn, typename RequestType::BaseRequestType>
    doRequest(const typename RequestType::BaseRequestType & request, RequestFn request_fn) const
    {
        if (const auto * casted_request = dynamic_cast<const RequestType *>(&request); casted_request)
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

        std::optional<S3::URI> last_uri;
        while (true)
        {
            auto result = request_fn(request);
            if (result.IsSuccess())
                return result;

            auto error = result.GetError();

            if (error.GetResponseCode() != Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
                return result;

            std::unique_lock lock(uri_cache_mutex);
            if (!updateEndpointForBucketFromErrorAssumeLocked(bucket, result.GetError()))
                return result;

            const auto * cached_uri_ptr = getURIForBucketAssumeLocked(bucket);

            if (!cached_uri_ptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "URI for bucket {} should be present in cache", bucket);

            auto cached_uri = *cached_uri_ptr;

            lock.unlock();

            // check if we already tried with this URI
            if (last_uri && last_uri->uri == cached_uri.uri)
                return result;

            last_uri = std::move(cached_uri);
        }
    }

    void updateRegionForBucketAssumeLocked(const std::string & bucket) const;
    void updateEndpointForBucketForHead(const std::string & bucket) const;
    bool updateEndpointForBucketFromErrorAssumeLocked(const std::string & bucket, const Aws::S3::S3Error & error) const;

    const std::string * getRegionForBucketAssumeLocked(const std::string & bucket) const;
    const URI * getURIForBucketAssumeLocked(const std::string & bucket) const;

    bool detect_region = true;
    mutable std::mutex region_cache_mutex;
    mutable std::unordered_map<std::string, std::string> region_for_bucket_cache;

    mutable std::mutex uri_cache_mutex;
    mutable std::unordered_map<std::string, URI> uri_for_bucket_cache;

    Poco::Logger * log;
};

}

}

#endif
