#pragma once

#include <aws/core/http/HttpResponse.h>
#include "config.h"

#if USE_AWS_S3

#include <Common/logger_useful.h>

#include <IO/S3/URI.h>
#include <IO/S3/Requests.h>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/core/client/AWSErrorMarshaller.h>

namespace DB
{

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
        endpoint_provider->GetBuiltInParameters().GetParameter("Region").GetString(explicit_region);
        std::string endpoint;
        endpoint_provider->GetBuiltInParameters().GetParameter("Endpoint").GetString(endpoint);
        detect_region = explicit_region == Aws::Region::AWS_GLOBAL && endpoint.find(".amazonaws.com") != std::string::npos;
    }

    S3Client(const S3Client & other)
        : Aws::S3::S3Client(other)
        , explicit_region(other.explicit_region)
        , detect_region(other.detect_region)
        , region_for_bucket_cache(other.region_for_bucket_cache)
        , uri_for_bucket_cache(other.uri_for_bucket_cache)
        , log(&Poco::Logger::get("S3Client"))
    {
    }

    class RetryStrategy : public Aws::Client::DefaultRetryStrategy
    {
    public:
        using Aws::Client::DefaultRetryStrategy::DefaultRetryStrategy;

        bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;
    };


    Model::HeadObjectOutcome HeadObject(const HeadObjectRequest & request) const;
    Model::ListObjectsV2Outcome ListObjectsV2(const ListObjectsV2Request & request) const;
    Model::ListObjectsOutcome ListObjects(const ListObjectsRequest & request) const;
    Model::GetObjectOutcome GetObject(const GetObjectRequest & request) const;

    Model::AbortMultipartUploadOutcome AbortMultipartUpload(const AbortMultipartUploadRequest & request) const;
    Model::CreateMultipartUploadOutcome CreateMultipartUpload(const CreateMultipartUploadRequest & request) const;
    Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(const CompleteMultipartUploadRequest & request) const;
    Model::UploadPartOutcome UploadPart(const UploadPartRequest & request) const;
    Model::UploadPartCopyOutcome UploadPartCopy(const UploadPartCopyRequest & request) const;

    Model::CopyObjectOutcome CopyObject(const CopyObjectRequest & request) const;
    Model::PutObjectOutcome PutObject(const PutObjectRequest & request) const;
    Model::DeleteObjectOutcome DeleteObject(const DeleteObjectRequest & request) const;
    Model::DeleteObjectsOutcome DeleteObjects(const DeleteObjectsRequest & request) const;

private:
    /// Make regular functions private
    using Aws::S3::S3Client::HeadObject;
    using Aws::S3::S3Client::ListObjectsV2;
    using Aws::S3::S3Client::ListObjects;
    using Aws::S3::S3Client::GetObject;

    using Aws::S3::S3Client::AbortMultipartUpload;
    using Aws::S3::S3Client::CreateMultipartUpload;
    using Aws::S3::S3Client::CompleteMultipartUpload;
    using Aws::S3::S3Client::UploadPart;
    using Aws::S3::S3Client::UploadPartCopy;

    using Aws::S3::S3Client::CopyObject;
    using Aws::S3::S3Client::PutObject;
    using Aws::S3::S3Client::DeleteObject;
    using Aws::S3::S3Client::DeleteObjects;

    template <typename RequestType, typename RequestFn>
    std::invoke_result_t<RequestFn, RequestType>
    doRequest(const RequestType & request, RequestFn request_fn) const
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

        bool found_new_endpoint = false;
        SCOPE_EXIT({
            // if we found correct endpoint after 301 responses, update the cache for future requests
            if (!found_new_endpoint)
                return;

            auto uri_override = request.getURIOverride();
            assert(uri_override.has_value());
            updateURIForBucket(bucket, std::move(*uri_override));
        });

        while (true)
        {
            auto result = request_fn(request);
            if (result.IsSuccess())
                return result;

            auto error = result.GetError();

            std::string new_region;
            if (checkIfWrongRegionDefined(bucket, error, new_region))
            {
                request.overrideRegion(new_region);
                continue;
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

            // we possibly got new location, need to try with that one
            auto new_uri = getURIFromError(error);
            if (!new_uri)
                return result;

            const auto & current_uri_override = request.getURIOverride();
            /// we already tried with this URI
            if (current_uri_override && current_uri_override->uri == new_uri->uri)
            {
                LOG_INFO(log, "Getting redirected to the same invalid location {}", new_uri->uri.toString());
                return result;
            }

            found_new_endpoint = true;
            request.overrideURI(*new_uri);
        }
    }

    void updateURIForBucket(const std::string & bucket, S3::URI new_uri) const;
    std::optional<S3::URI> getURIFromError(const Aws::S3::S3Error & error) const;
    bool updateURIForBucketForHead(const std::string & bucket) const;

    std::string getRegionForBucket(const std::string & bucket, bool force_detect = false) const;
    std::optional<S3::URI> getURIForBucket(const std::string & bucket) const;

    bool checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const;
    void insertRegionOverride(const std::string & bucket, const std::string & region) const;

    std::string explicit_region;
    mutable bool detect_region = true;
    mutable std::mutex region_cache_mutex;
    mutable std::unordered_map<std::string, std::string> region_for_bucket_cache;

    mutable std::mutex uri_cache_mutex;
    mutable std::unordered_map<std::string, URI> uri_for_bucket_cache;

    Poco::Logger * log;
};

}

}

#endif
