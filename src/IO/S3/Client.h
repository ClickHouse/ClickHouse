#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <IO/S3/URI.h>
#include <IO/S3/Requests.h>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/core/client/AWSErrorMarshaller.h>
#include <aws/core/client/RetryStrategy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_REDIRECTS;
}

namespace S3
{

namespace Model = Aws::S3::Model;

struct ClientCache
{
    ClientCache() = default;

    ClientCache(const ClientCache & other)
        : region_for_bucket_cache(other.region_for_bucket_cache)
        , uri_for_bucket_cache(other.uri_for_bucket_cache)
    {}

    ClientCache(ClientCache && other) = delete;

    ClientCache & operator=(const ClientCache &) = delete;
    ClientCache & operator=(ClientCache &&) = delete;

    void clearCache();

    std::mutex region_cache_mutex;
    std::unordered_map<std::string, std::string> region_for_bucket_cache;

    std::mutex uri_cache_mutex;
    std::unordered_map<std::string, URI> uri_for_bucket_cache;
};

class ClientCacheRegistry
{
public:
    static ClientCacheRegistry & instance()
    {
        static ClientCacheRegistry registry;
        return registry;
    }

    void registerClient(const std::shared_ptr<ClientCache> & client_cache);
    void unregisterClient(ClientCache * client);
    void clearCacheForAll();
private:
    ClientCacheRegistry() = default;

    std::mutex clients_mutex;
    std::unordered_map<ClientCache *, std::weak_ptr<ClientCache>> client_caches;
};

/// Client that improves the client from the AWS SDK
/// - inject region and URI into requests so they are rerouted to the correct destination if needed
/// - automatically detect endpoint and regions for each bucket and cache them
///
/// For this client to work correctly both Client::RetryStrategy and Requests defined in <IO/S3/Requests.h> should be used.
class Client : public Aws::S3::S3Client
{
public:
    template <typename... Args>
    static std::unique_ptr<Client> create(Args &&... args)
    {
        (verifyArgument(args), ...);
        return std::unique_ptr<Client>(new Client(std::forward<Args>(args)...));
    }

    Client & operator=(const Client &) = delete;

    Client(Client && other) = delete;
    Client & operator=(Client &&) = delete;

    ~Client() override
    {
        try
        {
            ClientCacheRegistry::instance().unregisterClient(cache.get());
        }
        catch (...)
        {
            tryLogCurrentException(log);
            throw;
        }
    }

    /// Decorator for RetryStrategy needed for this client to work correctly
    class RetryStrategy : public Aws::Client::RetryStrategy
    {
    public:
        explicit RetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> wrapped_strategy_);

        /// NOLINTNEXTLINE(google-runtime-int)
        bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

        /// NOLINTNEXTLINE(google-runtime-int)
        long CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

        /// NOLINTNEXTLINE(google-runtime-int)
        long GetMaxAttempts() const override;

        void GetSendToken() override;

        bool HasSendToken() override;

        void RequestBookkeeping(const Aws::Client::HttpResponseOutcome& httpResponseOutcome) override;
        void RequestBookkeeping(const Aws::Client::HttpResponseOutcome& httpResponseOutcome, const Aws::Client::AWSError<Aws::Client::CoreErrors>& lastError) override;
    private:
        std::shared_ptr<Aws::Client::RetryStrategy> wrapped_strategy;
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
    template <typename... Args>
    explicit Client(size_t max_redirects_, Args &&... args)
        : Aws::S3::S3Client(std::forward<Args>(args)...)
        , max_redirects(max_redirects_)
        , log(&Poco::Logger::get("S3Client"))
    {
        auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(accessEndpointProvider().get());
        endpoint_provider->GetBuiltInParameters().GetParameter("Region").GetString(explicit_region);
        std::string endpoint;
        endpoint_provider->GetBuiltInParameters().GetParameter("Endpoint").GetString(endpoint);
        detect_region = explicit_region == Aws::Region::AWS_GLOBAL && endpoint.find(".amazonaws.com") != std::string::npos;

        cache = std::make_shared<ClientCache>();
        ClientCacheRegistry::instance().registerClient(cache);
    }

    Client(const Client & other)
        : Aws::S3::S3Client(other)
        , explicit_region(other.explicit_region)
        , detect_region(other.detect_region)
        , max_redirects(other.max_redirects)
        , log(&Poco::Logger::get("S3Client"))
    {
        cache = std::make_shared<ClientCache>(*other.cache);
        ClientCacheRegistry::instance().registerClient(cache);
    }

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
        // if we found correct endpoint after 301 responses, update the cache for future requests
        SCOPE_EXIT(
            if (found_new_endpoint)
            {
                auto uri_override = request.getURIOverride();
                assert(uri_override.has_value());
                updateURIForBucket(bucket, std::move(*uri_override));
            }
        );

        for (size_t attempt = 0; attempt <= max_redirects; ++attempt)
        {
            auto result = request_fn(request);
            if (result.IsSuccess())
                return result;

            const auto & error = result.GetError();

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

        throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects");
    }

    void updateURIForBucket(const std::string & bucket, S3::URI new_uri) const;
    std::optional<S3::URI> getURIFromError(const Aws::S3::S3Error & error) const;
    std::optional<Aws::S3::S3Error> updateURIForBucketForHead(const std::string & bucket) const;

    std::string getRegionForBucket(const std::string & bucket, bool force_detect = false) const;
    std::optional<S3::URI> getURIForBucket(const std::string & bucket) const;

    bool checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const;
    void insertRegionOverride(const std::string & bucket, const std::string & region) const;

    template <typename T>
    static void verifyArgument(const T & /*arg*/)
    {}

    template <std::derived_from<Aws::Client::ClientConfiguration> T>
    static void verifyArgument(const T & client_config)
    {
        if (!client_config.retryStrategy)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The S3 client can only be used with Client::RetryStrategy, define it in the client configuration");

        assert_cast<const RetryStrategy &>(*client_config.retryStrategy);
    }

    std::string explicit_region;
    mutable bool detect_region = true;

    mutable std::shared_ptr<ClientCache> cache;

    const size_t max_redirects;

    Poco::Logger * log;
};

}

}

#endif
