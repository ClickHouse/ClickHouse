#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <IO/S3/URI.h>
#include <IO/S3/Requests.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/Credentials.h>

#include <aws/core/Aws.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/core/client/AWSErrorMarshaller.h>
#include <aws/core/client/RetryStrategy.h>

namespace DB::S3
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
///
/// To add support for new type of request
/// - ExtendedRequest should be defined inside IO/S3/Requests.h
/// - new method accepting that request should be defined in this Client (check other requests for reference)
/// - method handling the request from Aws::S3::S3Client should be left to private so we don't use it by accident
class Client : private Aws::S3::S3Client
{
public:
    /// we use a factory method to verify arguments before creating a client because
    /// there are certain requirements on arguments for it to work correctly
    /// e.g. Client::RetryStrategy should be used
    static std::unique_ptr<Client> create(
            size_t max_redirects_,
            const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider,
            const Aws::Client::ClientConfiguration & client_configuration,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
            bool use_virtual_addressing);

    static std::unique_ptr<Client> create(const Client & other);

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

    /// Returns the initial endpoint.
    const String & getInitialEndpoint() const { return initial_endpoint; }

    /// Decorator for RetryStrategy needed for this client to work correctly.
    /// We want to manually handle permanent moves (status code 301) because:
    /// - redirect location is written in XML format inside the response body something that doesn't exist for HEAD
    ///   requests so we need to manually find the correct location
    /// - we want to cache the new location to decrease number of roundtrips for future requests
    /// This decorator doesn't retry if 301 is detected and fallbacks to the inner retry strategy otherwise.
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

    using Aws::S3::S3Client::EnableRequestProcessing;
    using Aws::S3::S3Client::DisableRequestProcessing;
private:
    Client(size_t max_redirects_,
           const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentials_provider,
           const Aws::Client::ClientConfiguration& client_configuration,
           Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
           bool use_virtual_addressing);

    Client(const Client & other);

    /// Leave regular functions private so we don't accidentally use them
    /// otherwise region and endpoint redirection won't work
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
    doRequest(const RequestType & request, RequestFn request_fn) const;

    void updateURIForBucket(const std::string & bucket, S3::URI new_uri) const;
    std::optional<S3::URI> getURIFromError(const Aws::S3::S3Error & error) const;
    std::optional<Aws::S3::S3Error> updateURIForBucketForHead(const std::string & bucket) const;

    std::string getRegionForBucket(const std::string & bucket, bool force_detect = false) const;
    std::optional<S3::URI> getURIForBucket(const std::string & bucket) const;

    bool checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const;
    void insertRegionOverride(const std::string & bucket, const std::string & region) const;

    String initial_endpoint;

    std::string explicit_region;
    mutable bool detect_region = true;

    mutable std::shared_ptr<ClientCache> cache;

    const size_t max_redirects;

    Poco::Logger * log;
};

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::unique_ptr<S3::Client> create(
        const PocoHTTPClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        const String & server_side_encryption_customer_key_base64,
        HTTPHeaderEntries headers,
        CredentialsConfiguration credentials_configuration);

    PocoHTTPClientConfiguration createClientConfiguration(
        const String & force_region,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects,
        bool enable_s3_requests_logging,
        bool for_disk_s3,
        const ThrottlerPtr & get_request_throttler,
        const ThrottlerPtr & put_request_throttler);

private:
    ClientFactory();

    Aws::SDKOptions aws_options;
    std::atomic<bool> s3_requests_logging_enabled;
};

}

#endif
