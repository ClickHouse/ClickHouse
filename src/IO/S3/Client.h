#pragma once

#include <optional>
#include <base/types.h>

#include "config.h"

namespace DB::S3
{

/// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html
/// Needed by S3Common.h even if USE_AWS_S3 is 0
struct ServerSideEncryptionKMSConfig
{
    // If key_id is non-null, enable SSE-KMS. If key_id is "", use the AWS managed key
    std::optional<String> key_id = std::nullopt;
    std::optional<String> encryption_context = std::nullopt;
    std::optional<bool> bucket_key_enabled = std::nullopt;

    bool operator==(const ServerSideEncryptionKMSConfig & other) const = default;
};

}

#if USE_AWS_S3

#include <Common/assert_cast.h>
#include <base/scope_guard.h>

#include <IO/S3/URI.h>
#include <IO/S3/Requests.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/ProviderType.h>

#include <aws/core/Aws.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/core/client/AWSErrorMarshaller.h>
#include <aws/core/client/RetryStrategy.h>

namespace MockS3
{
    struct Client;
}

namespace DB::S3
{

namespace Model = Aws::S3::Model;

struct ClientCache
{
    ClientCache() = default;

    ClientCache(const ClientCache & other);

    ClientCache(ClientCache && other) = delete;

    ClientCache & operator=(const ClientCache &) = delete;
    ClientCache & operator=(ClientCache &&) = delete;

    void clearCache();

    mutable std::mutex region_cache_mutex;
    std::unordered_map<std::string, std::string> region_for_bucket_cache TSA_GUARDED_BY(region_cache_mutex);

    mutable std::mutex uri_cache_mutex;
    std::unordered_map<std::string, URI> uri_for_bucket_cache TSA_GUARDED_BY(uri_cache_mutex);
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
    std::unordered_map<ClientCache *, std::weak_ptr<ClientCache>> client_caches TSA_GUARDED_BY(clients_mutex);
};

bool isS3ExpressEndpoint(const std::string & endpoint);

struct ClientSettings
{
    bool use_virtual_addressing = false;
    /// Disable checksum to avoid extra read of the input stream
    bool disable_checksum = false;
    /// Should client send ComposeObject request after upload to GCS.
    ///
    /// Previously ComposeObject request was required to make Copy possible,
    /// but not anymore (see [1]).
    ///
    ///   [1]: https://cloud.google.com/storage/docs/release-notes#June_23_2023
    ///
    /// Ability to enable it preserved since likely it is required for old
    /// files.
    bool gcs_issue_compose_request = false;
    bool is_s3express_bucket = false;
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
    class RetryStrategy;

    /// we use a factory method to verify arguments before creating a client because
    /// there are certain requirements on arguments for it to work correctly
    /// e.g. Client::RetryStrategy should be used
    static std::unique_ptr<Client> create(
            size_t max_redirects_,
            ServerSideEncryptionKMSConfig sse_kms_config_,
            const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider,
            const PocoHTTPClientConfiguration & client_configuration,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
            const ClientSettings & client_settings);

    std::unique_ptr<Client> clone() const;

    Client & operator=(const Client &) = delete;

    Client(Client && other) = delete;
    Client & operator=(Client &&) = delete;

    ~Client() override;

    /// Returns the initial endpoint.
    const String & getInitialEndpoint() const { return initial_endpoint; }
    const String & getRegion() const { return explicit_region; }

    Aws::Auth::AWSCredentials getCredentials() const;

    /// We want to manually handle permanent moves (status code 301) because:
    /// - redirect location is written in XML format inside the response body something that doesn't exist for HEAD
    ///   requests so we need to manually find the correct location
    /// - we want to cache the new location to decrease number of roundtrips for future requests
    /// Other retries are processed with exponential backoff timeout
    /// which is limited and rundomly spread
    class RetryStrategy : public Aws::Client::RetryStrategy
    {
    public:
        explicit RetryStrategy(uint32_t maxRetries_ = 10, uint32_t scaleFactor_ = 25, uint32_t maxDelayMs_ = 5000);

        /// NOLINTNEXTLINE(google-runtime-int)
        bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

        /// NOLINTNEXTLINE(google-runtime-int)
        long CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override;

        /// NOLINTNEXTLINE(google-runtime-int)
        long GetMaxAttempts() const override;

    private:
        uint32_t maxRetries;
        uint32_t scaleFactor;
        uint32_t maxDelayMs;
    };

    /// SSE-KMS headers MUST be signed, so they need to be added before the SDK signs the message
    /// (before sending the request with one of the methods below).
    /// Per the docs (https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html),
    /// the headers should only be set for PutObject, CopyObject, POST Object, and CreateMultipartUpload.
    template <typename RequestType>
    void setKMSHeaders(RequestType & request) const;

    Model::HeadObjectOutcome HeadObject(HeadObjectRequest & request) const;
    Model::ListObjectsV2Outcome ListObjectsV2(ListObjectsV2Request & request) const;
    Model::ListObjectsOutcome ListObjects(ListObjectsRequest & request) const;
    Model::GetObjectOutcome GetObject(GetObjectRequest & request) const;

    Model::AbortMultipartUploadOutcome AbortMultipartUpload(AbortMultipartUploadRequest & request) const;
    Model::CreateMultipartUploadOutcome CreateMultipartUpload(CreateMultipartUploadRequest & request) const;
    Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(CompleteMultipartUploadRequest & request) const;
    Model::UploadPartOutcome UploadPart(UploadPartRequest & request) const;
    Model::UploadPartCopyOutcome UploadPartCopy(UploadPartCopyRequest & request) const;

    Model::CopyObjectOutcome CopyObject(CopyObjectRequest & request) const;
    Model::PutObjectOutcome PutObject(PutObjectRequest & request) const;
    Model::DeleteObjectOutcome DeleteObject(DeleteObjectRequest & request) const;
    Model::DeleteObjectsOutcome DeleteObjects(DeleteObjectsRequest & request) const;

    using ComposeObjectOutcome = Aws::Utils::Outcome<Aws::NoResult, Aws::S3::S3Error>;

    using Aws::S3::S3Client::EnableRequestProcessing;
    using Aws::S3::S3Client::DisableRequestProcessing;

    void BuildHttpRequest(const Aws::AmazonWebServiceRequest& request,
                          const std::shared_ptr<Aws::Http::HttpRequest>& httpRequest) const override;

    bool supportsMultiPartCopy() const;

    bool isS3ExpressBucket() const { return client_settings.is_s3express_bucket; }

    bool isClientForDisk() const
    {
        return client_configuration.for_disk_s3;
    }

    ThrottlerPtr getPutRequestThrottler() const { return client_configuration.put_request_throttler; }
    ThrottlerPtr getGetRequestThrottler() const { return client_configuration.get_request_throttler; }

private:
    friend struct ::MockS3::Client;

    Client(size_t max_redirects_,
           ServerSideEncryptionKMSConfig sse_kms_config_,
           const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider_,
           const PocoHTTPClientConfiguration & client_configuration,
           Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
           const ClientSettings & client_settings_);

    Client(
        const Client & other, const PocoHTTPClientConfiguration & client_configuration);

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

    ComposeObjectOutcome ComposeObject(ComposeObjectRequest & request) const;

    template <typename RequestType, typename RequestFn>
    std::invoke_result_t<RequestFn, RequestType>
    doRequest(RequestType & request, RequestFn request_fn) const;

    template <bool IsReadMethod, typename RequestType, typename RequestFn>
    std::invoke_result_t<RequestFn, RequestType>
    doRequestWithRetryNetworkErrors(RequestType & request, RequestFn request_fn) const;

    void updateURIForBucket(const std::string & bucket, S3::URI new_uri) const;
    std::optional<S3::URI> getURIFromError(const Aws::S3::S3Error & error) const;
    std::optional<Aws::S3::S3Error> updateURIForBucketForHead(const std::string & bucket) const;

    std::string getRegionForBucket(const std::string & bucket, bool force_detect = false) const;
    std::optional<S3::URI> getURIForBucket(const std::string & bucket) const;

    bool checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const;
    void insertRegionOverride(const std::string & bucket, const std::string & region) const;

    template <typename RequestResult>
    RequestResult processRequestResult(RequestResult && outcome) const;

    String initial_endpoint;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    PocoHTTPClientConfiguration client_configuration;
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads;
    ClientSettings client_settings;

    std::string explicit_region;
    mutable bool detect_region = true;

    /// provider type can determine if some functionality is supported
    /// but for same provider, we would need to generate different headers depending on the
    /// mode
    /// E.g. GCS can work in AWS mode in some cases and accept headers with x-amz prefix
    ProviderType provider_type{ProviderType::UNKNOWN};
    ApiMode api_mode{ApiMode::AWS};

    mutable std::shared_ptr<ClientCache> cache;

    const size_t max_redirects;

    const ServerSideEncryptionKMSConfig sse_kms_config;

    LoggerPtr log;
};

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::unique_ptr<S3::Client> create(
        const PocoHTTPClientConfiguration & cfg,
        ClientSettings client_settings,
        const String & access_key_id,
        const String & secret_access_key,
        const String & server_side_encryption_customer_key_base64,
        ServerSideEncryptionKMSConfig sse_kms_config,
        HTTPHeaderEntries headers,
        CredentialsConfiguration credentials_configuration,
        const String & session_token = "");

    PocoHTTPClientConfiguration createClientConfiguration(
        const String & force_region,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects,
        unsigned int s3_retry_attempts,
        bool enable_s3_requests_logging,
        bool for_disk_s3,
        const ThrottlerPtr & get_request_throttler,
        const ThrottlerPtr & put_request_throttler,
        const String & protocol = "https");

private:
    ClientFactory();

    Aws::SDKOptions aws_options;
    std::atomic<bool> s3_requests_logging_enabled;
};

}

#endif
