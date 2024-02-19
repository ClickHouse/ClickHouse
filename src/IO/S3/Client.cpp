#include <IO/S3/Client.h>

#if USE_AWS_S3

#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/core/client/AWSErrorMarshaller.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/ErrorMacros.h>

#include <Poco/Net/NetException.h>

#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/S3/AWSLogger.h>
#include <IO/S3/Credentials.h>

#include <Common/assert_cast.h>

#include <Common/logger_useful.h>
#include <Common/ProxyConfigurationResolverProvider.h>


namespace ProfileEvents
{
    extern const Event S3WriteRequestsErrors;
    extern const Event S3ReadRequestsErrors;

    extern const Event DiskS3WriteRequestsErrors;
    extern const Event DiskS3ReadRequestsErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_REDIRECTS;
}

namespace S3
{

Client::RetryStrategy::RetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> wrapped_strategy_)
    : wrapped_strategy(std::move(wrapped_strategy_))
{
    if (!wrapped_strategy)
        wrapped_strategy = Aws::Client::InitRetryStrategy();
}

/// NOLINTNEXTLINE(google-runtime-int)
bool Client::RetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const
{
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
        return false;

    return wrapped_strategy->ShouldRetry(error, attemptedRetries);
}

/// NOLINTNEXTLINE(google-runtime-int)
long Client::RetryStrategy::CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const
{
    return wrapped_strategy->CalculateDelayBeforeNextRetry(error, attemptedRetries);
}

/// NOLINTNEXTLINE(google-runtime-int)
long Client::RetryStrategy::GetMaxAttempts() const
{
    return wrapped_strategy->GetMaxAttempts();
}

void Client::RetryStrategy::GetSendToken()
{
    return wrapped_strategy->GetSendToken();
}

bool Client::RetryStrategy::HasSendToken()
{
    return wrapped_strategy->HasSendToken();
}

void Client::RetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome& httpResponseOutcome)
{
    return wrapped_strategy->RequestBookkeeping(httpResponseOutcome);
}

void Client::RetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome& httpResponseOutcome, const Aws::Client::AWSError<Aws::Client::CoreErrors>& lastError)
{
    return wrapped_strategy->RequestBookkeeping(httpResponseOutcome, lastError);
}

namespace
{

void verifyClientConfiguration(const Aws::Client::ClientConfiguration & client_config)
{
    if (!client_config.retryStrategy)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The S3 client can only be used with Client::RetryStrategy, define it in the client configuration");

    assert_cast<const Client::RetryStrategy &>(*client_config.retryStrategy);
}

}

std::unique_ptr<Client> Client::create(
    size_t max_redirects_,
    ServerSideEncryptionKMSConfig sse_kms_config_,
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider,
    const PocoHTTPClientConfiguration & client_configuration,
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
    bool use_virtual_addressing)
{
    verifyClientConfiguration(client_configuration);
    return std::unique_ptr<Client>(
        new Client(max_redirects_, std::move(sse_kms_config_), credentials_provider, client_configuration, sign_payloads, use_virtual_addressing));
}

std::unique_ptr<Client> Client::clone(
    std::optional<std::shared_ptr<RetryStrategy>> override_retry_strategy,
    std::optional<Int64> override_request_timeout_ms) const
{
    PocoHTTPClientConfiguration new_configuration = client_configuration;
    if (override_retry_strategy.has_value())
        new_configuration.retryStrategy = *override_retry_strategy;
    if (override_request_timeout_ms.has_value())
        new_configuration.requestTimeoutMs = *override_request_timeout_ms;
    return std::unique_ptr<Client>(new Client(*this, new_configuration));
}

namespace
{

ProviderType deduceProviderType(const std::string & url)
{
    if (url.find(".amazonaws.com") != std::string::npos)
        return ProviderType::AWS;

    if (url.find("storage.googleapis.com") != std::string::npos)
        return ProviderType::GCS;

    return ProviderType::UNKNOWN;
}

}

Client::Client(
    size_t max_redirects_,
    ServerSideEncryptionKMSConfig sse_kms_config_,
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider_,
    const PocoHTTPClientConfiguration & client_configuration_,
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads_,
    bool use_virtual_addressing_)
    : Aws::S3::S3Client(credentials_provider_, client_configuration_, sign_payloads_, use_virtual_addressing_)
    , credentials_provider(credentials_provider_)
    , client_configuration(client_configuration_)
    , sign_payloads(sign_payloads_)
    , use_virtual_addressing(use_virtual_addressing_)
    , max_redirects(max_redirects_)
    , sse_kms_config(std::move(sse_kms_config_))
    , log(&Poco::Logger::get("S3Client"))
{
    auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(accessEndpointProvider().get());
    endpoint_provider->GetBuiltInParameters().GetParameter("Region").GetString(explicit_region);
    endpoint_provider->GetBuiltInParameters().GetParameter("Endpoint").GetString(initial_endpoint);

    provider_type = deduceProviderType(initial_endpoint);
    LOG_TRACE(log, "Provider type: {}", toString(provider_type));

    if (provider_type == ProviderType::GCS)
    {
        /// GCS can operate in 2 modes for header and query params names:
        /// - with both x-amz and x-goog prefixes allowed (but cannot mix different prefixes in same request)
        /// - only with x-goog prefix
        /// first mode is allowed only with HMAC (or unsigned requests) so when we
        /// find credential keys we can simply behave as the underlying storage is S3
        /// otherwise, we need to be aware we are making requests to GCS
        /// and replace all headers with a valid prefix when needed
        if (credentials_provider)
        {
            auto credentials = credentials_provider->GetAWSCredentials();
            if (credentials.IsEmpty())
                api_mode = ApiMode::GCS;
        }
    }

    LOG_TRACE(log, "API mode of the S3 client: {}", api_mode);

    detect_region = provider_type == ProviderType::AWS && explicit_region == Aws::Region::AWS_GLOBAL;

    cache = std::make_shared<ClientCache>();
    ClientCacheRegistry::instance().registerClient(cache);
}

Client::Client(
    const Client & other, const PocoHTTPClientConfiguration & client_configuration_)
    : Aws::S3::S3Client(other.credentials_provider, client_configuration_, other.sign_payloads,
                        other.use_virtual_addressing)
    , initial_endpoint(other.initial_endpoint)
    , credentials_provider(other.credentials_provider)
    , client_configuration(client_configuration_)
    , sign_payloads(other.sign_payloads)
    , use_virtual_addressing(other.use_virtual_addressing)
    , explicit_region(other.explicit_region)
    , detect_region(other.detect_region)
    , provider_type(other.provider_type)
    , max_redirects(other.max_redirects)
    , sse_kms_config(other.sse_kms_config)
    , log(&Poco::Logger::get("S3Client"))
{
    cache = std::make_shared<ClientCache>(*other.cache);
    ClientCacheRegistry::instance().registerClient(cache);
}

Aws::Auth::AWSCredentials Client::getCredentials() const
{
    return credentials_provider->GetAWSCredentials();
}

bool Client::checkIfWrongRegionDefined(const std::string & bucket, const Aws::S3::S3Error & error, std::string & region) const
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

void Client::insertRegionOverride(const std::string & bucket, const std::string & region) const
{
    std::lock_guard lock(cache->region_cache_mutex);
    auto [it, inserted] = cache->region_for_bucket_cache.emplace(bucket, region);
    if (inserted)
        LOG_INFO(log, "Detected different region ('{}') for bucket {} than the one defined ('{}')", region, bucket, explicit_region);
}

template <typename RequestType>
void Client::setKMSHeaders(RequestType & request) const
{
    // Don't do anything unless a key ID was specified
    if (sse_kms_config.key_id)
    {
        request.SetServerSideEncryption(Model::ServerSideEncryption::aws_kms);
        // If the key ID was specified but is empty, treat it as using the AWS managed key and omit the header
        if (!sse_kms_config.key_id->empty())
            request.SetSSEKMSKeyId(*sse_kms_config.key_id);
        if (sse_kms_config.encryption_context)
            request.SetSSEKMSEncryptionContext(*sse_kms_config.encryption_context);
        if (sse_kms_config.bucket_key_enabled)
            request.SetBucketKeyEnabled(*sse_kms_config.bucket_key_enabled);
    }
}

// Explicitly instantiate this method only for the request types that support KMS headers
template void Client::setKMSHeaders<CreateMultipartUploadRequest>(CreateMultipartUploadRequest & request) const;
template void Client::setKMSHeaders<CopyObjectRequest>(CopyObjectRequest & request) const;
template void Client::setKMSHeaders<PutObjectRequest>(PutObjectRequest & request) const;

Model::HeadObjectOutcome Client::HeadObject(const HeadObjectRequest & request) const
{
    const auto & bucket = request.GetBucket();

    request.setApiMode(api_mode);

    if (auto region = getRegionForBucket(bucket); !region.empty())
    {
        if (!detect_region)
            LOG_INFO(log, "Using region override {} for bucket {}", region, bucket);
        request.overrideRegion(std::move(region));
    }

    if (auto uri = getURIForBucket(bucket); uri.has_value())
        request.overrideURI(std::move(*uri));

    auto result = HeadObject(static_cast<const Model::HeadObjectRequest&>(request));
    if (result.IsSuccess())
        return result;

    const auto & error = result.GetError();

    std::string new_region;
    if (checkIfWrongRegionDefined(bucket, error, new_region))
    {
        request.overrideRegion(new_region);
        return Aws::S3::S3Client::HeadObject(request);
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
        if (auto maybe_error = updateURIForBucketForHead(bucket); maybe_error.has_value())
            return *maybe_error;

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

    /// The next call is NOT a recurcive call
    /// This is a virtuall call Aws::S3::S3Client::HeadObject(const Model::HeadObjectRequest&)
    return HeadObject(static_cast<const Model::HeadObjectRequest&>(request));
}

/// For each request, we wrap the request functions from Aws::S3::Client with doRequest
/// doRequest calls virtuall function from Aws::S3::Client while DB::S3::Client has not virtual calls for each request type

Model::ListObjectsV2Outcome Client::ListObjectsV2(const ListObjectsV2Request & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ true>(
        request, [this](const Model::ListObjectsV2Request & req) { return ListObjectsV2(req); });
}

Model::ListObjectsOutcome Client::ListObjects(const ListObjectsRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ true>(
        request, [this](const Model::ListObjectsRequest & req) { return ListObjects(req); });
}

Model::GetObjectOutcome Client::GetObject(const GetObjectRequest & request) const
{
    return doRequest(request, [this](const Model::GetObjectRequest & req) { return GetObject(req); });
}

Model::AbortMultipartUploadOutcome Client::AbortMultipartUpload(const AbortMultipartUploadRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::AbortMultipartUploadRequest & req) { return AbortMultipartUpload(req); });
}

Model::CreateMultipartUploadOutcome Client::CreateMultipartUpload(const CreateMultipartUploadRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CreateMultipartUploadRequest & req) { return CreateMultipartUpload(req); });
}

Model::CompleteMultipartUploadOutcome Client::CompleteMultipartUpload(const CompleteMultipartUploadRequest & request) const
{
    auto outcome = doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CompleteMultipartUploadRequest & req) { return CompleteMultipartUpload(req); });

    if (!outcome.IsSuccess() || provider_type != ProviderType::GCS)
        return outcome;

    const auto & key = request.GetKey();
    const auto & bucket = request.GetBucket();

    /// For GCS we will try to compose object at the end, otherwise we cannot do a native copy
    /// for the object (e.g. for backups)
    /// We don't care if the compose fails, because the upload was still successful, only the
    /// performance for copying the object will be affected
    S3::ComposeObjectRequest compose_req;
    compose_req.SetBucket(bucket);
    compose_req.SetKey(key);
    compose_req.SetComponentNames({key});
    compose_req.SetContentType("binary/octet-stream");
    auto compose_outcome = ComposeObject(compose_req);

    if (compose_outcome.IsSuccess())
        LOG_TRACE(log, "Composing object was successful");
    else
        LOG_INFO(log, "Failed to compose object. Message: {}, Key: {}, Bucket: {}", compose_outcome.GetError().GetMessage(), key, bucket);

    return outcome;
}

Model::CopyObjectOutcome Client::CopyObject(const CopyObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CopyObjectRequest & req) { return CopyObject(req); });
}

Model::PutObjectOutcome Client::PutObject(const PutObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::PutObjectRequest & req) { return PutObject(req); });
}

Model::UploadPartOutcome Client::UploadPart(const UploadPartRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::UploadPartRequest & req) { return UploadPart(req); });
}

Model::UploadPartCopyOutcome Client::UploadPartCopy(const UploadPartCopyRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::UploadPartCopyRequest & req) { return UploadPartCopy(req); });
}

Model::DeleteObjectOutcome Client::DeleteObject(const DeleteObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::DeleteObjectRequest & req) { return DeleteObject(req); });
}

Model::DeleteObjectsOutcome Client::DeleteObjects(const DeleteObjectsRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::DeleteObjectsRequest & req) { return DeleteObjects(req); });
}

Client::ComposeObjectOutcome Client::ComposeObject(const ComposeObjectRequest & request) const
{
    auto request_fn = [this](const ComposeObjectRequest & req)
    {
        auto & endpoint_provider = const_cast<Client &>(*this).accessEndpointProvider();
        AWS_OPERATION_CHECK_PTR(endpoint_provider, ComposeObject, Aws::Client::CoreErrors, Aws::Client::CoreErrors::ENDPOINT_RESOLUTION_FAILURE);

        if (!req.BucketHasBeenSet())
        {
            AWS_LOGSTREAM_ERROR("ComposeObject", "Required field: Bucket, is not set")
            return ComposeObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::MISSING_PARAMETER, "MISSING_PARAMETER", "Missing required field [Bucket]", false));
        }

        if (!req.KeyHasBeenSet())
        {
            AWS_LOGSTREAM_ERROR("ComposeObject", "Required field: Key, is not set")
            return ComposeObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::MISSING_PARAMETER, "MISSING_PARAMETER", "Missing required field [Key]", false));
        }

        auto endpointResolutionOutcome = endpoint_provider->ResolveEndpoint(req.GetEndpointContextParams());
        AWS_OPERATION_CHECK_SUCCESS(endpointResolutionOutcome, ComposeObject, Aws::Client::CoreErrors, Aws::Client::CoreErrors::ENDPOINT_RESOLUTION_FAILURE, endpointResolutionOutcome.GetError().GetMessage());
        endpointResolutionOutcome.GetResult().AddPathSegments(req.GetKey());
        endpointResolutionOutcome.GetResult().SetQueryString("?compose");
        return ComposeObjectOutcome(MakeRequest(req, endpointResolutionOutcome.GetResult(), Aws::Http::HttpMethod::HTTP_PUT));
    };

    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, request_fn);
}

template <typename RequestType, typename RequestFn>
std::invoke_result_t<RequestFn, RequestType>
Client::doRequest(const RequestType & request, RequestFn request_fn) const
{
    const auto & bucket = request.GetBucket();
    request.setApiMode(api_mode);

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

template <bool IsReadMethod, typename RequestType, typename RequestFn>
std::invoke_result_t<RequestFn, RequestType>
Client::doRequestWithRetryNetworkErrors(const RequestType & request, RequestFn request_fn) const
{
    auto with_retries = [this, request_fn_ = std::move(request_fn)] (const RequestType & request_)
    {
        chassert(client_configuration.retryStrategy);
        const Int64 max_attempts = client_configuration.retryStrategy->GetMaxAttempts();
        std::exception_ptr last_exception = nullptr;
        for (Int64 attempt_no = 0; attempt_no < max_attempts; ++attempt_no)
        {
            try
            {
                /// S3 does retries network errors actually.
                /// But it is matter when errors occur.
                /// This code retries a specific case when
                /// network error happens when XML document is being read from the response body.
                /// Hence, the response body is a stream, network errors are possible at reading.
                /// S3 doesn't retry them.

                /// Not all requests can be retried in that way.
                /// Requests that read out response body to build the result are possible to retry.
                /// Requests that expose the response stream as an answer are not retried with that code. E.g. GetObject.
                return request_fn_(request_);
            }
            catch (Poco::Net::ConnectionResetException &)
            {

                if constexpr (IsReadMethod)
                {
                    if (client_configuration.for_disk_s3)
                        ProfileEvents::increment(ProfileEvents::DiskS3ReadRequestsErrors);
                    else
                        ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors);
                }
                else
                {
                    if (client_configuration.for_disk_s3)
                        ProfileEvents::increment(ProfileEvents::DiskS3WriteRequestsErrors);
                    else
                        ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors);
                }

                tryLogCurrentException(log, "Will retry");
                last_exception = std::current_exception();

                auto error = Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::NETWORK_CONNECTION, /*retry*/ true);
                client_configuration.retryStrategy->CalculateDelayBeforeNextRetry(error, attempt_no);
                continue;
            }
        }

        chassert(last_exception);
        std::rethrow_exception(last_exception);
    };

    return doRequest(request, with_retries);
}

bool Client::supportsMultiPartCopy() const
{
    return provider_type != ProviderType::GCS;
}

void Client::BuildHttpRequest(const Aws::AmazonWebServiceRequest& request,
                      const std::shared_ptr<Aws::Http::HttpRequest>& httpRequest) const
{
    Aws::S3::S3Client::BuildHttpRequest(request, httpRequest);

    if (api_mode == ApiMode::GCS)
    {
        /// some GCS requests don't like S3 specific headers that the client sets
        httpRequest->DeleteHeader("x-amz-api-version");
        httpRequest->DeleteHeader("amz-sdk-invocation-id");
        httpRequest->DeleteHeader("amz-sdk-request");
    }
}

std::string Client::getRegionForBucket(const std::string & bucket, bool force_detect) const
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

std::optional<S3::URI> Client::getURIFromError(const Aws::S3::S3Error & error) const
{
    auto endpoint = GetErrorMarshaller()->ExtractEndpoint(error);
    if (endpoint.empty())
        return std::nullopt;

    auto & s3_client = const_cast<Client &>(*this);
    const auto * endpoint_provider = dynamic_cast<Aws::S3::Endpoint::S3DefaultEpProviderBase *>(s3_client.accessEndpointProvider().get());
    auto resolved_endpoint = endpoint_provider->ResolveEndpoint({});

    if (!resolved_endpoint.IsSuccess())
        return std::nullopt;

    auto uri = resolved_endpoint.GetResult().GetURI();
    uri.SetAuthority(endpoint);

    return S3::URI(uri.GetURIString());
}

// Do a list request because head requests don't have body in response
std::optional<Aws::S3::S3Error> Client::updateURIForBucketForHead(const std::string & bucket) const
{
    ListObjectsV2Request req;
    req.SetBucket(bucket);
    req.SetMaxKeys(1);
    auto result = ListObjectsV2(req);
    if (result.IsSuccess())
        return std::nullopt;
    return result.GetError();
}

std::optional<S3::URI> Client::getURIForBucket(const std::string & bucket) const
{
    std::lock_guard lock(cache->uri_cache_mutex);
    if (auto it = cache->uri_for_bucket_cache.find(bucket); it != cache->uri_for_bucket_cache.end())
        return it->second;

    return std::nullopt;
}

void Client::updateURIForBucket(const std::string & bucket, S3::URI new_uri) const
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


void ClientCache::clearCache()
{
    {
        std::lock_guard lock(region_cache_mutex);
        region_for_bucket_cache.clear();
    }
    {
        std::lock_guard lock(uri_cache_mutex);
        uri_for_bucket_cache.clear();
    }
}

void ClientCacheRegistry::registerClient(const std::shared_ptr<ClientCache> & client_cache)
{
    std::lock_guard lock(clients_mutex);
    auto [it, inserted] = client_caches.emplace(client_cache.get(), client_cache);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Same S3 client registered twice");
}

void ClientCacheRegistry::unregisterClient(ClientCache * client)
{
    std::lock_guard lock(clients_mutex);
    auto erased = client_caches.erase(client);
    if (erased == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unregister S3 client, either it was already unregistered or not registered at all");
}

void ClientCacheRegistry::clearCacheForAll()
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
            LOG_INFO(&Poco::Logger::get("ClientCacheRegistry"), "Deleting leftover S3 client cache");
            it = client_caches.erase(it);
        }
    }

}

ClientFactory::ClientFactory()
{
    aws_options = Aws::SDKOptions{};
    Aws::InitAPI(aws_options);
    Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>(false));
    Aws::Http::SetHttpClientFactory(std::make_shared<PocoHTTPClientFactory>());
}

ClientFactory::~ClientFactory()
{
    Aws::Utils::Logging::ShutdownAWSLogging();
    Aws::ShutdownAPI(aws_options);
}

ClientFactory & ClientFactory::instance()
{
    static ClientFactory ret;
    return ret;
}

std::unique_ptr<S3::Client> ClientFactory::create( // NOLINT
    const PocoHTTPClientConfiguration & cfg_,
    bool is_virtual_hosted_style,
    const String & access_key_id,
    const String & secret_access_key,
    const String & server_side_encryption_customer_key_base64,
    ServerSideEncryptionKMSConfig sse_kms_config,
    HTTPHeaderEntries headers,
    CredentialsConfiguration credentials_configuration,
    const String & session_token)
{
    PocoHTTPClientConfiguration client_configuration = cfg_;
    client_configuration.updateSchemeAndRegion();

    if (!server_side_encryption_customer_key_base64.empty())
    {
        /// See Client::GeneratePresignedUrlWithSSEC().

        headers.push_back({Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
            Aws::S3::Model::ServerSideEncryptionMapper::GetNameForServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256)});

        headers.push_back({Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
            server_side_encryption_customer_key_base64});

        Aws::Utils::ByteBuffer buffer = Aws::Utils::HashingUtils::Base64Decode(server_side_encryption_customer_key_base64);
        String str_buffer(reinterpret_cast<char *>(buffer.GetUnderlyingData()), buffer.GetLength());
        headers.push_back({Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
            Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(str_buffer))});
    }

    // These will be added after request signing
    client_configuration.extra_headers = std::move(headers);

    Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key, session_token);
    auto credentials_provider = std::make_shared<S3CredentialsProviderChain>(
            client_configuration,
            std::move(credentials),
            credentials_configuration);

    client_configuration.retryStrategy = std::make_shared<Client::RetryStrategy>(std::move(client_configuration.retryStrategy));
    return Client::create(
        client_configuration.s3_max_redirects,
        std::move(sse_kms_config),
        credentials_provider,
        client_configuration, // Client configuration.
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        is_virtual_hosted_style || client_configuration.endpointOverride.empty() /// Use virtual addressing if endpoint is not specified.
    );
}

PocoHTTPClientConfiguration ClientFactory::createClientConfiguration( // NOLINT
    const String & force_region,
    const RemoteHostFilter & remote_host_filter,
    unsigned int s3_max_redirects,
    bool enable_s3_requests_logging,
    bool for_disk_s3,
    const ThrottlerPtr & get_request_throttler,
    const ThrottlerPtr & put_request_throttler,
    const String & protocol)
{
    auto proxy_configuration_resolver = DB::ProxyConfigurationResolverProvider::get(DB::ProxyConfiguration::protocolFromString(protocol));

    auto per_request_configuration = [=] () { return proxy_configuration_resolver->resolve(); };
    auto error_report = [=] (const DB::ProxyConfiguration & req) { proxy_configuration_resolver->errorReport(req); };

    auto config = PocoHTTPClientConfiguration(
        per_request_configuration,
        force_region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging,
        for_disk_s3,
        get_request_throttler,
        put_request_throttler,
        error_report);

    config.scheme = Aws::Http::SchemeMapper::FromString(protocol.c_str());

    return config;
}

}

}

#endif
