#include <IO/S3/Client.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

#if USE_AWS_S3

#include <aws/core/Aws.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/ErrorMacros.h>

#include <Poco/Net/NetException.h>

#include <IO/Expect404ResponseScope.h>
#include <IO/S3/Requests.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/S3/AWSLogger.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>

#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProxyConfigurationResolverProvider.h>

#include <Core/Settings.h>

#include <base/sleep.h>
#include <Common/thread_local_rng.h>
#include <random>

namespace ProfileEvents
{
    extern const Event S3WriteRequestsErrors;
    extern const Event S3ReadRequestsErrors;

    extern const Event DiskS3WriteRequestsErrors;
    extern const Event DiskS3ReadRequestsErrors;

    extern const Event S3Clients;
    extern const Event TinyS3Clients;
}

namespace CurrentMetrics
{
    extern const Metric DiskS3NoSuchKeyErrors;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_use_adaptive_timeouts;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_REDIRECTS;
}

namespace S3
{

Client::RetryStrategy::RetryStrategy(uint32_t maxRetries_, uint32_t scaleFactor_, uint32_t maxDelayMs_)
    : maxRetries(maxRetries_)
    , scaleFactor(scaleFactor_)
    , maxDelayMs(maxDelayMs_)
{
    chassert(maxDelayMs <= uint64_t(scaleFactor) * (1ul << 31l));
}

/// NOLINTNEXTLINE(google-runtime-int)
bool Client::RetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const
{
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
        return false;

    if (attemptedRetries >= maxRetries)
        return false;

    if (CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
        return false;

    /// It does not make sense to retry when GCS suggest to use Rewrite
    if (useGCSRewrite(error))
        return false;

    return error.ShouldRetry();
}

bool Client::RetryStrategy::useGCSRewrite(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error)
{
    return error.GetResponseCode() == Aws::Http::HttpResponseCode::GATEWAY_TIMEOUT
        && error.GetExceptionName() == "InternalError"
        && error.GetMessage().contains("use the Rewrite method in the JSON API");
}


/// NOLINTNEXTLINE(google-runtime-int)
long Client::RetryStrategy::CalculateDelayBeforeNextRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>&, long attemptedRetries) const
{
    if (attemptedRetries == 0)
    {
        return 0;
    }

    uint64_t backoffLimitedPow = 1ul << std::min(attemptedRetries, 31l);
    return std::min<uint64_t>(scaleFactor * backoffLimitedPow, maxDelayMs);
}

/// NOLINTNEXTLINE(google-runtime-int)
long Client::RetryStrategy::GetMaxAttempts() const
{
    return maxRetries + 1;
}

namespace
{

void verifyClientConfiguration(const Aws::Client::ClientConfiguration & client_config)
{
    if (!client_config.retryStrategy)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The S3 client can only be used with Client::RetryStrategy, define it in the client configuration");

    assert_cast<const Client::RetryStrategy &>(*client_config.retryStrategy);
}

void addAdditionalAMZHeadersToCanonicalHeadersList(
    Aws::AmazonWebServiceRequest & request,
    const HTTPHeaderEntries & extra_headers
)
{
    for (const auto & [name, value] : extra_headers)
    {
        if (name.starts_with("x-amz-"))
        {
            request.SetAdditionalCustomHeaderValue(name, value);
        }
    }
}

}

std::unique_ptr<Client> Client::create(
    size_t max_redirects_,
    ServerSideEncryptionKMSConfig sse_kms_config_,
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider,
    const PocoHTTPClientConfiguration & client_configuration,
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads,
    const ClientSettings & client_settings)
{
    verifyClientConfiguration(client_configuration);
    return std::unique_ptr<Client>(
        new Client(max_redirects_, std::move(sse_kms_config_), credentials_provider, client_configuration, sign_payloads, client_settings));
}

std::unique_ptr<Client> Client::clone() const
{
    return std::unique_ptr<Client>(new Client(*this, client_configuration));
}

namespace
{

ProviderType deduceProviderType(const std::string & url)
{
    if (url.contains(".amazonaws.com"))
        return ProviderType::AWS;

    if (url.contains("storage.googleapis.com"))
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
    const ClientSettings & client_settings_)
    : Aws::S3::S3Client(credentials_provider_, client_configuration_, sign_payloads_, client_settings_.use_virtual_addressing)
    , credentials_provider(credentials_provider_)
    , client_configuration(client_configuration_)
    , sign_payloads(sign_payloads_)
    , client_settings(client_settings_)
    , max_redirects(max_redirects_)
    , sse_kms_config(std::move(sse_kms_config_))
    , log(getLogger("S3Client"))
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

    ProfileEvents::increment(ProfileEvents::S3Clients);
}

Client::Client(
    const Client & other, const PocoHTTPClientConfiguration & client_configuration_)
    : Aws::S3::S3Client(other.credentials_provider, client_configuration_, other.sign_payloads,
                        other.client_settings.use_virtual_addressing)
    , initial_endpoint(other.initial_endpoint)
    , credentials_provider(other.credentials_provider)
    , client_configuration(client_configuration_)
    , sign_payloads(other.sign_payloads)
    , client_settings(other.client_settings)
    , explicit_region(other.explicit_region)
    , detect_region(other.detect_region)
    , provider_type(other.provider_type)
    , max_redirects(other.max_redirects)
    , sse_kms_config(other.sse_kms_config)
    , log(getLogger("S3Client"))
{
    cache = std::make_shared<ClientCache>(*other.cache);
    ClientCacheRegistry::instance().registerClient(cache);

    ProfileEvents::increment(ProfileEvents::TinyS3Clients);
}


Client::~Client()
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

Aws::Auth::AWSCredentials Client::getCredentials() const
{
    return credentials_provider->GetAWSCredentials();
}

bool Client::checkIfCredentialsChanged(const Aws::S3::S3Error & error) const
{
    return (error.GetExceptionName() == "AuthenticationRequired");
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

Model::HeadObjectOutcome Client::HeadObject(HeadObjectRequest & request) const
{
    const auto & bucket = request.GetBucket();

    request.setApiMode(api_mode);

    if (isS3ExpressBucket())
        request.setIsS3ExpressBucket();

    addAdditionalAMZHeadersToCanonicalHeadersList(request, client_configuration.extra_headers);

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
    return processRequestResult(
        HeadObject(static_cast<const Model::HeadObjectRequest&>(request)));
}

/// For each request, we wrap the request functions from Aws::S3::Client with doRequest
/// doRequest calls virtuall function from Aws::S3::Client while DB::S3::Client has not virtual calls for each request type

Model::ListObjectsV2Outcome Client::ListObjectsV2(ListObjectsV2Request & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ true>(
        request, [this](const Model::ListObjectsV2Request & req) { return ListObjectsV2(req); });
}

Model::ListObjectsOutcome Client::ListObjects(ListObjectsRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ true>(
        request, [this](const Model::ListObjectsRequest & req) { return ListObjects(req); });
}

Model::GetObjectOutcome Client::GetObject(GetObjectRequest & request) const
{
    return processRequestResult(
        doRequest(request, [this](const Model::GetObjectRequest & req) { return GetObject(req); }));
}

Model::AbortMultipartUploadOutcome Client::AbortMultipartUpload(AbortMultipartUploadRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::AbortMultipartUploadRequest & req) { return AbortMultipartUpload(req); });
}

Model::CreateMultipartUploadOutcome Client::CreateMultipartUpload(CreateMultipartUploadRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CreateMultipartUploadRequest & req) { return CreateMultipartUpload(req); });
}

Model::CompleteMultipartUploadOutcome Client::CompleteMultipartUpload(CompleteMultipartUploadRequest & request) const
{
    auto outcome = doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CompleteMultipartUploadRequest & req) { return CompleteMultipartUpload(req); });

    const auto & key = request.GetKey();
    const auto & bucket = request.GetBucket();

    if (!outcome.IsSuccess()
        && outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD)
    {
        auto check_request = HeadObjectRequest()
                                 .WithBucket(bucket)
                                 .WithKey(key);
        auto check_outcome = HeadObject(check_request);

        /// if the key exists, than MultipartUpload has been completed at some of the retries
        /// rewrite outcome with success status
        if (check_outcome.IsSuccess())
            outcome = Aws::S3::Model::CompleteMultipartUploadOutcome(Aws::S3::Model::CompleteMultipartUploadResult());
    }

    if (outcome.IsSuccess() && provider_type == ProviderType::GCS && client_settings.gcs_issue_compose_request)
    {
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
            LOG_INFO(
                log,
                "Failed to compose object. Message: {}, Key: {}, Bucket: {}",
                compose_outcome.GetError().GetMessage(), key, bucket);
    }

    return outcome;
}

Model::CopyObjectOutcome Client::CopyObject(CopyObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::CopyObjectRequest & req) { return CopyObject(req); });
}

Model::PutObjectOutcome Client::PutObject(PutObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::PutObjectRequest & req) { return PutObject(req); });
}

Model::UploadPartOutcome Client::UploadPart(UploadPartRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::UploadPartRequest & req) { return UploadPart(req); });
}

Model::UploadPartCopyOutcome Client::UploadPartCopy(UploadPartCopyRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::UploadPartCopyRequest & req) { return UploadPartCopy(req); });
}

Model::DeleteObjectOutcome Client::DeleteObject(DeleteObjectRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::DeleteObjectRequest & req) { Expect404ResponseScope scope; return DeleteObject(req); });
}

Model::DeleteObjectsOutcome Client::DeleteObjects(DeleteObjectsRequest & request) const
{
    return doRequestWithRetryNetworkErrors</*IsReadMethod*/ false>(
        request, [this](const Model::DeleteObjectsRequest & req) { Expect404ResponseScope scope; return DeleteObjects(req); });
}

Client::ComposeObjectOutcome Client::ComposeObject(ComposeObjectRequest & request) const
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
Client::doRequest(RequestType & request, RequestFn request_fn) const
{
    addAdditionalAMZHeadersToCanonicalHeadersList(request, client_configuration.extra_headers);
    const auto & bucket = request.GetBucket();
    request.setApiMode(api_mode);

    /// We have to use checksums for S3Express buckets, so the order of checks should be the following
    if (client_settings.is_s3express_bucket)
        request.setIsS3ExpressBucket();
    else if (client_settings.disable_checksum)
        request.disableChecksum();

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

        if (checkIfCredentialsChanged(error))
        {
            LOG_INFO(log, "Credentials changed, attempting again");
            credentials_provider->SetNeedRefresh();
            continue;
        }

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

        if (initial_endpoint.substr(11) == "amazonaws.com") // Check if user didn't mention any region
            new_uri->addRegionToURI(request.getRegionOverride());

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
Client::doRequestWithRetryNetworkErrors(RequestType & request, RequestFn request_fn) const
{
    addAdditionalAMZHeadersToCanonicalHeadersList(request, client_configuration.extra_headers);
    auto with_retries = [this, request_fn_ = std::move(request_fn)] (const RequestType & request_)
    {
        chassert(client_configuration.retryStrategy);
        const Int64 max_attempts = client_configuration.retryStrategy->GetMaxAttempts();
        chassert(max_attempts > 0);
        std::exception_ptr last_exception = nullptr;
        for (Int64 attempt_no = 0; attempt_no < max_attempts; ++attempt_no)
        {
            /// Sometimes we need to slow down because other requests failed with network errors to free the S3 server a bit.
            slowDownAfterNetworkError();

            try
            {
                /// S3 does retries network errors actually.
                /// But it does matter when errors occur.
                /// This code retries a specific case when
                /// network error happens when XML document is being read from the response body.
                /// Hence, the response body is a stream, network errors are possible at reading.
                /// S3 doesn't retry them.

                /// Not all requests can be retried in that way.
                /// Requests that read out response body to build the result are possible to retry.
                /// Requests that expose the response stream as an answer are not retried with that code. E.g. GetObject.
                return request_fn_(request_);
            }
            catch (Poco::Net::NetException &)
            {
                /// This includes "connection reset", "malformed message", and possibly other exceptions.

                if constexpr (IsReadMethod)
                {
                    if (isClientForDisk())
                        ProfileEvents::increment(ProfileEvents::DiskS3ReadRequestsErrors);
                    else
                        ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors);
                }
                else
                {
                    if (isClientForDisk())
                        ProfileEvents::increment(ProfileEvents::DiskS3WriteRequestsErrors);
                    else
                        ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors);
                }

                tryLogCurrentException(log, "Will retry");
                last_exception = std::current_exception();

                auto error = Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::NETWORK_CONNECTION, /*retry*/ true);

                /// Check if query is canceled
                if (!client_configuration.retryStrategy->ShouldRetry(error, attempt_no))
                    break;

                sleepAfterNetworkError(error, attempt_no);
            }
        }

        chassert(last_exception);
        std::rethrow_exception(last_exception);
    };

    return doRequest(request, with_retries);
}

template <typename RequestResult>
RequestResult Client::processRequestResult(RequestResult && outcome) const
{
    if (outcome.IsSuccess() || !isClientForDisk())
        return std::forward<RequestResult>(outcome);

    if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY && !Expect404ResponseScope::is404Expected())
        CurrentMetrics::add(CurrentMetrics::DiskS3NoSuchKeyErrors);

    String enriched_message = fmt::format(
        "{} {}",
        outcome.GetError().GetMessage(),
        Expect404ResponseScope::is404Expected() ? "This error is expected for S3 disk."  : "This error happened for S3 disk.");

    auto error = outcome.GetError();
    error.SetMessage(enriched_message);

    return RequestResult(error);
}

void Client::sleepAfterNetworkError(Aws::Client::AWSError<Aws::Client::CoreErrors> error, Int64 attempt_no) const
{
    auto sleep_ms = client_configuration.retryStrategy->CalculateDelayBeforeNextRetry(error, attempt_no);
    if (!client_configuration.s3_slow_all_threads_after_network_error)
    {
        LOG_WARNING(log, "Request failed, now waiting {} ms before attempting again", sleep_ms);
        sleepForMilliseconds(sleep_ms);
        return;
    }

    /// Set the time other s3 requests must wait until.
    UInt64 current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    UInt64 next_time_ms = current_time_ms + sleep_ms;
    /// next_time_to_retry_after_network_error = std::max(next_time_to_retry_after_network_error, next_time_ms)
    for (UInt64 stored_next_time = next_time_to_retry_after_network_error;
         (stored_next_time < next_time_ms) && !next_time_to_retry_after_network_error.compare_exchange_weak(stored_next_time, next_time_ms);)
    {
    }
}

void Client::slowDownAfterNetworkError() const
{
    if (!client_configuration.s3_slow_all_threads_after_network_error)
        return;

    /// Wait until `next_time_to_retry_after_network_error`.
    for (;;)
    {
        UInt64 current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        UInt64 next_time_ms = next_time_to_retry_after_network_error.load();
        if (current_time_ms >= next_time_ms)
            break;
        UInt64 sleep_ms = next_time_ms - current_time_ms;

        /// Adds jitter: a random factor in the range [100%, 110%] to the delay.
        /// This prevents synchronized retries, reducing the risk of overwhelming the S3 server.
        std::uniform_real_distribution<double> dist(1.0, 1.1);
        double jitter = dist(thread_local_rng);
        sleep_ms = static_cast<UInt64>(jitter * sleep_ms);

        LOG_WARNING(log, "Some request failed, now waiting {} ms before executing a request", sleep_ms);
        sleepForMilliseconds(sleep_ms);
    }
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
        /// all "x-amz-*" headers have to be either converted or deleted
        /// note that "amz-sdk-invocation-id" and "amz-sdk-request" are preserved
        httpRequest->DeleteHeader("x-amz-api-version");
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

    addAdditionalAMZHeadersToCanonicalHeadersList(req, client_configuration.extra_headers);

    std::string region;
    auto outcome = HeadBucket(req);
    if (outcome.IsSuccess())
    {
        const auto & result = outcome.GetResult();
        region = result.GetBucketRegion();
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

ClientCache::ClientCache(const ClientCache & other)
{
    {
        std::lock_guard lock(other.region_cache_mutex);
        region_for_bucket_cache = other.region_for_bucket_cache;
    }
    {
        std::lock_guard lock(other.uri_cache_mutex);
        uri_for_bucket_cache = other.uri_for_bucket_cache;
    }
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
            LOG_INFO(getLogger("ClientCacheRegistry"), "Deleting leftover S3 client cache");
            it = client_caches.erase(it);
        }
    }

}

ClientFactory::ClientFactory()
{
    aws_options = Aws::SDKOptions{};

    aws_options.cryptoOptions = Aws::CryptoOptions{};
    aws_options.cryptoOptions.initAndCleanupOpenSSL = false;

    aws_options.httpOptions = Aws::HttpOptions{};
    aws_options.httpOptions.initAndCleanupCurl = false;
    aws_options.httpOptions.httpClientFactory_create_fn = []() { return std::make_shared<PocoHTTPClientFactory>(); };

    aws_options.loggingOptions = Aws::LoggingOptions{};
    aws_options.loggingOptions.logger_create_fn = []() { return std::make_shared<AWSLogger>(false); };

    aws_options.ioOptions = Aws::IoOptions{};
    /// We don't need to initialize TLS, because we use PocoHTTPClientFactory
    aws_options.ioOptions.tlsConnectionOptions_create_fn = []() { return nullptr; };

#if USE_JEMALLOC
    memory_manager = std::make_unique<AwsNodumpMemoryManager>();
    aws_options.memoryManagementOptions.memoryManager = memory_manager.get();
#endif

    Aws::InitAPI(aws_options);
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
    ClientSettings client_settings,
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

    client_configuration.retryStrategy = std::make_shared<Client::RetryStrategy>(client_configuration.s3_retry_attempts);

    /// Use virtual addressing if endpoint is not specified.
    if (client_configuration.endpointOverride.empty())
        client_settings.use_virtual_addressing = true;

    return Client::create(
        client_configuration.s3_max_redirects,
        std::move(sse_kms_config),
        credentials_provider,
        client_configuration, // Client configuration.
        client_settings.is_s3express_bucket ? Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent
                                            : Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        client_settings);
}

PocoHTTPClientConfiguration ClientFactory::createClientConfiguration( // NOLINT
    const String & force_region,
    const RemoteHostFilter & remote_host_filter,
    unsigned int s3_max_redirects,
    unsigned int s3_retry_attempts,
    bool s3_slow_all_threads_after_network_error,
    bool enable_s3_requests_logging,
    bool for_disk_s3,
    const ThrottlerPtr & get_request_throttler,
    const ThrottlerPtr & put_request_throttler,
    const String & protocol)
{
    auto context = Context::getGlobalContextInstance();
    chassert(context);
    auto proxy_configuration_resolver = ProxyConfigurationResolverProvider::get(ProxyConfiguration::protocolFromString(protocol), context->getConfigRef());

    auto per_request_configuration = [=]{ return proxy_configuration_resolver->resolve(); };
    auto error_report = [=](const ProxyConfiguration & req) { proxy_configuration_resolver->errorReport(req); };

    auto config = PocoHTTPClientConfiguration(
        per_request_configuration,
        force_region,
        remote_host_filter,
        s3_max_redirects,
        s3_retry_attempts,
        s3_slow_all_threads_after_network_error,
        enable_s3_requests_logging,
        for_disk_s3,
        context->getGlobalContext()->getSettingsRef()[Setting::s3_use_adaptive_timeouts],
        get_request_throttler,
        put_request_throttler,
        error_report);

    config.scheme = Aws::Http::SchemeMapper::FromString(protocol.c_str());

    return config;
}

bool isS3ExpressEndpoint(const std::string & endpoint)
{
    /// On one hand this check isn't 100% reliable, on the other - all it will change is whether we attach checksums to the requests.
    return endpoint.contains("s3express");
}
}

}

#endif
