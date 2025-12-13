#include <Storages/Kafka/AWSMSKIAMAuth.h>

#include <cppkafka/configuration.h>
#include <librdkafka/rdkafka.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include <Common/Base64.h>
#include <base/types.h>
#include <chrono>
#include <memory>

#if defined(USE_AWS_S3) && USE_AWS_S3
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/PocoHTTPClient.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int AWS_ERROR;
}

#if defined(USE_AWS_S3) && USE_AWS_S3

namespace
{
    /// OAuth token lifetime: 5 minutes (AWS standard)
    /// librdkafka auto-refreshes at 80% (4 minutes)
    constexpr int64_t MSK_IAM_TOKEN_LIFETIME_SECONDS = 300;

    /// OAuth callback context - MEMORY SAFE with shared_ptr
    ///
    /// CRITICAL: librdkafka copies rd_kafka_conf_t (cppkafka always copies).
    /// Opaque pointer is copied, so multiple rd_kafka_t share same pointer.
    /// Solution: Store shared_ptr, reference counting handles cleanup.
    struct OAuthBearerTokenRefreshContext
    {
        String region;
        LoggerPtr log;
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    };

    /// Generate AWS MSK IAM token using AWS SDK AWSAuthV4Signer
    /// Uses official AWS SDK instead of manual SigV4 implementation
    String generateAWSMSKToken(
        const String & region,
        const Aws::Auth::AWSCredentials & credentials)
    {
        try
        {
            // Build MSK service endpoint: kafka.<region>.amazonaws.com
            String service_host = "kafka." + region + ".amazonaws.com";

            // Build URI: https://{service_host}/?Action=kafka-cluster:Connect
            Aws::Http::URI uri;
            uri.SetScheme(Aws::Http::Scheme::HTTPS);
            uri.SetAuthority(service_host.c_str());
            uri.SetPath("/");
            uri.AddQueryStringParameter("Action", "kafka-cluster:Connect");

            // Create HTTP GET request
            auto request = Aws::Http::CreateHttpRequest(
                uri,
                Aws::Http::HttpMethod::HTTP_GET,
                Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

            // Create signer for kafka-cluster service
            Aws::Client::AWSAuthV4Signer signer(
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials),
                "kafka-cluster",
                region.c_str(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false);

            // Generate presigned URL (SDK handles SigV4)
            // Note: AWS MSK standard uses 900s (15min) expiry for presigned URL
            // This provides buffer time even though OAuth token lifetime is 300s (5min)
            if (!signer.PresignRequest(*request, region.c_str(), "kafka-cluster", 900))
            {
                throw Exception(ErrorCodes::AWS_ERROR, "Failed to presign AWS MSK IAM request");
            }

            String presigned_url = request->GetURIString();

            // Validate presigned URL format
            if (presigned_url.empty() || !presigned_url.contains("Action=kafka-cluster%3AConnect"))
            {
                throw Exception(ErrorCodes::AWS_ERROR,
                    "Invalid presigned URL generated: missing required Action parameter");
            }

            // AWS MSK requires Base64-URL encoding (RFC 4648 Section 5)
            // url_encoding=true converts + to -, / to _, and removes padding
            return base64Encode(presigned_url, /* url_encoding */ true, /* no_padding */ true);
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::AWS_ERROR, "Failed to generate AWS MSK token: {}", e.what());
        }
    }

    /// Cleanup callback (called by librdkafka on destruction)
    void oauthBearerContextDestructor(void * opaque)
    {
        auto * context_ptr = static_cast<std::shared_ptr<OAuthBearerTokenRefreshContext>*>(opaque);
        delete context_ptr;  // Decrements refcount
    }

    /// OAuth token refresh callback
    /// Thread-safe: AWS SDK uses internal locks
    void oauthBearerTokenRefreshCallback(
        rd_kafka_t * rk,
        const char * /* oauthbearer_config */,
        void * opaque)
    {
        auto * context_ptr = static_cast<std::shared_ptr<OAuthBearerTokenRefreshContext>*>(opaque);

        if (!context_ptr || !(*context_ptr))
        {
            auto fallback_log = getLogger("AWSMSKIAMAuth");
            LOG_ERROR(fallback_log, "Invalid OAuth context");
            rd_kafka_oauthbearer_set_token_failure(rk, "invalid context");
            return;
        }

        auto & context = **context_ptr;

        try
        {
            // Build MSK service endpoint for token generation
            // Note: This is the AWS MSK service endpoint, not the actual broker address
            // AWS MSK IAM requires signing against kafka.<region>.amazonaws.com
            // regardless of the actual broker addresses (which may be VPC endpoints or custom DNS)
            String service_host = "kafka." + context.region + ".amazonaws.com";

            LOG_DEBUG(context.log, "AWS MSK IAM token refresh for {}", service_host);

            // Get credentials
            auto credentials = context.credentials_provider->GetAWSCredentials();
            if (credentials.IsEmpty())
            {
                LOG_ERROR(context.log, "Failed to get AWS credentials for MSK IAM authentication");
                rd_kafka_oauthbearer_set_token_failure(rk, "No AWS credentials available");
                return;
            }

            // Generate token
            String token = generateAWSMSKToken(context.region, credentials);

            // Token expiry
            auto now = std::chrono::system_clock::now();
            auto expiry_time = now + std::chrono::seconds(MSK_IAM_TOKEN_LIFETIME_SECONDS);
            auto expiry_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                expiry_time.time_since_epoch()).count();

            // Set token in librdkafka
            rd_kafka_error_t * error = rd_kafka_oauthbearer_set_token(
                rk,
                token.c_str(),
                expiry_ms,
                credentials.GetAWSAccessKeyId().c_str(),
                nullptr, 0,
                nullptr);

            if (error)
            {
                String error_msg = rd_kafka_error_string(error);
                rd_kafka_error_destroy(error);
                LOG_ERROR(context.log, "Failed to set OAuth token: {}", error_msg);
                rd_kafka_oauthbearer_set_token_failure(rk, error_msg.c_str());
            }
            else
            {
                LOG_INFO(context.log, "AWS MSK IAM OAuth token refreshed");
            }
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(context.log, "Exception in token refresh: {}", e.what());
            rd_kafka_oauthbearer_set_token_failure(rk, e.what());
        }
    }
}

String AWSMSKIAMAuth::extractRegionFromBroker(const String & broker_address)
{
    // MSK formats:
    // Standard: b-1.cluster.kafka.us-east-1.amazonaws.com:9098
    // Serverless: boot-X.kafka-serverless.us-east-1.amazonaws.com:9098
    // VPC Endpoint: vpce-xxx.kafka.us-east-1.vpce.amazonaws.com:9098

    if (broker_address.empty())
        return "";

    // Remove port if present (e.g., :9098)
    String broker_host = broker_address;
    size_t colon_pos = broker_host.find(':');
    if (colon_pos != String::npos)
        broker_host = broker_host.substr(0, colon_pos);

    // Look for .amazonaws.com
    size_t aws_pos = broker_host.find(".amazonaws.com");
    if (aws_pos == String::npos)
        return "";

    // Extract region: find the component before .amazonaws.com
    // For VPC endpoints (vpce-xxx.kafka.region.vpce.amazonaws.com), skip .vpce
    size_t region_end = aws_pos;
    if (aws_pos >= 5 && broker_host.substr(aws_pos - 5, 5) == ".vpce")
        region_end = aws_pos - 5;

    size_t region_start = broker_host.rfind('.', region_end - 1);
    if (region_start == String::npos)
        return ""; // No dot found before region

    region_start++; // Skip the dot

    if (region_start >= region_end)
        return "";

    return broker_host.substr(region_start, region_end - region_start);
}

void AWSMSKIAMAuth::configureOAuthCallbacks(
    cppkafka::Configuration & config,
    const String & region,
    const String & broker_list,
    bool use_environment_credentials,
    LoggerPtr log)
{
    String effective_region = region;

    // Auto-detect region from broker
    if (effective_region.empty() && !broker_list.empty())
    {
        size_t comma_pos = broker_list.find(',');
        String first_broker = (comma_pos != String::npos)
            ? broker_list.substr(0, comma_pos)
            : broker_list;

        // Trim whitespace
        size_t start = first_broker.find_first_not_of(" \t\r\n");
        size_t end = first_broker.find_last_not_of(" \t\r\n");
        if (start != String::npos && end != String::npos)
        {
            first_broker = first_broker.substr(start, end - start + 1);
            effective_region = extractRegionFromBroker(first_broker);

            if (!effective_region.empty())
            {
                LOG_INFO(log, "Auto-detected region {} from broker {}", effective_region, first_broker);
            }
        }
    }
    else if (!effective_region.empty())
    {
        LOG_INFO(log, "Using configured region {}", effective_region);
    }

    if (effective_region.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot detect AWS region from broker list {}. "
            "For standard AWS MSK endpoints, brokers should match format: "
            "b-X.cluster.kafka.<region>.amazonaws.com:9098 (Standard MSK), "
            "boot-X.kafka-serverless.<region>.amazonaws.com:9098 (Serverless MSK), "
            "vpce-X.kafka.<region>.vpce.amazonaws.com:9098 (VPC Endpoint). "
            "For PrivateLink or custom DNS, explicitly set kafka_aws_region setting",
            broker_list);
    }

    // Set OAUTHBEARER (converted from user's rdkafka.sasl.mechanism=AWS_MSK_IAM)
    config.set("sasl.mechanism", "OAUTHBEARER");
    config.set("security.protocol", "SASL_SSL");

    // Create ClickHouse S3CredentialsProviderChain
    // This ensures credentials are obtained using ClickHouse's security model
    DB::S3::PocoHTTPClientConfiguration aws_client_configuration;
    aws_client_configuration.region = effective_region;

    // Configure credentials - use_environment_credentials controlled by server config only
    // Other fields use their default values from CredentialsConfiguration struct
    S3::CredentialsConfiguration credentials_configuration;
    credentials_configuration.use_environment_credentials = use_environment_credentials;

    // Empty credentials - rely on the provider chain
    Aws::Auth::AWSCredentials empty_credentials;

    auto credentials_provider = std::make_shared<S3::S3CredentialsProviderChain>(
        aws_client_configuration,
        empty_credentials,
        credentials_configuration);

    auto context = std::make_shared<OAuthBearerTokenRefreshContext>(
        OAuthBearerTokenRefreshContext{effective_region, log, credentials_provider});

    // Configure librdkafka
    rd_kafka_conf_t * rd_config = config.get_handle();

    rd_kafka_conf_enable_sasl_queue(rd_config, 1);  // Background refresh
    rd_kafka_conf_set_oauthbearer_token_refresh_cb(rd_config, oauthBearerTokenRefreshCallback);
    rd_kafka_conf_set_opaque_destructor(rd_config, oauthBearerContextDestructor);

    // Store shared_ptr (each conf copy increments refcount)
    // Note: rd_kafka_conf_set_opaque takes ownership, destructor will be called on cleanup
    auto * context_ptr = new std::shared_ptr<OAuthBearerTokenRefreshContext>(context);
    rd_kafka_conf_set_opaque(rd_config, context_ptr);

    LOG_INFO(log, "Configured AWS MSK IAM OAuth, region {}, use_environment_credentials {}", effective_region, use_environment_credentials);
}

#else // !USE_AWS_S3

void AWSMSKIAMAuth::configureOAuthCallbacks(
    cppkafka::Configuration & /* config */,
    const String & /* region */,
    const String & /* broker_list */,
    bool /* use_environment_credentials */,
    LoggerPtr /* log */)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "AWS MSK IAM authentication requires ClickHouse built with AWS S3 support (USE_AWS_S3=1)");
}

String AWSMSKIAMAuth::extractRegionFromBroker(const String & /* broker_address */)
{
    return "";
}

#endif // USE_AWS_S3

}
