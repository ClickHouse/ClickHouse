#include <gtest/gtest.h>

#include <IO/S3/Credentials.h>
#include "config.h"


#if USE_AWS_S3

#include <atomic>
#include <cstdlib>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include <base/scope_guard.h>

#include <boost/algorithm/string/split.hpp>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteSettings.h>
#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3Settings.h>
#include <Poco/Util/ServerApplication.h>

#include <IO/S3/tests/TestPocoHTTPServer.h>

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
}

namespace ProfileEvents
{
    extern const Event S3SingleAttemptRetryConsultations;
}

/*
 * When all tests are executed together, `Context::getGlobalContextInstance()` is not null. Global context is used by
 * ProxyResolvers to get proxy configuration (used by S3 clients). If global context does not have a valid ConfigRef, it relies on
 * Poco::Util::Application::instance() to grab the config. However, at this point, the application is not yet initialized and
 * `Poco::Util::Application::instance()` returns nullptr. This causes the test to fail. To fix this, we create a dummy application that takes
 * care of initialization.
 * */
[[maybe_unused]] static Poco::Util::ServerApplication app;

static void restoreEnvVarForAwsS3ClientTests(const char * name, bool had_value, const std::string & saved_value)
{
    if (had_value)
    {
        (void)::setenv(name, saved_value.c_str(), 1); // NOLINT(concurrency-mt-unsafe)
    }
    else
    {
        (void)::unsetenv(name); // NOLINT(concurrency-mt-unsafe)
    }
}

static String getSSEAndSignedHeaders(const Poco::Net::MessageHeader & message_header)
{
    String content;
    for (const auto & [header_name, header_value] : message_header)
    {
        if (header_name.starts_with("x-amz-server-side-encryption"))
        {
            content += header_name + ": " + header_value + "\n";
        }
        else if (header_name == "authorization")
        {
            std::vector<String> parts;
            boost::split(parts, header_value, [](char c){ return c == ' '; });
            for (const auto & part : parts)
            {
                if (part.starts_with("SignedHeaders="))
                    content += header_name + ": ... " + part + " ...\n";
            }
        }
    }
    return content;
}

static void doReadRequest(std::shared_ptr<const DB::S3::Client> client, const DB::S3::URI & uri)
{
    String version_id;
    UInt64 max_single_read_retries = 1;

    DB::ReadSettings read_settings;
    DB::S3::S3RequestSettings request_settings;
    request_settings[DB::S3RequestSetting::max_single_read_retries] = max_single_read_retries;
    DB::ReadBufferFromS3 read_buffer(
        client,
        uri.bucket,
        uri.key,
        version_id,
        request_settings,
        read_settings
    );

    String content;
    DB::readStringUntilEOF(content, read_buffer);
}

static void doWriteRequest(std::shared_ptr<const DB::S3::Client> client, const DB::S3::URI & uri)
{
    UInt64 max_unexpected_write_error_retries = 1;

    DB::S3::S3RequestSettings request_settings;
    request_settings[DB::S3RequestSetting::max_unexpected_write_error_retries] = max_unexpected_write_error_retries;
    DB::WriteBufferFromS3 write_buffer(
        client,
        uri.bucket,
        uri.key,
        DB::DBMS_DEFAULT_BUFFER_SIZE,
        request_settings,
        {}
    );

    write_buffer.write('\0'); // doesn't matter what we write here, just needs to be something
    write_buffer.finalize();
}

using RequestFn = std::function<void(std::shared_ptr<const DB::S3::Client>, const DB::S3::URI &)>;

static void testServerSideEncryption(
    RequestFn do_request,
    bool disable_checksum,
    String server_side_encryption_customer_key_base64,
    DB::S3::ServerSideEncryptionKMSConfig sse_kms_config,
    String expected_headers,
    bool is_s3express_bucket = false)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    unsigned int s3_retry_attempts = 0;
    bool s3_slow_all_threads_after_network_error = true;
    bool s3_slow_all_threads_after_retryable_error = true;
    DB::S3::URI uri(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/test.txt");
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    bool enable_s3_requests_logging = false;

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = s3_retry_attempts},
        s3_slow_all_threads_after_network_error,
        s3_slow_all_threads_after_retryable_error,
        enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        uri.uri.getScheme());

    client_configuration.endpointOverride = uri.endpoint;

    DB::HTTPHeaderEntries headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = disable_checksum,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = is_s3express_bucket,
    };

    std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        sse_kms_config,
        headers,
        DB::S3::CredentialsConfiguration
        {
            .use_environment_credentials = use_environment_credentials,
            .use_insecure_imds_request = use_insecure_imds_request,
        }
    );

    ASSERT_TRUE(client);

    do_request(client, uri);
    String content = getSSEAndSignedHeaders(http.getLastRequestHeader());
    EXPECT_EQ(content, expected_headers);
}

TEST(IOTestAwsS3Client, DoesNotRetryPreconditionFailed)
{
    /// B166: a 412 Precondition Failed (conditional CAS/dedup writes of the content-addressed
    /// backend) must NOT be retried, even when the SDK marks it retryable because an S3-compatible
    /// server (e.g. RustFS) returned a body whose ExceptionName it could not parse. Retrying it is a
    /// storm that stalls the write path.
    DB::S3::Client::RetryStrategy strategy(DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 10});

    Aws::Client::AWSError<Aws::Client::CoreErrors> precondition(Aws::Client::CoreErrors::UNKNOWN, /*isRetryable=*/true);
    precondition.SetResponseCode(Aws::Http::HttpResponseCode::PRECONDITION_FAILED);
    EXPECT_FALSE(strategy.ShouldRetry(precondition, /*attemptedRetries=*/0));
    EXPECT_TRUE(DB::S3::isPreconditionFailedError(precondition));       // one policy: agrees via response code

    /// A genuinely transient error is still retried (the guard is specific to 412).
    Aws::Client::AWSError<Aws::Client::CoreErrors> unavailable(Aws::Client::CoreErrors::SLOW_DOWN, /*isRetryable=*/true);
    unavailable.SetResponseCode(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE);
    EXPECT_TRUE(strategy.ShouldRetry(unavailable, /*attemptedRetries=*/0));
    EXPECT_FALSE(DB::S3::isPreconditionFailedError(unavailable));

    /// The one 412 policy also matches on the canonical <Code> name / raw body (the two CA conditional
    /// ops see an error whose ExceptionName the SDK DID parse, or whose body carries the token).
    Aws::Client::AWSError<Aws::S3::S3Errors> named(Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "precondition failed", false);
    EXPECT_TRUE(DB::S3::isPreconditionFailedError(named));

    /// Typed-exception surface consumed by S3 request finalization: name and message.
    EXPECT_TRUE(DB::S3Exception("boom", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed").isPreconditionFailed());
    EXPECT_FALSE(DB::S3Exception("boom", Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey").isPreconditionFailed());
}

/// Every consultation is counted, not just the first: simulating two retryable 5xx decisions in a row
/// proves the counter tracks each SDK consultation rather than being fixed/clamped at 1, which is what
/// makes it a live tripwire ("SDK-level retries must remain zero for conditional writes") rather than a
/// value nothing ever touches.
TEST(IOTestAwsS3Client, SingleAttemptRetryStrategyRefusesAndCounts)
{
    using ProfileEvents::global_counters;
    const auto before = global_counters[ProfileEvents::S3SingleAttemptRetryConsultations].load();
    DB::S3::SingleAttemptRetryStrategy strategy;
    const Aws::Client::AWSError<Aws::Client::CoreErrors> retryable_5xx(
        Aws::Client::CoreErrors::INTERNAL_FAILURE, /*isRetryable=*/true);
    EXPECT_FALSE(strategy.ShouldRetry(retryable_5xx, /*attempted=*/0));
    EXPECT_FALSE(strategy.ShouldRetry(retryable_5xx, /*attempted=*/1));
    EXPECT_EQ(strategy.GetMaxAttempts(), 1);
    EXPECT_EQ(global_counters[ProfileEvents::S3SingleAttemptRetryConsultations].load() - before, 2u);
}

struct ConditionalPutWireObservation
{
    bool negotiated_expect_continue = false;
    bool has_if_none_match = false;
    bool has_generation_match = false;
    std::string generation_match;
};

/// Drive a single-part conditional PUT (`If-None-Match: *`) with `body_size` bytes through a real
/// S3 client whose `expect_continue_min_bytes` gate is `threshold`, against the mock HTTP server, and
/// report what the request that reached the wire carried. `http_client` selects the GCS-mode client
/// to exercise; `request_mode` decides whether that client sees the write as native-conditional.
static ConditionalPutWireObservation observeConditionalPut(
    uint64_t threshold,
    size_t body_size,
    const std::string & http_client = "",
    DB::ObjectStorageRequestMode request_mode = DB::ObjectStorageRequestMode::Default)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    DB::S3::URI uri(http.getUrl() + "/IOTestAwsS3ClientExpectContinue/test.txt");

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ true,
        /* s3_slow_all_threads_after_retryable_error = */ true,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        uri.uri.getScheme());

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.expect_continue_min_bytes = threshold;
    client_configuration.http_client = http_client;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        /* access_key_id = */ "ACCESS_KEY_ID",
        /* secret_access_key = */ "SECRET_ACCESS_KEY",
        /* server_side_encryption_customer_key_base64 = */ "",
        /* sse_kms_config = */ {},
        /* headers = */ {},
        DB::S3::CredentialsConfiguration{
            .use_environment_credentials = false,
            .use_insecure_imds_request = false,
        });

    DB::S3::S3RequestSettings request_settings;
    request_settings[DB::S3RequestSetting::max_unexpected_write_error_retries] = 1;

    DB::WriteSettings write_settings;
    write_settings.object_storage_write_if_none_match = "*";
    write_settings.object_storage_request_mode = request_mode;

    DB::WriteBufferFromS3 write_buffer(
        client,
        uri.bucket,
        uri.key,
        DB::DBMS_DEFAULT_BUFFER_SIZE,
        request_settings,
        /* blob_log = */ nullptr,
        /* object_metadata = */ std::nullopt,
        /* schedule = */ {},
        write_settings);

    const std::string body(body_size, 'x');
    write_buffer.write(body.data(), body.size());
    write_buffer.finalize();

    const auto & header = http.getLastRequestHeader();
    ConditionalPutWireObservation observed;
    observed.negotiated_expect_continue = header.has("Expect");
    observed.has_if_none_match = header.has("if-none-match");
    observed.has_generation_match = header.has("x-goog-if-generation-match");
    if (observed.has_generation_match)
        observed.generation_match = header.get("x-goog-if-generation-match");
    return observed;
}

static bool conditionalPutNegotiatesExpectContinue(uint64_t threshold, size_t body_size)
{
    return observeConditionalPut(threshold, body_size).negotiated_expect_continue;
}

TEST(IOTestAwsS3Client, ExpectContinueOnlyWhenThresholdPositive)
{
    /// RExpect: `Expect: 100-continue` (B118) is scoped to CAS-owned conditional writes. A non-CAS S3
    /// client carries the default threshold 0 (disabled) and must NOT negotiate Expect on a conditional
    /// PUT — that is the upstream wire behaviour a non-CAS disk (e.g. Iceberg's If-None-Match commits)
    /// must keep. A CAS conditional-write client raises the threshold (see the single-attempt client in
    /// ObjectStorageBackend) and DOES negotiate it for a body at least that large.
    EXPECT_FALSE(conditionalPutNegotiatesExpectContinue(/*threshold=*/0, /*body_size=*/64));
    EXPECT_TRUE(conditionalPutNegotiatesExpectContinue(/*threshold=*/8, /*body_size=*/64));
    /// A positive threshold still excludes a body below it (only large bodies warrant the round-trip).
    EXPECT_FALSE(conditionalPutNegotiatesExpectContinue(/*threshold=*/128, /*body_size=*/64));
}

/// The GCS-mode clients translate conditions before delegating to the common HTTP boundary, so the
/// `Expect: 100-continue` gate — which lives at that boundary and recognises
/// `x-goog-if-generation-match` alongside the standard headers — still sees the condition either way.
/// Both requests below use the same endpoint, a mock server with no `storage.googleapis.com` in its
/// hostname, so nothing here is endpoint-sniffed.
TEST(IOTestAwsS3Client, GcsHmacTranslatesConditionsOnlyWhenMarkedAndKeepsExpectGate)
{
    const auto native = observeConditionalPut(
        /*threshold=*/8, /*body_size=*/64, "gcs_hmac", DB::ObjectStorageRequestMode::NativeConditional);
    EXPECT_TRUE(native.has_generation_match);
    EXPECT_EQ(native.generation_match, "0");
    EXPECT_FALSE(native.has_if_none_match);
    EXPECT_TRUE(native.negotiated_expect_continue);

    /// A Default request through the very same client keeps the standard ETag precondition, and the
    /// threshold semantics are unchanged by which form the condition took.
    const auto standard = observeConditionalPut(
        /*threshold=*/8, /*body_size=*/64, "gcs_hmac", DB::ObjectStorageRequestMode::Default);
    EXPECT_TRUE(standard.has_if_none_match);
    EXPECT_FALSE(standard.has_generation_match);
    EXPECT_TRUE(standard.negotiated_expect_continue);

    /// The pre-existing body-size gate still applies to both forms.
    EXPECT_FALSE(observeConditionalPut(
        /*threshold=*/128, /*body_size=*/64, "gcs_hmac", DB::ObjectStorageRequestMode::NativeConditional)
            .negotiated_expect_continue);
    EXPECT_FALSE(observeConditionalPut(
        /*threshold=*/128, /*body_size=*/64, "gcs_hmac", DB::ObjectStorageRequestMode::Default)
            .negotiated_expect_continue);
}

/// A `Default` PUT through the GOOG4 client must survive the authentication allowlist: whatever
/// `x-amz-*` headers the SDK puts on an ordinary write have to be translated or consumed, never
/// rejected. This is the ordinary-traffic regression the allowlist could break.
TEST(IOTestAwsS3Client, GcsHmacDefaultPutPassesTheAuthenticationAllowlist)
{
    EXPECT_NO_THROW(observeConditionalPut(
        /*threshold=*/0, /*body_size=*/64, "gcs_hmac", DB::ObjectStorageRequestMode::Default));
}

TEST(IOTestAwsS3Client, AppendExtraSSECHeadersRead)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748
    testServerSideEncryption(
        doReadRequest,
        /* disable_checksum= */ false,
        "Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=",
        {},
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "clickhouse-request;"
        "content-type;"
        "host;"
        "x-amz-api-version;"
        "x-amz-content-sha256;"
        "x-amz-date;"
        "x-amz-server-side-encryption-customer-algorithm;"
        "x-amz-server-side-encryption-customer-key;"
        "x-amz-server-side-encryption-customer-key-md5, ...\n"
        "x-amz-server-side-encryption-customer-algorithm: AES256\n"
        "x-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\n"
        "x-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

TEST(IOTestAwsS3Client, AppendExtraSSECHeadersWrite)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748
    testServerSideEncryption(
        doWriteRequest,
        /* disable_checksum= */ false,
        "Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=",
        {},
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "content-length;"
        "content-md5;"
        "content-type;"
        "host;"
        "x-amz-content-sha256;"
        "x-amz-date;"
        "x-amz-server-side-encryption-customer-algorithm;"
        "x-amz-server-side-encryption-customer-key;"
        "x-amz-server-side-encryption-customer-key-md5, ...\n"
        "x-amz-server-side-encryption-customer-algorithm: AES256\n"
        "x-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\n"
        "x-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

TEST(IOTestAwsS3Client, AppendExtraSSECHeadersWriteDisableChecksum)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748
    testServerSideEncryption(
        doWriteRequest,
        /* disable_checksum= */ true,
        "Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=",
        {},
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "content-length;"
        "content-type;"
        "host;"
        "x-amz-content-sha256;"
        "x-amz-date;"
        "x-amz-server-side-encryption-customer-algorithm;"
        "x-amz-server-side-encryption-customer-key;"
        "x-amz-server-side-encryption-customer-key-md5, ...\n"
        "x-amz-server-side-encryption-customer-algorithm: AES256\n"
        "x-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\n"
        "x-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

TEST(IOTestAwsS3Client, AppendExtraSSEKMSHeadersRead)
{
    DB::S3::ServerSideEncryptionKMSConfig sse_kms_config;
    sse_kms_config.key_id = "alias/test-key";
    sse_kms_config.encryption_context = "arn:aws:s3:::bucket_ARN";
    sse_kms_config.bucket_key_enabled = true;
    // KMS headers shouldn't be set on a read request
    testServerSideEncryption(
        doReadRequest,
        /* disable_checksum= */ false,
        "",
        sse_kms_config,
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "clickhouse-request;"
        "content-type;"
        "host;"
        "x-amz-api-version;"
        "x-amz-content-sha256;"
        "x-amz-date, ...\n");
}

TEST(IOTestAwsS3Client, AppendExtraSSEKMSHeadersWrite)
{
    DB::S3::ServerSideEncryptionKMSConfig sse_kms_config;
    sse_kms_config.key_id = "alias/test-key";
    sse_kms_config.encryption_context = "arn:aws:s3:::bucket_ARN";
    sse_kms_config.bucket_key_enabled = true;
    testServerSideEncryption(
        doWriteRequest,
        /* disable_checksum= */ false,
        "",
        sse_kms_config,
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "content-length;"
        "content-md5;"
        "content-type;"
        "host;"
        "x-amz-content-sha256;"
        "x-amz-date;"
        "x-amz-server-side-encryption;"
        "x-amz-server-side-encryption-aws-kms-key-id;"
        "x-amz-server-side-encryption-bucket-key-enabled;"
        "x-amz-server-side-encryption-context, ...\n"
        "x-amz-server-side-encryption: aws:kms\n"
        "x-amz-server-side-encryption-aws-kms-key-id: alias/test-key\n"
        "x-amz-server-side-encryption-bucket-key-enabled: true\n"
        "x-amz-server-side-encryption-context: arn:aws:s3:::bucket_ARN\n");
}


TEST(IOTestAwsS3Client, ChecksumHeaderIsPresentForS3Express)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748
    testServerSideEncryption(
        doWriteRequest,
        /* disable_checksum= */ true,
        "",
        {},
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "content-length;"
        "content-type;"
        "host;"
        "x-amz-checksum-crc32;"
        "x-amz-content-sha256;"
        "x-amz-date;"
        "x-amz-sdk-checksum-algorithm, ...\n",
        /*is_s3express_bucket=*/true);
}

TEST(IOTestAwsS3Client, DetectRegionFromS3ExpressEndpoint)
{
    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    unsigned int s3_retry_attempts = 0;
    bool s3_slow_all_threads_after_network_error = true;
    bool s3_slow_all_threads_after_retryable_error = true;
    bool enable_s3_requests_logging = false;
    DB::S3::URI uri("https://test-perf-bucket--eun1-az1--x-s3.s3express-eun1-az1.eu-north-1.amazonaws.com/test.csv");

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/"",
        remote_host_filter,
        s3_max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = s3_retry_attempts},
        s3_slow_all_threads_after_network_error,
        s3_slow_all_threads_after_retryable_error,
        enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        "https");

    client_configuration.endpointOverride = uri.endpoint;

    DB::HTTPHeaderEntries headers;
    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = DB::S3::isS3ExpressEndpoint(uri.endpoint),
    };

    std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        /*access_key_id=*/"ACCESS_KEY_ID",
        /*secret_access_key=*/"SECRET_ACCESS_KEY",
        /*server_side_encryption_customer_key_base64=*/"",
        {},
        headers,
        DB::S3::CredentialsConfiguration{});

    ASSERT_TRUE(client);
    EXPECT_EQ(client->getRegion(), "eu-north-1");
}

namespace
{

void validateCredential(const std::string_view credential_string, const std::string_view service_name, const std::string_view expected_access_key, const std::string_view expected_region)
{
    ASSERT_FALSE(credential_string.empty());
    if (!expected_access_key.empty())
    {
        const auto expected_start = fmt::format("Credential={}", expected_access_key);
        ASSERT_TRUE(credential_string.starts_with(expected_start));
    }

    if (!expected_region.empty())
    {
        const auto expected_end = fmt::format("{}/{}/aws4_request,", expected_region, service_name);
        ASSERT_TRUE(credential_string.ends_with(expected_end));

    }
}

void validateAssumeRoleQueryParams(const Poco::URI::QueryParameters query_params, const std::string_view expected_role_arn, const std::string_view expected_role_session_name, const std::string_view expected_external_id = "")
{
    bool external_id_present = false;
    for (const auto & [param, value] : query_params)
    {
        if (param == "Action")
            ASSERT_EQ(value, "AssumeRole");
        else if (param == "RoleArn")
            ASSERT_EQ(value, expected_role_arn);
        else if (param == "RoleSessionName")
            ASSERT_EQ(value, expected_role_session_name);
        else if (param == "ExternalId")
        {
            external_id_present = true;
            ASSERT_EQ(value, expected_external_id);
        }
    }
    /// ExternalId is optional and must be sent only when configured.
    ASSERT_EQ(external_id_present, !expected_external_id.empty());
}

}

TEST(IOTestAwsS3Client, InstanceProfileCredentialsProviderCaching)
{
    DB::S3::ClientFactory::instance();

    Aws::Client::ClientConfiguration client_config;
    client_config.connectTimeoutMs = 50;
    client_config.requestTimeoutMs = 1000;

    auto provider1 = DB::S3::AWSInstanceProfileCredentialsProvider::create(client_config, /*use_secure_pull=*/true);
    ASSERT_TRUE(provider1);

    auto provider2 = DB::S3::AWSInstanceProfileCredentialsProvider::create(client_config, /*use_secure_pull=*/true);
    ASSERT_TRUE(provider2);
    EXPECT_EQ(provider1.get(), provider2.get());

    auto provider3 = DB::S3::AWSInstanceProfileCredentialsProvider::create(client_config, /*use_secure_pull=*/false);
    ASSERT_TRUE(provider3);
    EXPECT_NE(provider1.get(), provider3.get());

    auto provider4 = DB::S3::AWSInstanceProfileCredentialsProvider::create(client_config, /*use_secure_pull=*/false);
    ASSERT_TRUE(provider4);
    EXPECT_EQ(provider3.get(), provider4.get());
}

TEST(IOTestAwsS3Client, AssumeRole)
{
    const auto get_credential_string =  [&](const Poco::Net::MessageHeader & headers) -> std::string
    {
        for (const auto & [header_name, header_value] : headers)
        {
            if (header_name == "authorization")
            {
                std::vector<String> parts;
                boost::split(parts, header_value, [](char c){ return c == ' '; });
                for (const auto & part : parts)
                {
                    if (part.starts_with("Credential="))
                    {
                        return part;
                    }
                }
            }
        }

        return "";
    };

    TestPocoHTTPServer http;

    static constexpr std::string_view role_access_key = "role_access_key";
    static constexpr std::string_view role_secret_key = "role_secret_key";

    TestPocoHTTPStsServer sts_http(std::string{role_access_key}, std::string{role_secret_key});

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    unsigned int s3_retry_attempts = 0;
    bool s3_slow_all_threads_after_network_error = true;
    bool s3_slow_all_threads_after_retryable_error = true;
    DB::S3::URI uri(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/test.txt");
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "eu-west-1";
    String version_id;
    UInt64 max_single_read_retries = 1;
    bool enable_s3_requests_logging = false;

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = s3_retry_attempts},
        s3_slow_all_threads_after_network_error,
        s3_slow_all_threads_after_retryable_error,
        enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        "http");

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>();

    DB::HTTPHeaderEntries headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;


    const auto read_from_s3 = [&](const std::string & role_arn, const std::string & role_session_name, const std::string & external_id = "")
    {
        DB::S3::ClientSettings client_settings{
            .use_virtual_addressing = uri.is_virtual_hosted_style,
            .disable_checksum = false,
        };

        std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
            client_configuration,
            client_settings,
            access_key_id,
            secret_access_key,
            "",
            {},
            headers,
            DB::S3::CredentialsConfiguration
            {
                .use_environment_credentials = use_environment_credentials,
                .use_insecure_imds_request = use_insecure_imds_request,
                .role_arn = role_arn,
                .role_session_name = role_session_name,
                .external_id = external_id,
                .sts_endpoint_override = sts_http.getUrl()
            }
        );

        ASSERT_TRUE(client);

        DB::ReadSettings read_settings;
        DB::S3::S3RequestSettings request_settings;
        request_settings[DB::S3RequestSetting::max_single_read_retries] = max_single_read_retries;
        DB::ReadBufferFromS3 read_buffer(
            client,
            uri.bucket,
            uri.key,
            version_id,
            request_settings,
            read_settings
        );

        std::string content;
        DB::readStringUntilEOF(content, read_buffer);

    };

    {
        SCOPED_TRACE("With role arn and role session name set");

        std::string role_arn = "arn::role/my_role";
        std::string role_session_name = "session_name";

        read_from_s3(role_arn, role_session_name);

        validateCredential(get_credential_string(http.getLastRequestHeader()), "s3", role_access_key, region);

        ASSERT_TRUE(sts_http.hasLastRequest());
        validateCredential(get_credential_string(sts_http.getLastRequestHeader()), "sts", access_key_id, region);
        validateAssumeRoleQueryParams(sts_http.getLastQueryParams(), role_arn, role_session_name);
    }

    {
        SCOPED_TRACE("With no role arn set");

        sts_http.resetLastRequest();

        read_from_s3("", "");

        validateCredential(get_credential_string(http.getLastRequestHeader()), "s3", access_key_id, region);
        ASSERT_FALSE(sts_http.hasLastRequest());
    }

    {
        SCOPED_TRACE("With role arn set and no role session name");

        sts_http.resetLastRequest();

        std::string role_arn = "arn::role/my_role";

        read_from_s3(role_arn, "");

        validateCredential(get_credential_string(http.getLastRequestHeader()), "s3", role_access_key, region);

        ASSERT_TRUE(sts_http.hasLastRequest());
        validateCredential(get_credential_string(sts_http.getLastRequestHeader()), "sts", access_key_id, region);
        validateAssumeRoleQueryParams(sts_http.getLastQueryParams(), role_arn, "ClickHouseSession");
    }

    {
        SCOPED_TRACE("With role arn and external id set");

        sts_http.resetLastRequest();

        std::string role_arn = "arn::role/my_role";
        std::string role_session_name = "session_name";
        std::string external_id = "my_external_id";

        read_from_s3(role_arn, role_session_name, external_id);

        validateCredential(get_credential_string(http.getLastRequestHeader()), "s3", role_access_key, region);

        ASSERT_TRUE(sts_http.hasLastRequest());
        validateCredential(get_credential_string(sts_http.getLastRequestHeader()), "sts", access_key_id, region);
        validateAssumeRoleQueryParams(sts_http.getLastQueryParams(), role_arn, role_session_name, external_id);
    }
}

TEST(IOTestAwsS3Client, ClientCacheRegistryGetOrCreateCacheForKey)
{
    auto & registry = DB::S3::ClientCacheRegistry::instance();

    std::shared_ptr<DB::S3::ClientCache> cache_ab1 = registry.getOrCreateCacheForKey("endpoint1", "bucket1");
    std::shared_ptr<DB::S3::ClientCache> cache_ab2 = registry.getOrCreateCacheForKey("endpoint1", "bucket1");
    EXPECT_EQ(cache_ab1.get(), cache_ab2.get()) << "Same (endpoint, bucket) should return the same cache";

    std::shared_ptr<DB::S3::ClientCache> cache_b1 = registry.getOrCreateCacheForKey("endpoint1", "bucket2");
    EXPECT_NE(cache_ab1.get(), cache_b1.get()) << "Different bucket should return different cache";

    std::shared_ptr<DB::S3::ClientCache> cache_e2 = registry.getOrCreateCacheForKey("endpoint2", "bucket1");
    EXPECT_NE(cache_ab1.get(), cache_e2.get()) << "Different endpoint should return different cache";

    auto cache_concat1 = registry.getOrCreateCacheForKey("ab", "c");
    auto cache_concat2 = registry.getOrCreateCacheForKey("a", "bc");
    EXPECT_NE(cache_concat1.get(), cache_concat2.get())
        << "Pairs with identical concatenation but different boundary must not share a cache";
}

TEST(IOTestAwsS3Client, ClientSharesCacheWithClone)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::URI uri("https://s3.eu-central-1.amazonaws.com/my-bucket/key");
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "eu-central-1",
        remote_host_filter,
        10,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        true,
        true,
        false,
        false,
        {},
        {},
        "https");
    client_configuration.endpointOverride = uri.endpoint;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    auto shared_cache = DB::S3::ClientCacheRegistry::instance().getOrCreateCacheForKey(uri.endpoint, uri.bucket);
    std::unique_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        "access",
        "secret",
        "",
        {},
        {},
        DB::S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false},
        "",
        shared_cache);

    ASSERT_TRUE(client);
    std::unique_ptr<DB::S3::Client> clone = client->clone();
    ASSERT_TRUE(clone);

    EXPECT_EQ(client->getRawCache(), shared_cache.get()) << "Client should use the shared cache";
    EXPECT_EQ(clone->getRawCache(), client->getRawCache()) << "Clone should share the same cache as original";
}

TEST(IOTestAwsS3Client, ClientCacheRegistryRefcount)
{
    /// Verify ClientCacheRegistry refcounting directly via the test-only refcount accessor.
    /// We can't go through Client construction/destruction because Client::~Client catches
    /// and logs exceptions from unregisterClient, so a refcount bug would be invisible;
    /// and we can't probe via the throwing path (the entry was already removed) because in
    /// debug/sanitizer builds LOGICAL_ERROR aborts the process instead of throwing.
    auto & registry = DB::S3::ClientCacheRegistry::instance();
    auto shared_cache = registry.getOrCreateCacheForKey(
        "https://s3.us-east-1.amazonaws.com",
        "test-refcount-bucket");

    ASSERT_EQ(registry.getClientRefcountForTesting(shared_cache.get()), 0u);

    registry.registerClient(shared_cache);
    EXPECT_EQ(registry.getClientRefcountForTesting(shared_cache.get()), 1u);

    /// Second registration of the same cache must bump the refcount, not silently no-op.
    registry.registerClient(shared_cache);
    EXPECT_EQ(registry.getClientRefcountForTesting(shared_cache.get()), 2u);

    registry.unregisterClient(shared_cache.get());
    EXPECT_EQ(registry.getClientRefcountForTesting(shared_cache.get()), 1u);

    registry.unregisterClient(shared_cache.get());
    EXPECT_EQ(registry.getClientRefcountForTesting(shared_cache.get()), 0u);
}

TEST(IOTestAwsS3Client, WebIdentityConfiguredFromEnvironment)
{
    constexpr const char * k_role = "AWS_ROLE_ARN";
    constexpr const char * k_token = "AWS_WEB_IDENTITY_TOKEN_FILE";

    const char * prev_role = std::getenv(k_role); // NOLINT(concurrency-mt-unsafe)
    const char * prev_token = std::getenv(k_token); // NOLINT(concurrency-mt-unsafe)
    const bool had_role = prev_role != nullptr;
    const bool had_token = prev_token != nullptr;
    const std::string saved_role = had_role ? std::string(prev_role) : std::string();
    const std::string saved_token = had_token ? std::string(prev_token) : std::string();

    SCOPE_EXIT({ restoreEnvVarForAwsS3ClientTests(k_role, had_role, saved_role); });
    SCOPE_EXIT({ restoreEnvVarForAwsS3ClientTests(k_token, had_token, saved_token); });

    ASSERT_EQ(0, ::setenv(k_role, "arn:aws:iam::123456789012:role/clickhouse_unit_test_role", 1)); // NOLINT(concurrency-mt-unsafe)
    ASSERT_EQ(0, ::setenv(k_token, "/tmp/clickhouse_web_identity_token_path_for_gtest", 1)); // NOLINT(concurrency-mt-unsafe)

    EXPECT_TRUE(DB::S3::AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::isWebIdentityConfigured({}));
}

TEST(IOTestAwsS3Client, WebIdentityConfiguredFromKmsRoleOverrideAndTokenFile)
{
    constexpr const char * k_role = "AWS_ROLE_ARN";
    constexpr const char * k_token = "AWS_WEB_IDENTITY_TOKEN_FILE";

    const char * prev_role = std::getenv(k_role); // NOLINT(concurrency-mt-unsafe)
    const char * prev_token = std::getenv(k_token); // NOLINT(concurrency-mt-unsafe)
    const bool had_role = prev_role != nullptr;
    const bool had_token = prev_token != nullptr;
    const std::string saved_role = had_role ? std::string(prev_role) : std::string();
    const std::string saved_token = had_token ? std::string(prev_token) : std::string();

    SCOPE_EXIT({ restoreEnvVarForAwsS3ClientTests(k_role, had_role, saved_role); });
    SCOPE_EXIT({ restoreEnvVarForAwsS3ClientTests(k_token, had_token, saved_token); });

    ASSERT_EQ(0, ::setenv(k_role, "", 1)); // NOLINT(concurrency-mt-unsafe)
    ASSERT_EQ(0, ::setenv(k_token, "/tmp/clickhouse_web_identity_token_path_for_gtest_override", 1)); // NOLINT(concurrency-mt-unsafe)

    EXPECT_TRUE(DB::S3::AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::isWebIdentityConfigured(
        "arn:aws:iam::123456789012:role/from_kms_role_arn_override"));
}

namespace
{

/// Builds a real `DB::S3::Client` with the given `http_client` value, wired the same way
/// `ClientFactory::create` wires disk configuration, but never sent over the wire: these tests only
/// exercise `Client::BuildHttpRequest`, which does no I/O.
std::unique_ptr<DB::S3::Client> makeClientWithHttpClient(const std::string & http_client)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::URI uri("https://storage.googleapis.com/bucket/key");

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/"us-east-1",
        remote_host_filter,
        /*s3_max_redirects=*/100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /*s3_slow_all_threads_after_network_error=*/true,
        /*s3_slow_all_threads_after_retryable_error=*/true,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*opt_disk_name=*/{},
        /*request_throttler=*/{},
        uri.uri.getScheme());

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.http_client = http_client;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    return DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        /*access_key_id=*/"ACCESS_KEY_ID",
        /*secret_access_key=*/"SECRET_ACCESS_KEY",
        /*server_side_encryption_customer_key_base64=*/"",
        /*sse_kms_config=*/{},
        /*headers=*/{},
        DB::S3::CredentialsConfiguration{
            .use_environment_credentials = false,
            .use_insecure_imds_request = false,
        });
}

/// A minimal scripted HTTP server: the Nth request it receives is answered with
/// `responses[min(N, responses.size() - 1)]`, so a short script (e.g. one retryable response then one
/// success) naturally "then always succeeds" once it runs out. Lets a test drive one genuine SDK-level
/// retry through a real `DB::S3::Client`, rather than standing in for the SDK's own per-attempt
/// behaviour by calling the same functions twice by hand.
struct ScriptedResponse
{
    Poco::Net::HTTPResponse::HTTPStatus status;
    std::vector<std::pair<std::string, std::string>> headers;
};

/// One real request as it reached the wire: method plus every header, captured before the scripted
/// response is sent. Lets a test drive several real SDK calls (e.g. CreateMultipartUpload, UploadPart,
/// CompleteMultipartUpload) against one server and inspect what each one actually carried, rather than
/// only the single most-recent request `TestPocoHTTPServer` keeps.
struct CapturedRequest
{
    std::string method;
    Poco::Net::MessageHeader headers;
};

class ScriptedResponseServer
{
public:
    explicit ScriptedResponseServer(std::vector<ScriptedResponse> responses_)
        : responses(std::move(responses_))
        , server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , handler_factory(new Factory(*this))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    /// `server_socket->address()` is the wildcard bind address (`0.0.0.0:PORT`), which is not a usable
    /// connection target and could silently conflate distinct servers under the same host string. Build
    /// the URL from an explicit loopback address plus the bound port instead.
    std::string getUrl() const { return "http://127.0.0.1:" + std::to_string(server_socket->address().port()); }

    /// Requests in arrival order. The tests using this all drive their calls sequentially against a
    /// single-threaded client, so no concurrent capture ever races with a concurrent read here.
    const std::vector<CapturedRequest> & getCapturedRequests() const { return captured_requests; }

private:
    class Handler : public Poco::Net::HTTPRequestHandler
    {
    public:
        explicit Handler(ScriptedResponseServer & owner_) : owner(owner_) { }

        void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
        {
            owner.captured_requests.push_back(CapturedRequest{request.getMethod(), request});

            /// The connection is kept alive across requests (the SDK reuses it for the multipart/batch
            /// sequences this server now handles), so an unread request body left in the socket buffer
            /// corrupts the next request's parse -- its bytes prepend the following request line. Every
            /// request body must be drained here even though nothing needs its content.
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());

            const size_t index = owner.request_count.fetch_add(1);
            const auto & scripted = owner.responses[std::min(index, owner.responses.size() - 1)];
            response.setStatus(scripted.status);
            for (const auto & [name, value] : scripted.headers)
                response.set(name, value);
            response.setContentLength(0);
            response.send();
        }

    private:
        ScriptedResponseServer & owner;
    };

    class Factory : public Poco::Net::HTTPRequestHandlerFactory
    {
    public:
        explicit Factory(ScriptedResponseServer & owner_) : owner(owner_) { }
        Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override { return new Handler(owner); }

    private:
        ScriptedResponseServer & owner;
    };

    std::vector<ScriptedResponse> responses;
    std::atomic<size_t> request_count{0};
    std::vector<CapturedRequest> captured_requests;
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<Factory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

/// `Client::BuildHttpRequest` is not `final`, and the protected constructor `Client` exposes is
/// commented "visible for testing" — this subclass uses exactly that seam to observe every real
/// `BuildHttpRequest` call the vendored SDK makes for a genuine attempt, without adding any
/// observability to production code (the mode has no wire representation by design, so there is no
/// other way to see it from outside the process).
class RecordingClient : public DB::S3::Client
{
public:
    RecordingClient(
        size_t max_redirects_,
        DB::S3::ServerSideEncryptionKMSConfig sse_kms_config_,
        const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider_,
        const DB::S3::PocoHTTPClientConfiguration & client_configuration_,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads_,
        const DB::S3::ClientSettings & client_settings_)
        : DB::S3::Client(max_redirects_, std::move(sse_kms_config_), credentials_provider_, client_configuration_, sign_payloads_, client_settings_)
    {
    }

    /// One entry per real `BuildHttpRequest` call, i.e. one per genuine SDK attempt.
    mutable std::vector<bool> observed_native_conditional;

    void BuildHttpRequest(const Aws::AmazonWebServiceRequest & request, const std::shared_ptr<Aws::Http::HttpRequest> & httpRequest) const override
    {
        DB::S3::Client::BuildHttpRequest(request, httpRequest);
        observed_native_conditional.push_back(DB::S3::isNativeConditionalRequest(*httpRequest));
    }
};

std::unique_ptr<RecordingClient> makeRecordingClient(
    const std::string & endpoint, unsigned int max_retries, unsigned int max_redirects, const std::string & http_client = "gcs_hmac")
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::URI uri(endpoint + "/bucket");

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/"us-east-1",
        remote_host_filter,
        max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = max_retries},
        /*s3_slow_all_threads_after_network_error=*/false,
        /*s3_slow_all_threads_after_retryable_error=*/false,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*opt_disk_name=*/{},
        /*request_throttler=*/{},
        uri.uri.getScheme());

    client_configuration.endpointOverride = uri.endpoint;
    /// The default `gcs_hmac`, not `gcp_oauth`: both make `supportsGcsNativeConditionalRequests()`
    /// true, but `gcp_oauth` fetches a bearer token from the GCE metadata server on every real request
    /// (`PocoHTTPClientGCPOAuth::requestBearerToken`) -- a real network call this test cannot make.
    /// `gcs_hmac` signs locally from the credentials handed to it below, no token fetch involved. An
    /// empty `http_client` selects the ordinary (non-GCS) HMAC path instead, wired the same way as a
    /// plain S3-compatible disk -- it never invokes either GCS client class.
    client_configuration.http_client = http_client;
    /// `ClientFactory::create` would clamp this to 1 when `s3_slow_all_threads_after_retryable_error`
    /// is set (external retry coordination); here we want the SDK's own retry loop to actually run.
    client_configuration.retryStrategy = std::make_shared<DB::S3::Client::RetryStrategy>(client_configuration.retry_strategy);

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    Aws::Auth::AWSCredentials credentials("ACCESS_KEY_ID", "SECRET_ACCESS_KEY");
    auto credentials_provider = DB::S3::getCredentialsProvider(
        client_configuration,
        credentials,
        DB::S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false});
    /// `PocoHTTPClientGCSHMAC`'s constructor throws `LOGICAL_ERROR` without this -- `ClientFactory::create`
    /// wires it the same way for the real `gcs_hmac` path.
    client_configuration.gcs_hmac_credentials_provider = credentials_provider;

    return std::make_unique<RecordingClient>(
        max_redirects,
        DB::S3::ServerSideEncryptionKMSConfig{},
        credentials_provider,
        client_configuration,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        client_settings);
}

}

TEST(IOTestAwsS3Client, RequestModeDefaultsToDefault)
{
    DB::WriteSettings settings;
    EXPECT_EQ(settings.object_storage_request_mode, DB::ObjectStorageRequestMode::Default);
}

TEST(IOTestAwsS3Client, FactoryAlwaysCreatesExtendedHttpRequest)
{
    DB::S3::PocoHTTPClientFactory factory;
    const Aws::IOStreamFactory stream_factory = [] { return nullptr; };

    auto from_string_uri = factory.CreateHttpRequest(
        Aws::String("http://localhost/bucket/key"), Aws::Http::HttpMethod::HTTP_GET, stream_factory);
    ASSERT_TRUE(from_string_uri);
    EXPECT_TRUE(dynamic_cast<DB::S3::ExtendedHttpRequest *>(from_string_uri.get()));

    auto from_uri = factory.CreateHttpRequest(
        Aws::Http::URI("http://localhost/bucket/key"), Aws::Http::HttpMethod::HTTP_PUT, stream_factory);
    ASSERT_TRUE(from_uri);
    EXPECT_TRUE(dynamic_cast<DB::S3::ExtendedHttpRequest *>(from_uri.get()));
}

TEST(IOTestAwsS3Client, NativeConditionalModeRequiresExplicitGcsHttpClient)
{
    EXPECT_TRUE(makeClientWithHttpClient("gcp_oauth")->supportsGcsNativeConditionalRequests());
    EXPECT_TRUE(makeClientWithHttpClient("gcs_hmac")->supportsGcsNativeConditionalRequests());
    /// The comparison is case-insensitive, matching how `ClientFactory::create` already lower-cases
    /// this same field before dispatching on it.
    EXPECT_TRUE(makeClientWithHttpClient("GCS_HMAC")->supportsGcsNativeConditionalRequests());
    EXPECT_FALSE(makeClientWithHttpClient("")->supportsGcsNativeConditionalRequests());
    EXPECT_FALSE(makeClientWithHttpClient("some_other_client")->supportsGcsNativeConditionalRequests());
}

TEST(IOTestAwsS3Client, ForeignHttpRequestReadsAsDefault)
{
    Aws::Http::Standard::StandardHttpRequest foreign_request(Aws::Http::URI("http://localhost/x"), Aws::Http::HttpMethod::HTTP_GET);
    EXPECT_FALSE(DB::S3::isNativeConditionalRequest(foreign_request));
}

TEST(IOTestAwsS3Client, NativeConditionalStaysFalseThroughBuildHttpRequestOnNonGcsClient)
{
    /// Closes a coverage gap: nothing else in this file drives `Client::BuildHttpRequest` with a
    /// native-marked request against a non-GCS `http_client`. Without this, dropping or inverting the
    /// `&& supportsGcsNativeConditionalRequests()` conjunct would not fail any test here, even though
    /// that conjunct is exactly what keeps the HTTP bit false for AWS-compatible CAS requests.
    auto client = makeClientWithHttpClient("some_other_client");
    ASSERT_FALSE(client->supportsGcsNativeConditionalRequests());

    DB::S3::PutObjectRequest request;
    request.SetBucket("bucket");
    request.SetKey("key");
    request.setNativeConditional(true);

    DB::S3::PocoHTTPClientFactory factory;
    const Aws::IOStreamFactory stream_factory = [] { return nullptr; };
    auto http_request = factory.CreateHttpRequest(
        Aws::Http::URI("https://s3.amazonaws.com/bucket/key"), Aws::Http::HttpMethod::HTTP_PUT, stream_factory);
    client->BuildHttpRequest(request, http_request);
    EXPECT_FALSE(DB::S3::isNativeConditionalRequest(*http_request));
}

TEST(IOTestAwsS3Client, NativeConditionalModeIsRederivedOnEverySdkAttempt)
{
    /// Drives real `DB::S3::Client::GetBucketVersioning` calls through a `RecordingClient`, which
    /// records `isNativeConditionalRequest` on every real `Client::BuildHttpRequest` call -- i.e. once
    /// per genuine SDK attempt, including the extra attempt a real SDK-level retry triggers.
    ///
    /// This does not separately drive a real 301 redirect: in the vendored SDK, `AttemptExhaustively`
    /// recreates the HTTP request unconditionally at the retry tail regardless of cause
    /// (`contrib/aws/src/aws-cpp-sdk-core/source/client/AWSClient.cpp:405`), and `BuildHttpRequest` runs
    /// at the top of the next `AttemptOneRequest` exactly as in the retry case (`AWSClient.cpp:564`) --
    /// a redirect only changes the URI passed into that same recreation, it is not a separate mechanism.
    /// `Client::doRequest`'s own manual redirect loop is even less in doubt: it re-enters `MakeRequest`
    /// wholesale, which calls `BuildHttpRequest` fresh by construction. So the retry case below already
    /// exercises the machinery a redirect would use.
    auto runOnce = [](const std::string & endpoint, bool native_conditional) -> std::vector<bool>
    {
        /// Note: `ASSERT_*` cannot be used in this lambda -- it returns `std::vector<bool>`, not
        /// `void`, and the macro expands to a bare `return;` on failure. `EXPECT_*` only records.
        auto client = makeRecordingClient(endpoint, /*max_retries=*/2, /*max_redirects=*/2);
        EXPECT_TRUE(client->supportsGcsNativeConditionalRequests());

        DB::S3::GetBucketVersioningRequest request;
        request.SetBucket("bucket");
        request.setNativeConditional(native_conditional);

        auto outcome = client->GetBucketVersioning(request);
        EXPECT_TRUE(outcome.IsSuccess());
        return client->observed_native_conditional;
    };

    {
        SCOPED_TRACE("ordinary request: one successful attempt, mode stays false");
        ScriptedResponseServer server({{Poco::Net::HTTPResponse::HTTP_OK, {}}});
        const auto observed = runOnce(server.getUrl(), /*native_conditional=*/false);
        ASSERT_EQ(observed.size(), 1u);
        EXPECT_FALSE(observed[0]);
    }

    {
        SCOPED_TRACE("native request through a genuine SDK-level retry: both attempts see the mode");
        ScriptedResponseServer server({
            {Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, {}},
            {Poco::Net::HTTPResponse::HTTP_OK, {}},
        });
        const auto observed = runOnce(server.getUrl(), /*native_conditional=*/true);
        /// The size assertion is load-bearing, not cosmetic: a 500 that was silently not retried (or a
        /// retry that reused a stale HTTP request) would leave a one-element vector, and an
        /// all-elements-true assertion alone would not catch that.
        ASSERT_EQ(observed.size(), 2u);
        EXPECT_TRUE(observed[0]);
        EXPECT_TRUE(observed[1]);
    }

    {
        SCOPED_TRACE("ordinary again: the mode does not leak from a previous native call");
        ScriptedResponseServer server({{Poco::Net::HTTPResponse::HTTP_OK, {}}});
        const auto observed = runOnce(server.getUrl(), /*native_conditional=*/false);
        ASSERT_EQ(observed.size(), 1u);
        EXPECT_FALSE(observed[0]);
    }
}

/// Response adaptation is gated on the same typed bit as the request side. Both HEADs below get an
/// identical response — a GCS-style one carrying both an ETag and a generation — so the only thing
/// that can produce different results is the mode.
TEST(IOTestAwsS3Client, ResponseGenerationAndMetadataAdaptedOnlyWhenMarked)
{
    const std::vector<ScriptedResponse> script{{Poco::Net::HTTPResponse::HTTP_OK, {
        {"ETag", "\"6654c734ccab8f440ff0825eb443dc7f\""},
        {"x-goog-generation", "1783078552147137"},
        {"x-goog-meta-cas-envelope", "v1"},
    }}};

    auto headOnce = [&script](bool native_conditional)
    {
        ScriptedResponseServer server(script);
        auto client = makeRecordingClient(server.getUrl(), /*max_retries=*/0, /*max_redirects=*/0);

        DB::S3::HeadObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        request.setNativeConditional(native_conditional);
        return client->HeadObject(request);
    };

    {
        SCOPED_TRACE("marked: the generation becomes the SDK-visible ETag and the metadata crosses over");
        auto outcome = headOnce(/*native_conditional=*/true);
        ASSERT_TRUE(outcome.IsSuccess());
        EXPECT_EQ(outcome.GetResult().GetETag(), "\"1783078552147137\"");
        const auto & metadata = outcome.GetResult().GetMetadata();
        ASSERT_TRUE(metadata.contains("cas-envelope"));
        EXPECT_EQ(metadata.at("cas-envelope"), "v1");
    }

    {
        SCOPED_TRACE("Default: the upstream ETag survives even though a generation is present");
        auto outcome = headOnce(/*native_conditional=*/false);
        ASSERT_TRUE(outcome.IsSuccess());
        EXPECT_EQ(outcome.GetResult().GetETag(), "\"6654c734ccab8f440ff0825eb443dc7f\"");
        EXPECT_FALSE(outcome.GetResult().GetMetadata().contains("cas-envelope"));
    }
}

/// Pins the ordinary S3-interoperability HMAC path (`http_client` left empty, exactly as configured for
/// a plain S3-compatible disk): it never becomes a `PocoHTTPClientGCPOAuth` or `PocoHTTPClientGCSHMAC`,
/// so none of the GCS request/response adaptation in `GCSConditionalDialect.cpp` is even reachable from
/// it, CAS or no CAS. `native_conditional=true` is still passed on the HEAD below to prove that even a
/// caller that mismarks a request cannot make this client honour it -- `supportsGcsNativeConditionalRequests`
/// already gates the request-side bit off (see `NativeConditionalModeRequiresExplicitGcsHttpClient` /
/// `NativeConditionalStaysFalseThroughBuildHttpRequestOnNonGcsClient`), and this closes the matching gap
/// on the response side: this would fail if `applyGcsConditionalDialectToResponse` were ever hoisted out
/// of the two GCS subclasses into the shared `PocoHTTPClient::makeRequestInternal`.
TEST(IOTestAwsS3Client, OrdinaryHmacClientNeverAppliesGcsAdaptation)
{
    const std::vector<ScriptedResponse> script{{Poco::Net::HTTPResponse::HTTP_OK, {
        {"ETag", "\"deadbeefcafebabe0000000000000001\""},
        {"x-goog-generation", "1234567890123456"},
        {"x-goog-meta-cas-envelope", "v1"},
    }}};
    ScriptedResponseServer server(script);
    auto client = makeRecordingClient(server.getUrl(), /*max_retries=*/0, /*max_redirects=*/0, /*http_client=*/"");

    DB::S3::HeadObjectRequest request;
    request.SetBucket("bucket");
    request.SetKey("key");
    request.setNativeConditional(true);
    auto outcome = client->HeadObject(request);

    ASSERT_TRUE(outcome.IsSuccess());
    EXPECT_EQ(outcome.GetResult().GetETag(), "\"deadbeefcafebabe0000000000000001\"");
    EXPECT_FALSE(outcome.GetResult().GetMetadata().contains("cas-envelope"));

    ASSERT_EQ(client->observed_native_conditional.size(), 1u);
    EXPECT_FALSE(client->observed_native_conditional[0]);
}

/// Drives real `PutObject`, `CopyObject`, `DeleteObject`, and batch `DeleteObjects` requests through the
/// same ordinary (non-GCS) HMAC client and inspects the literal wire headers. Every assertion here is
/// falsifiable by a concrete regression: `EXPECT_TRUE(... has ...)` on an `x-amz-*` name fails if that
/// header were ever renamed or dropped (e.g. by widening the GOOG4 allowlist's reach, or applying
/// `renameToGoogPrefix` outside the two GCS clients), and the SigV4 `EXPECT_TRUE(starts_with(...))`
/// checks fail if Bearer or GOOG4 authentication ever became reachable from a client with no
/// `http_client` configured.
TEST(IOTestAwsS3Client, OrdinaryHmacRequestsKeepUpstreamHeadersAndAuth)
{
    ScriptedResponseServer server({{Poco::Net::HTTPResponse::HTTP_OK, {}}});
    auto client = makeRecordingClient(server.getUrl(), /*max_retries=*/0, /*max_redirects=*/0, /*http_client=*/"");
    const auto & captured = server.getCapturedRequests();

    {
        SCOPED_TRACE("PUT with x-amz-meta-*");
        DB::S3::PutObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        request.AddMetadata("cas-envelope", "v1");
        /// `SetContentLength` explicitly, matching every production caller (e.g. `copyS3File.cpp`'s
        /// `fillPutRequest`) for stylistic consistency -- the SDK computes it from the body itself when
        /// omitted (`ExtendedRequest::IsStreaming` is always `false`, so the chunked path never engages),
        /// so this call is not load-bearing for the keep-alive corruption below. That corruption's sole
        /// cause is `ScriptedResponseServer` not draining the request body before responding; see the
        /// fix in `ScriptedResponseServer::Handler::handleRequest`.
        request.SetContentLength(7);
        request.SetBody(Aws::MakeShared<std::stringstream>("gtest", "payload"));
        client->PutObject(request);

        ASSERT_EQ(captured.size(), 1u);
        EXPECT_TRUE(captured[0].headers.has("x-amz-meta-cas-envelope"));
        EXPECT_FALSE(captured[0].headers.has("x-goog-meta-cas-envelope"));
        EXPECT_TRUE(captured[0].headers.get("authorization", "").starts_with("AWS4-HMAC-SHA256"));
        /// An earlier version of this assertion claimed the SDK's default checksum is always present;
        /// a real run showed that is wrong. The conclusion below is right, but the reason is NOT that an
        /// unset algorithm leaves nothing to compute: a bare `PutObjectRequest` does report a default
        /// algorithm name from `GetChecksumAlgorithmName()`. What actually suppresses the header is that
        /// `PocoHTTPClientConfiguration` sets `requestChecksumCalculation` to `WHEN_REQUIRED`
        /// unconditionally, which leaves the SDK's checksum interceptor gating purely on
        /// `RequestChecksumRequired()` -- and this fork overrides that to `is_s3express_bucket`.
        /// Independently, `setChecksumAlgorithm` is only ever reached from `setIsS3ExpressBucket`.
        /// So the fact pinned here is that an ordinary HMAC client injects no checksum, and it is NOT a
        /// test of `WriteBufferFromS3`'s S3Express-only checksum policy: this test never goes through
        /// `WriteBufferFromS3` at all, so widening that policy tomorrow would not be caught here.
        EXPECT_FALSE(captured[0].headers.has("x-amz-checksum-crc32"));
        EXPECT_FALSE(captured[0].headers.has("x-amz-sdk-checksum-algorithm"));
    }

    {
        SCOPED_TRACE("If-None-Match with a non-star value: passes through unmolested");
        /// A non-star `If-None-Match` reaching `applyGcsConditionalDialectToRequest` aborts the process
        /// with `LOGICAL_ERROR` (see ops notes) -- but that function is never called for this client at
        /// all, so this is not the reachable case the death-test split exists for. `EXPECT_NO_THROW` is
        /// the correct assertion here precisely because the guard is structurally unreachable, which is
        /// exactly what this test is pinning.
        DB::S3::PutObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        request.SetIfNoneMatch("some-non-star-value");
        /// `SetContentLength` explicitly, matching every production caller (e.g. `copyS3File.cpp`'s
        /// `fillPutRequest`) for stylistic consistency -- the SDK computes it from the body itself when
        /// omitted (`ExtendedRequest::IsStreaming` is always `false`, so the chunked path never engages),
        /// so this call is not load-bearing for the keep-alive corruption below. That corruption's sole
        /// cause is `ScriptedResponseServer` not draining the request body before responding; see the
        /// fix in `ScriptedResponseServer::Handler::handleRequest`.
        request.SetContentLength(7);
        request.SetBody(Aws::MakeShared<std::stringstream>("gtest", "payload"));
        EXPECT_NO_THROW(client->PutObject(request));

        ASSERT_EQ(captured.size(), 2u);
        EXPECT_EQ(captured[1].headers.get("if-none-match", ""), "some-non-star-value");
        EXPECT_FALSE(captured[1].headers.has("x-goog-if-generation-match"));
    }

    {
        SCOPED_TRACE("CopyObject: existing targeted mappings are the AWS ones, no goog- rename");
        /// Also a negative control for `CopyObjectRequestGetRequestSpecificHeadersRenamesOnlyUnderGcsApiMode`
        /// below: this client's `api_mode` never becomes GCS (no `gcs_hmac`, no GCS-shaped endpoint, real
        /// credentials), so `CopyObjectRequest::GetRequestSpecificHeaders` must leave these headers alone.
        DB::S3::CopyObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("dest-key");
        request.SetCopySource("bucket/src-key");
        request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
        request.SetStorageClass(Aws::S3::Model::StorageClass::STANDARD);
        request.AddMetadata("cas-envelope", "v1");
        client->CopyObject(request);

        ASSERT_EQ(captured.size(), 3u);
        EXPECT_TRUE(captured[2].headers.has("x-amz-copy-source"));
        EXPECT_TRUE(captured[2].headers.has("x-amz-metadata-directive"));
        EXPECT_TRUE(captured[2].headers.has("x-amz-storage-class"));
        EXPECT_TRUE(captured[2].headers.has("x-amz-meta-cas-envelope"));
        EXPECT_FALSE(captured[2].headers.has("x-goog-copy-source"));
        EXPECT_FALSE(captured[2].headers.has("x-goog-metadata-directive"));
        EXPECT_FALSE(captured[2].headers.has("x-goog-storage-class"));
    }

    {
        SCOPED_TRACE("DELETE: single object");
        DB::S3::DeleteObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("key");
        client->DeleteObject(request);

        ASSERT_EQ(captured.size(), 4u);
        EXPECT_EQ(captured[3].method, "DELETE");
        EXPECT_TRUE(captured[3].headers.get("authorization", "").starts_with("AWS4-HMAC-SHA256"));
    }

    {
        SCOPED_TRACE("batch DeleteObjects");
        DB::S3::DeleteObjectsRequest request;
        request.SetBucket("bucket");
        Aws::S3::Model::ObjectIdentifier obj1;
        obj1.SetKey("key1");
        Aws::S3::Model::ObjectIdentifier obj2;
        obj2.SetKey("key2");
        std::vector<Aws::S3::Model::ObjectIdentifier> objects{obj1, obj2}; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        Aws::S3::Model::Delete del;
        del.SetObjects(objects);
        del.SetQuiet(true);
        request.SetDelete(del);
        client->DeleteObjects(request);

        ASSERT_EQ(captured.size(), 5u);
        EXPECT_EQ(captured[4].method, "POST");
        EXPECT_TRUE(captured[4].headers.get("authorization", "").starts_with("AWS4-HMAC-SHA256"));
    }
}

/// The deferred allowlist gap from Task 4: `GcsHmacDefaultPutPassesTheAuthenticationAllowlist` exercised
/// only a small single-part PUT. Multipart is a distinct request shape family (`CreateMultipartUpload`,
/// `UploadPart`, `CompleteMultipartUpload`), each with its own header set, and none of them were driven
/// through the GOOG4 preparation before. Each `EXPECT_NO_THROW` below fails if the allowlist regresses
/// to reject a header this shape actually carries (`BAD_ARGUMENTS` from `prepareGcsRequestForGoog4Authentication`);
/// the header assertions fail if a `Rename` mapping stops firing and a stale `x-amz-*` header reaches the wire.
/// `Default` mode is used throughout, so `applyGcsConditionalDialectToRequest` (with its `LOGICAL_ERROR`
/// guards) is never invoked here -- no death-test split is needed for this test.
TEST(IOTestAwsS3Client, GcsHmacDefaultMultipartPassesTheAuthenticationAllowlist)
{
    ScriptedResponseServer server({{Poco::Net::HTTPResponse::HTTP_OK, {}}});
    auto client = makeRecordingClient(server.getUrl(), /*max_retries=*/0, /*max_redirects=*/0, /*http_client=*/"gcs_hmac");
    const auto & captured = server.getCapturedRequests();

    {
        DB::S3::CreateMultipartUploadRequest create_request;
        create_request.SetBucket("bucket");
        create_request.SetKey("key");
        create_request.SetStorageClass(Aws::S3::Model::StorageClass::STANDARD);
        create_request.AddMetadata("cas-envelope", "v1");
        EXPECT_NO_THROW(client->CreateMultipartUpload(create_request));
    }

    {
        DB::S3::UploadPartRequest upload_part_request;
        upload_part_request.SetBucket("bucket");
        upload_part_request.SetKey("key");
        upload_part_request.SetUploadId("test-upload-id");
        upload_part_request.SetPartNumber(1);
        /// `SetContentLength` explicitly, matching `copyS3File.cpp`'s `makeUploadPartRequest` for
        /// stylistic consistency -- not load-bearing here (see the comment on the `PutObjectRequest`
        /// above). The real failure this test once hit -- `captured[2].method` reading back as
        /// `"part-bodyPOST"`, the following CompleteMultipartUpload's parse corrupted by this request's
        /// unread body on the shared keep-alive connection -- was caused solely by
        /// `ScriptedResponseServer` not draining the request body before responding.
        upload_part_request.SetContentLength(9);
        upload_part_request.SetBody(Aws::MakeShared<std::stringstream>("gtest", "part-body"));
        EXPECT_NO_THROW(client->UploadPart(upload_part_request));
    }

    {
        DB::S3::CompleteMultipartUploadRequest complete_request;
        complete_request.SetBucket("bucket");
        complete_request.SetKey("key");
        complete_request.SetUploadId("test-upload-id");
        Aws::S3::Model::CompletedMultipartUpload completed;
        Aws::S3::Model::CompletedPart part;
        part.WithPartNumber(1).WithETag("\"etag1\"");
        completed.AddParts(part);
        complete_request.SetMultipartUpload(completed);
        /// Deliberately not marked NativeConditional and no If-Match/If-None-Match is set, so this POST
        /// (uploadId, no partNumber) never reaches `applyGcsConditionalDialectToRequest`'s conditional
        /// CompleteMultipartUpload guard -- see the file-level comment above.
        EXPECT_NO_THROW(client->CompleteMultipartUpload(complete_request));
    }

    ASSERT_EQ(captured.size(), 3u);

    SCOPED_TRACE("CreateMultipartUpload: storage class and metadata renamed, nothing x-amz- left");
    EXPECT_TRUE(captured[0].headers.has("x-goog-storage-class"));
    EXPECT_TRUE(captured[0].headers.has("x-goog-meta-cas-envelope"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-storage-class"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-meta-cas-envelope"));

    EXPECT_EQ(captured[2].method, "POST");
}

/// The second deferred shape: CopyObject through the GOOG4 preparation (`prepareGcsRequestForGoog4Authentication`
/// in `GCSConditionalDialect.cpp`), not previously exercised at all. This is a DIFFERENT mechanism from
/// the pre-existing, non-CAS `CopyObjectRequest::GetRequestSpecificHeaders` rename in `Requests.cpp`,
/// which is gated on the request's `api_mode` field, not on `http_client`. The mock endpoint here
/// (`127.0.0.1:PORT`) has no GCS-recognisable substring, so `Client`'s constructor never sets
/// `api_mode` to `GCS` even for this `gcs_hmac` client (that requires `provider_type == GCS` first,
/// which is endpoint-string-only) -- the `x-goog-copy-source` etc. observed below come entirely from
/// the GOOG4 preparation step, not from `Requests.cpp`. See
/// `CopyObjectRequestGetRequestSpecificHeadersRenamesOnlyUnderGcsApiMode` for that separate mechanism,
/// tested directly against the request object with no client or server involved.
TEST(IOTestAwsS3Client, GcsHmacDefaultCopyObjectPassesTheAuthenticationAllowlist)
{
    ScriptedResponseServer server({{Poco::Net::HTTPResponse::HTTP_OK, {}}});
    auto client = makeRecordingClient(server.getUrl(), /*max_retries=*/0, /*max_redirects=*/0, /*http_client=*/"gcs_hmac");

    DB::S3::CopyObjectRequest request;
    request.SetBucket("bucket");
    request.SetKey("dest-key");
    request.SetCopySource("bucket/src-key");
    request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
    request.SetStorageClass(Aws::S3::Model::StorageClass::STANDARD);
    request.AddMetadata("cas-envelope", "v1");

    EXPECT_NO_THROW(client->CopyObject(request));

    const auto & captured = server.getCapturedRequests();
    ASSERT_EQ(captured.size(), 1u);
    EXPECT_TRUE(captured[0].headers.has("x-goog-copy-source"));
    EXPECT_TRUE(captured[0].headers.has("x-goog-metadata-directive"));
    EXPECT_TRUE(captured[0].headers.has("x-goog-storage-class"));
    EXPECT_TRUE(captured[0].headers.has("x-goog-meta-cas-envelope"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-copy-source"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-metadata-directive"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-storage-class"));
    EXPECT_FALSE(captured[0].headers.has("x-amz-meta-cas-envelope"));
}

/// The pre-existing (pre-CAS), non-GOOG4 CopyObject header mapping: `CopyObjectRequest::GetRequestSpecificHeaders`
/// in `Requests.cpp` renames `x-amz-copy-source`/`x-amz-metadata-directive`/`x-amz-storage-class`/
/// `x-amz-meta-*` to their `x-goog-` counterparts, gated purely on the request's `api_mode` field (set
/// by `Client::doRequest` from the CLIENT's own `api_mode`, itself derived from `deduceProviderType`
/// matching the endpoint string against `storage.googleapis.com` -- see the file-level comment on
/// `GcsHmacDefaultCopyObjectPassesTheAuthenticationAllowlist` above). Driving this end-to-end through a
/// real `Client` would need a live server reachable AT a `storage.googleapis.com`-shaped hostname, which
/// this test harness cannot provide cheaply (no local DNS/network alias for that name; see the
/// integration test's own note on the same gap). `setApiMode` is public on `ExtendedRequest` for
/// exactly this reason: it lets a unit test set the one bit `GetRequestSpecificHeaders` reads without
/// needing a `Client` or any I/O at all.
TEST(IOTestAwsS3Client, CopyObjectRequestGetRequestSpecificHeadersRenamesOnlyUnderGcsApiMode)
{
    auto makeRequest = []
    {
        DB::S3::CopyObjectRequest request;
        request.SetBucket("bucket");
        request.SetKey("dest-key");
        request.SetCopySource("bucket/src-key");
        request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
        request.SetStorageClass(Aws::S3::Model::StorageClass::STANDARD);
        request.AddMetadata("cas-envelope", "v1");
        return request;
    };

    {
        SCOPED_TRACE("api_mode left at its default (AWS): headers are untouched");
        auto request = makeRequest();
        const auto headers = request.GetRequestSpecificHeaders();
        EXPECT_TRUE(headers.contains("x-amz-copy-source"));
        EXPECT_TRUE(headers.contains("x-amz-metadata-directive"));
        EXPECT_TRUE(headers.contains("x-amz-storage-class"));
        EXPECT_TRUE(headers.contains("x-amz-meta-cas-envelope"));
        EXPECT_FALSE(headers.contains("x-goog-copy-source"));
    }

    {
        SCOPED_TRACE("api_mode explicitly set to GCS: every mapped header is renamed");
        auto request = makeRequest();
        request.setApiMode(DB::S3::ApiMode::GCS);
        const auto headers = request.GetRequestSpecificHeaders();
        EXPECT_TRUE(headers.contains("x-goog-copy-source"));
        EXPECT_TRUE(headers.contains("x-goog-metadata-directive"));
        EXPECT_TRUE(headers.contains("x-goog-storage-class"));
        EXPECT_TRUE(headers.contains("x-goog-meta-cas-envelope"));
        EXPECT_FALSE(headers.contains("x-amz-copy-source"));
        EXPECT_FALSE(headers.contains("x-amz-metadata-directive"));
        EXPECT_FALSE(headers.contains("x-amz-storage-class"));
        EXPECT_FALSE(headers.contains("x-amz-meta-cas-envelope"));
    }
}

TEST(IOTestAwsS3Client, WrongSigningRegionBadRequest)
{
    {
        SCOPED_TRACE("400 with non-empty x-amz-bucket-region");
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        response.set("x-amz-bucket-region", "us-west-2");
        EXPECT_TRUE(DB::S3::isS3WrongSigningRegionBadRequest(400, response));
    }
    {
        SCOPED_TRACE("2xx with header");
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.set("x-amz-bucket-region", "eu-central-1");
        EXPECT_FALSE(DB::S3::isS3WrongSigningRegionBadRequest(200, response));
    }
    {
        SCOPED_TRACE("400 without header");
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        EXPECT_FALSE(DB::S3::isS3WrongSigningRegionBadRequest(400, response));
    }
    {
        SCOPED_TRACE("400 with empty x-amz-bucket-region");
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        response.set("x-amz-bucket-region", "");
        EXPECT_FALSE(DB::S3::isS3WrongSigningRegionBadRequest(400, response));
    }
    {
        SCOPED_TRACE("404 with header");
        Poco::Net::HTTPResponse response;
        response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        response.set("x-amz-bucket-region", "ap-south-1");
        EXPECT_FALSE(DB::S3::isS3WrongSigningRegionBadRequest(404, response));
    }
}

#endif
