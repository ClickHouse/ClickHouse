#include <gtest/gtest.h>

#include <IO/S3/Credentials.h>
#include "config.h"


#if USE_AWS_S3

#include <cstdlib>
#include <memory>
#include <string>

#include <base/scope_guard.h>

#include <boost/algorithm/string/split.hpp>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/URI.h>

#include <Common/RemoteHostFilter.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3Settings.h>
#include <Poco/Util/ServerApplication.h>

#include <IO/S3/tests/TestPocoHTTPServer.h>

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
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
            .forbid_implicit_credentials = false,
        }
    );

    ASSERT_TRUE(client);

    do_request(client, uri);
    String content = getSSEAndSignedHeaders(http.getLastRequestHeader());
    EXPECT_EQ(content, expected_headers);
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
        DB::S3::CredentialsConfiguration{.forbid_implicit_credentials = false});

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
                .sts_endpoint_override = sts_http.getUrl(),
                .forbid_implicit_credentials = false,
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

namespace DB::ErrorCodes
{
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

namespace
{

/// Mock S3 endpoint that serves a fixed body and a fixed ETag for every GET, simulating an object
/// whose current generation (ETag) differs from the one captured at listing time.
class FixedETagHandler : public Poco::Net::HTTPRequestHandler
{
public:
    FixedETagHandler(std::string body_, std::string etag_) : body(std::move(body_)), etag(std::move(etag_)) {}
    void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
    {
        response.set("ETag", etag);
        response.setContentType("application/octet-stream");
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        auto & os = response.send();
        os << body;
        os.flush();
    }
private:
    std::string body;
    std::string etag;
};

class FixedETagHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    FixedETagHandlerFactory(std::string body_, std::string etag_) : body(std::move(body_)), etag(std::move(etag_)) {}
    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new FixedETagHandler(body, etag);
    }
private:
    std::string body;
    std::string etag;
};

class FixedETagServer
{
public:
    FixedETagServer(std::string body, std::string etag)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , server(std::make_unique<Poco::Net::HTTPServer>(
              new FixedETagHandlerFactory(std::move(body), std::move(etag)),
              *server_socket, new Poco::Net::HTTPServerParams()))
    {
        server->start();
    }
    std::string getUrl() const { return "http://" + server_socket->address().toString(); }
private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

/// Like FixedETagServer, but honors the `If-Match` conditional header the way S3/GCS do: if the
/// request carries an `If-Match` that differs from the object's ETag, it responds 412 Precondition
/// Failed (before sending any body) instead of 200. Used to exercise the server-enforced overwrite
/// detection path, as opposed to FixedETagServer which ignores If-Match (the defense-in-depth path).
class IfMatchAwareHandler : public Poco::Net::HTTPRequestHandler
{
public:
    IfMatchAwareHandler(std::string body_, std::string etag_) : body(std::move(body_)), etag(std::move(etag_)) {}
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        const std::string if_match = request.get("If-Match", "");
        if (!if_match.empty() && if_match != etag)
        {
            const std::string error_body =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<Error><Code>PreconditionFailed</Code>"
                "<Message>At least one of the preconditions you specified did not hold.</Message>"
                "<Condition>If-Match</Condition></Error>";
            response.setContentType("application/xml");
            response.setStatus(Poco::Net::HTTPResponse::HTTP_PRECONDITION_FAILED);
            response.setContentLength(static_cast<std::streamsize>(error_body.size()));
            auto & os = response.send();
            os << error_body;
            os.flush();
            return;
        }
        response.set("ETag", etag);
        response.setContentType("application/octet-stream");
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        auto & os = response.send();
        os << body;
        os.flush();
    }
private:
    std::string body;
    std::string etag;
};

class IfMatchAwareHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    IfMatchAwareHandlerFactory(std::string body_, std::string etag_) : body(std::move(body_)), etag(std::move(etag_)) {}
    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new IfMatchAwareHandler(body, etag);
    }
private:
    std::string body;
    std::string etag;
};

class IfMatchAwareServer
{
public:
    IfMatchAwareServer(std::string body, std::string etag)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , server(std::make_unique<Poco::Net::HTTPServer>(
              new IfMatchAwareHandlerFactory(std::move(body), std::move(etag)),
              *server_socket, new Poco::Net::HTTPServerParams()))
    {
        server->start();
    }
    std::string getUrl() const { return "http://" + server_socket->address().toString(); }
private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

std::shared_ptr<DB::S3::Client> createTestS3Client(const DB::S3::URI & uri)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects= */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error= */ true,
        /* s3_slow_all_threads_after_retryable_error= */ true,
        /* enable_s3_requests_logging= */ false,
        /* for_disk_s3= */ false,
        /* opt_disk_name= */ {},
        /* request_throttler= */ {},
        uri.uri.getScheme());
    client_configuration.endpointOverride = uri.endpoint;

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };
    return DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
        /* server_side_encryption_customer_key_base64= */ "",
        {},
        {},
        DB::S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false});
}

DB::ReadBufferFromS3 makeReadBuffer(
    std::shared_ptr<const DB::S3::Client> client, const DB::S3::URI & uri, size_t file_size, const String & expected_etag,
    const String & version_id = "")
{
    DB::ReadSettings read_settings;
    DB::S3::S3RequestSettings request_settings;
    request_settings[DB::S3RequestSetting::max_single_read_retries] = 3;
    return DB::ReadBufferFromS3(
        std::move(client),
        uri.bucket,
        uri.key,
        version_id,
        request_settings,
        read_settings,
        /* use_external_buffer= */ false,
        /* offset= */ 0,
        /* read_until_position= */ 0,
        /* restricted_seek= */ false,
        /* file_size= */ std::optional<size_t>(file_size),
        /* credentials_refresh_callback= */ [] { return nullptr; },
        /* blob_storage_log= */ {},
        /* expected_etag= */ expected_etag);
}

}

/// The object's ETag changed between listing and reading (an in-place overwrite). The read must
/// fail with S3_OBJECT_CHANGED_DURING_READ instead of returning bytes from the wrong generation.
/// FixedETagServer ignores the If-Match header (returns 200 with the new ETag), so this exercises the
/// client-side response-ETag comparison - the defense-in-depth layer for backends that drop If-Match.
TEST(IOTestAwsS3Client, ReadDetectsObjectReplacedDuringRead)
{
    const std::string body(128, 'x');
    FixedETagServer server(body, "\"etag-after-overwrite\"");
    DB::S3::URI uri(server.getUrl() + "/bucket/live/data.parquet");
    auto client = createTestS3Client(uri);
    ASSERT_TRUE(client);

    auto read_buffer = makeReadBuffer(client, uri, body.size(), /* expected_etag= */ "\"etag-at-listing\"");

    String content;
    try
    {
        DB::readStringUntilEOF(content, read_buffer);
        FAIL() << "Expected S3_OBJECT_CHANGED_DURING_READ, but the read succeeded";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::S3_OBJECT_CHANGED_DURING_READ) << e.message();
    }
}

/// When the ETag matches the listed one (no concurrent overwrite) the read proceeds normally.
TEST(IOTestAwsS3Client, ReadAcceptsMatchingETag)
{
    const std::string body(128, 'x');
    FixedETagServer server(body, "\"etag-stable\"");
    DB::S3::URI uri(server.getUrl() + "/bucket/live/data.parquet");
    auto client = createTestS3Client(uri);
    ASSERT_TRUE(client);

    auto read_buffer = makeReadBuffer(client, uri, body.size(), /* expected_etag= */ "\"etag-stable\"");

    String content;
    DB::readStringUntilEOF(content, read_buffer);
    EXPECT_EQ(content, body);
}

/// For an explicit pinned-version read (?versionId=), the requested generation is immutable, so the
/// ETag check must be skipped even when the (current-version) expected ETag differs from the GET's.
/// Otherwise a versioned read would spuriously fail with S3_OBJECT_CHANGED_DURING_READ.
TEST(IOTestAwsS3Client, ReadSkipsETagCheckForVersionedRead)
{
    const std::string body(128, 'x');
    FixedETagServer server(body, "\"etag-of-requested-version\"");
    DB::S3::URI uri(server.getUrl() + "/bucket/live/data.parquet");
    auto client = createTestS3Client(uri);
    ASSERT_TRUE(client);

    /// expected_etag is the *current* version's etag (differs from the requested version's), but a
    /// version_id is pinned, so validation must be skipped and the read must succeed.
    auto read_buffer = makeReadBuffer(
        client, uri, body.size(), /* expected_etag= */ "\"etag-of-current-version\"", /* version_id= */ "some-version-id");

    String content;
    DB::readStringUntilEOF(content, read_buffer);
    EXPECT_EQ(content, body);
}

/// Server-enforced overwrite detection: the backend honors If-Match and answers 412 Precondition
/// Failed when the conditional GET's ETag no longer matches. The 412 must be mapped to
/// S3_OBJECT_CHANGED_DURING_READ rather than surfacing as a generic S3 error.
TEST(IOTestAwsS3Client, ReadMapsIfMatchPreconditionFailedToObjectChanged)
{
    const std::string body(128, 'x');
    IfMatchAwareServer server(body, "\"etag-after-overwrite\"");
    DB::S3::URI uri(server.getUrl() + "/bucket/live/data.parquet");
    auto client = createTestS3Client(uri);
    ASSERT_TRUE(client);

    /// expected_etag (the listed generation) differs from the server's current ETag, so the
    /// If-Match the read sends will not match and the server returns 412.
    auto read_buffer = makeReadBuffer(client, uri, body.size(), /* expected_etag= */ "\"etag-at-listing\"");

    String content;
    try
    {
        DB::readStringUntilEOF(content, read_buffer);
        FAIL() << "Expected S3_OBJECT_CHANGED_DURING_READ, but the read succeeded";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::S3_OBJECT_CHANGED_DURING_READ) << e.message();
    }
}

/// When the conditional GET's If-Match matches the current ETag, the If-Match-honoring server returns
/// the body normally - no spurious failure.
TEST(IOTestAwsS3Client, ReadWithMatchingIfMatchSucceeds)
{
    const std::string body(128, 'x');
    IfMatchAwareServer server(body, "\"etag-stable\"");
    DB::S3::URI uri(server.getUrl() + "/bucket/live/data.parquet");
    auto client = createTestS3Client(uri);
    ASSERT_TRUE(client);

    auto read_buffer = makeReadBuffer(client, uri, body.size(), /* expected_etag= */ "\"etag-stable\"");

    String content;
    DB::readStringUntilEOF(content, read_buffer);
    EXPECT_EQ(content, body);
}

#endif
