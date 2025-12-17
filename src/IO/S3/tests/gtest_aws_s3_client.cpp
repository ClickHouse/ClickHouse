#include <gtest/gtest.h>

#include <IO/S3/Credentials.h>
#include "config.h"


#if USE_AWS_S3

#include <memory>

#include <boost/algorithm/string/split.hpp>

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


String getSSEAndSignedHeaders(const Poco::Net::MessageHeader & message_header)
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

void doReadRequest(std::shared_ptr<const DB::S3::Client> client, const DB::S3::URI & uri)
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

void doWriteRequest(std::shared_ptr<const DB::S3::Client> client, const DB::S3::URI & uri)
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

void testServerSideEncryption(
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

void validateAssumeRoleQueryParams(const Poco::URI::QueryParameters query_params, const std::string_view expected_role_arn, const std::string_view expected_role_session_name)
{
    for (const auto & [param, value] : query_params)
    {
        if (param == "Action")
            ASSERT_EQ(value, "AssumeRole");
        else if (param == "RoleArn")
            ASSERT_EQ(value, expected_role_arn);
        else if (param == "RoleSessionName")
            ASSERT_EQ(value, expected_role_session_name);
    }
}

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


    const auto read_from_s3 = [&](const std::string & role_arn, const std::string & role_session_name)
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
}

#endif
