#include <gtest/gtest.h>

#include "IO/S3/Credentials.h"
#include "config.h"


#if USE_AWS_S3

#include <memory>

#include <boost/algorithm/string.hpp>

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
#include <Storages/StorageS3Settings.h>
#include <Poco/Util/ServerApplication.h>

#include "TestPocoHTTPServer.h"

/*
 * When all tests are executed together, `Context::getGlobalContextInstance()` is not null. Global context is used by
 * ProxyResolvers to get proxy configuration (used by S3 clients). If global context does not have a valid ConfigRef, it relies on
 * Poco::Util::Application::instance() to grab the config. However, at this point, the application is not yet initialized and
 * `Poco::Util::Application::instance()` returns nullptr. This causes the test to fail. To fix this, we create a dummy application that takes
 * care of initialization.
 * */
[[maybe_unused]] static Poco::Util::ServerApplication app;


class NoRetryStrategy : public Aws::Client::StandardRetryStrategy
{
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &, long /* NOLINT */) const override { return false; }

public:
    ~NoRetryStrategy() override = default;
};

String getSSEAndSignedHeaders(const Poco::Net::MessageHeader & message_header)
{
    String content;
    for (const auto & [header_name, header_value] : message_header)
    {
        if (boost::algorithm::starts_with(header_name, "x-amz-server-side-encryption"))
        {
            content += header_name + ": " + header_value + "\n";
        }
        else if (header_name == "authorization")
        {
            std::vector<String> parts;
            boost::split(parts, header_value, [](char c){ return c == ' '; });
            for (const auto & part : parts)
            {
                if (boost::algorithm::starts_with(part, "SignedHeaders="))
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
    DB::S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = max_single_read_retries;
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

    DB::S3Settings::RequestSettings request_settings;
    request_settings.max_unexpected_write_error_retries = max_unexpected_write_error_retries;
    DB::WriteBufferFromS3 write_buffer(
        client,
        client,
        uri.bucket,
        uri.key,
        DBMS_DEFAULT_BUFFER_SIZE,
        request_settings
    );

    write_buffer.write('\0'); // doesn't matter what we write here, just needs to be something
    write_buffer.finalize();
}

using RequestFn = std::function<void(std::shared_ptr<const DB::S3::Client>, const DB::S3::URI &)>;

void testServerSideEncryption(
    RequestFn do_request,
    String server_side_encryption_customer_key_base64,
    DB::S3::ServerSideEncryptionKMSConfig sse_kms_config,
    String expected_headers)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    DB::S3::URI uri(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/test.txt");
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    bool enable_s3_requests_logging = false;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        /* get_request_throttler = */ {},
        /* put_request_throttler = */ {},
        uri.uri.getScheme()
    );

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<NoRetryStrategy>();

    DB::HTTPHeaderEntries headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        sse_kms_config,
        headers,
        DB::S3::CredentialsConfiguration
        {
            .use_environment_credentials = use_environment_credentials,
            .use_insecure_imds_request = use_insecure_imds_request
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
        "Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=",
        {},
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
        "content-type;"
        "host;"
        "x-amz-api-version;"
        "x-amz-content-sha256;"
        "x-amz-date, ...\n"
        "x-amz-server-side-encryption-customer-algorithm: AES256\n"
        "x-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\n"
        "x-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

TEST(IOTestAwsS3Client, AppendExtraSSECHeadersWrite)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748
    testServerSideEncryption(
        doWriteRequest,
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
        "x-amz-date, ...\n"
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
        "",
        sse_kms_config,
        "authorization: ... SignedHeaders="
        "amz-sdk-invocation-id;"
        "amz-sdk-request;"
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

#endif
