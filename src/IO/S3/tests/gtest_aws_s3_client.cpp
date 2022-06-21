#include <gtest/gtest.h>

#include <Common/config.h>


#if USE_AWS_S3

#include <memory>
#include <ostream>

#include <boost/algorithm/string.hpp>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/URI.h>
#include <aws/s3/S3Client.h>

#include <Common/RemoteHostFilter.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3Settings.h>

#include "TestPocoHTTPServer.h"


class NoRetryStrategy : public Aws::Client::StandardRetryStrategy
{
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> &, long /* NOLINT */) const override { return false; }

public:
    ~NoRetryStrategy() override = default;
};


TEST(IOTestAwsS3Client, AppendExtraSSECHeaders)
{
    /// See https://github.com/ClickHouse/ClickHouse/pull/19748

    class MyRequestHandler : public Poco::Net::HTTPRequestHandler
    {
    public:
        void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            std::ostream & out = response.send();
            for (const auto & [header_name, header_value] : request)
            {
                if (boost::algorithm::starts_with(header_name, "x-amz-server-side-encryption-customer-"))
                {
                    out << header_name << ": " << header_value << "\n";
                }
                else if (header_name == "authorization")
                {
                    std::vector<String> parts;
                    boost::split(parts, header_value, [](char c){ return c == ' '; });
                    for (const auto & part : parts)
                    {
                        if (boost::algorithm::starts_with(part, "SignedHeaders="))
                            out << header_name << ": ... " << part << " ...\n";
                    }
                }
            }
            out.flush();
        }
    };

    TestPocoHTTPServer<MyRequestHandler> http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    DB::S3::URI uri(Poco::URI(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/test.txt"));
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    String version_id;
    UInt64 max_single_read_retries = 1;
    bool enable_s3_requests_logging = false;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging
    );

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<NoRetryStrategy>();

    String server_side_encryption_customer_key_base64 = "Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=";

    DB::HeaderCollection headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    std::shared_ptr<Aws::S3::S3Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        headers,
        use_environment_credentials,
        use_insecure_imds_request
    );

    ASSERT_TRUE(client);

    DB::ReadSettings read_settings;
    DB::ReadBufferFromS3 read_buffer(
        client,
        uri.bucket,
        uri.key,
        version_id,
        max_single_read_retries,
        read_settings
    );

    String content;
    DB::readStringUntilEOF(content, read_buffer);
    EXPECT_EQ(content, "authorization: ... SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-type;host;x-amz-api-version;x-amz-content-sha256;x-amz-date, ...\nx-amz-server-side-encryption-customer-algorithm: AES256\nx-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\nx-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

TEST(IOTestAwsS3Client, SimpleRead)
{
    class MyRequestHandler : public Poco::Net::HTTPRequestHandler
    {
    public:
        void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            std::ostream & out = response.send();
            out << "Hello, world!";
            out.flush();
        }
    };

    TestPocoHTTPServer<MyRequestHandler> http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    DB::S3::URI uri(Poco::URI(http.getUrl() + "/IOTestAwsS3ClientSimpleRead/test.txt"));
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    String version_id;
    UInt64 max_single_read_retries = 1;
    bool enable_s3_requests_logging = false;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging
    );

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<NoRetryStrategy>();

    String server_side_encryption_customer_key_base64;
    DB::HeaderCollection headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    std::shared_ptr<Aws::S3::S3Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        headers,
        use_environment_credentials,
        use_insecure_imds_request
    );

    ASSERT_TRUE(client);

    DB::ReadSettings read_settings;
    DB::ReadBufferFromS3 read_buffer(
        client,
        uri.bucket,
        uri.key,
        version_id,
        max_single_read_retries,
        read_settings
    );

    EXPECT_TRUE(DB::checkString("Hello", read_buffer));
    EXPECT_TRUE(DB::checkChar(',', read_buffer));
    EXPECT_TRUE(DB::checkChar(' ', read_buffer));
    EXPECT_TRUE(DB::checkString("world", read_buffer));
    EXPECT_TRUE(DB::checkChar('!', read_buffer));
}

TEST(IOTestAwsS3Client, ReadErrorInfo)
{
    /// See https://github.com/ClickHouse/aws-sdk-cpp/pull/4

    class MyRequestHandler : public Poco::Net::HTTPRequestHandler
    {
    public:
        void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
            response.setContentType("application/xml");
            std::ostream & out = response.send();
            out << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>R5DECF7W791K495S</RequestId><HostId>Wm1fgZx30IQ6guLjc2Y606rcxtadaZMRbq8ORo++P70s2FUU50KrKl5kqZ/rebeiKDdnHhzAKfo=</HostId></Error>";
            out.flush();
        }
    };

    TestPocoHTTPServer<MyRequestHandler> http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    DB::S3::URI uri(Poco::URI(http.getUrl() + "/IOTestAwsS3ClientReadErrorInfo/test.txt"));
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    String version_id;
    UInt64 max_single_read_retries = 1;
    bool enable_s3_requests_logging = false;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging
    );

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<NoRetryStrategy>();

    String server_side_encryption_customer_key_base64;
    DB::HeaderCollection headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    std::shared_ptr<Aws::S3::S3Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        headers,
        use_environment_credentials,
        use_insecure_imds_request
    );

    ASSERT_TRUE(client);

    DB::ReadSettings read_settings;
    DB::ReadBufferFromS3 read_buffer(
        client,
        uri.bucket,
        uri.key,
        version_id,
        max_single_read_retries,
        read_settings
    );

    EXPECT_THROW({
        try {
            char c;
            read_buffer.peek(c);
        }
        catch (const DB::Exception & e)
        {
            /// Without ClickHouse/aws-sdk-cpp#4 we get "No response body." here.
            ASSERT_EQ(String(e.what()), "Access Denied");
            throw;
        }
    }, DB::Exception);
}

TEST(IOTestAwsS3Client, ReadTooManyRequestsException)
{
    /// See https://github.com/ClickHouse/aws-sdk-cpp/pull/9

    class MyRequestHandler : public Poco::Net::HTTPRequestHandler
    {
    public:
        void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS);
            response.setContentType("application/xml");
            std::ostream & out = response.send();
            out << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>TooManyRequestsException</Code><Message>Please reduce your request rate.</Message><RequestId>txfbd566d03042474888193-00608d7538</RequestId></Error>";
            out.flush();
        }
    };

    TestPocoHTTPServer<MyRequestHandler> http;

    DB::RemoteHostFilter remote_host_filter;
    unsigned int s3_max_redirects = 100;
    DB::S3::URI uri(Poco::URI(http.getUrl() + "/IOTestAwsS3ClientReadTooManyRequestsException/test.txt"));
    String access_key_id = "ACCESS_KEY_ID";
    String secret_access_key = "SECRET_ACCESS_KEY";
    String region = "us-east-1";
    String version_id;
    UInt64 max_single_read_retries = 1;
    bool enable_s3_requests_logging = false;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        remote_host_filter,
        s3_max_redirects,
        enable_s3_requests_logging
    );

    client_configuration.endpointOverride = uri.endpoint;
    client_configuration.retryStrategy = std::make_shared<NoRetryStrategy>();

    String server_side_encryption_customer_key_base64;
    DB::HeaderCollection headers;
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;

    std::shared_ptr<Aws::S3::S3Client> client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        access_key_id,
        secret_access_key,
        server_side_encryption_customer_key_base64,
        headers,
        use_environment_credentials,
        use_insecure_imds_request
    );

    ASSERT_TRUE(client);

    DB::ReadSettings read_settings;
    DB::ReadBufferFromS3 read_buffer(
        client,
        uri.bucket,
        uri.key,
        version_id,
        max_single_read_retries,
        read_settings
    );

    EXPECT_THROW({
        try {
            char c;
            read_buffer.peek(c);
        }
        catch (const DB::Exception & e)
        {
            /// Without ClickHouse/aws-sdk-cpp#9 we get:
            /// "Unable to parse ExceptionName: TooManyRequestsException Message: Please reduce your request rate."
            ASSERT_EQ(String(e.what()), "Please reduce your request rate.");
            throw;
        }
    }, DB::Exception);
}

#endif
