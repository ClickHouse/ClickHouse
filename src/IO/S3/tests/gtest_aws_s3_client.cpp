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

#endif
