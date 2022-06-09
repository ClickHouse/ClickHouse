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

#include <Common/RemoteHostFilter.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/S3/ClientFactory.h>
#include <IO/WriteBufferFromString.h>
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
            DB::WriteBufferFromOwnString tmp;
            for (const auto & [header_name, header_value] : request)
            {
                if (boost::algorithm::starts_with(header_name, "x-amz-server-side-encryption-customer-")
                    || boost::algorithm::starts_with(header_name, "x-amz-copy-source-server-side-encryption-customer-"))
                {
                    tmp << header_name << ": " << header_value << "\n";
                }
                else if (header_name == "authorization")
                {
                    std::vector<String> parts;
                    boost::split(parts, header_value, [](char c){ return c == ' '; });
                    for (const auto & part : parts)
                    {
                        if (boost::algorithm::starts_with(part, "SignedHeaders="))
                            tmp << header_name << ": ... " << part << " ...\n";
                    }
                }
            }
            std::ostream & out = response.send();
            if (Poco::URI(request.getURI()).getPath() == "/IOTestAwsS3ClientAppendExtraHeaders/TheKey")
            {
                out << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult><ETag>" << tmp.str() << "</ETag><LastModified>timestamp</LastModified><ChecksumCRC32>string</ChecksumCRC32><ChecksumCRC32C>string</ChecksumCRC32C><ChecksumSHA1>string</ChecksumSHA1><ChecksumSHA256>string</ChecksumSHA256></CopyObjectResult>";
            }
            else
            {
                out << tmp.str();
            }
            out.flush();
        }
    };

    TestPocoHTTPServer<MyRequestHandler> http;

    DB::RemoteHostFilter remote_host_filter;
    DB::S3::URI uri(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/test.txt");
    DB::S3::URI result_uri(http.getUrl() + "/IOTestAwsS3ClientAppendExtraHeaders/last");
    UInt64 max_single_read_retries = 1;
    String version_id;

    auto client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(remote_host_filter);
    client_configuration.setRetryStrategy(std::make_shared<NoRetryStrategy>());
    client_configuration.setCredentials("ACCESS_KEY_ID", "SECRET_ACCESS_KEY");
    client_configuration.setRegionOverride("us-east-1");
    client_configuration.setServerSideEncryptionCustomerKeyBase64("Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=");

    std::shared_ptr<DB::S3::Client> client = DB::S3::ClientFactory::instance().create(client_configuration, uri);

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
    EXPECT_EQ(content, "authorization: ... SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-type;host;x-amz-api-version;x-amz-content-sha256;x-amz-date;x-amz-server-side-encryption-customer-algorithm;x-amz-server-side-encryption-customer-key;x-amz-server-side-encryption-customer-key-md5, ...\nx-amz-server-side-encryption-customer-algorithm: AES256\nx-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\nx-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");

    Aws::S3::Model::CopyObjectRequest request;
    request.SetBucket("IOTestAwsS3ClientAppendExtraHeaders");
    request.SetCopySource("TheCopySource");
    request.SetKey("TheKey");
    auto result = client->copyObject(request); /// For `CopyObject` `S3::Client` shall send two keys.

    EXPECT_TRUE(result.IsSuccess());
    EXPECT_EQ(result.GetResult().GetCopyObjectResultDetails().GetETag(), "authorization: ... SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;host;x-amz-api-version;x-amz-content-sha256;x-amz-copy-source;x-amz-copy-source-server-side-encryption-customer-algorithm;x-amz-copy-source-server-side-encryption-customer-key;x-amz-copy-source-server-side-encryption-customer-key-md5;x-amz-date;x-amz-server-side-encryption-customer-algorithm;x-amz-server-side-encryption-customer-key;x-amz-server-side-encryption-customer-key-md5, ...\nx-amz-copy-source-server-side-encryption-customer-algorithm: AES256\nx-amz-copy-source-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\nx-amz-copy-source-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\nx-amz-server-side-encryption-customer-algorithm: AES256\nx-amz-server-side-encryption-customer-key: Kv/gDqdWVGIT4iDqg+btQvV3lc1idlm4WI+MMOyHOAw=\nx-amz-server-side-encryption-customer-key-md5: fMNuOw6OLU5GG2vc6RTA+g==\n");
}

#endif
