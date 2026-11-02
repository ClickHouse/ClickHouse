#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/Requests.h>
#include <IO/S3/tests/TestPocoHTTPServer.h>

#include <Common/ProxyConfiguration.h>
#include <Common/RemoteHostFilter.h>

#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <mutex>

namespace
{

std::string headerOrEmpty(const Aws::Http::HeaderValueCollection & headers, const std::string & name)
{
    auto it = headers.find(name);
    return it == headers.end() ? "" : it->second;
}

}

/// The names GCS spells differently are renamed, and the `x-amz-` spelling does not survive: GCS
/// rejects a request mixing the two prefixes.
TEST(GCSHeaderTranslation, RenamesTheTranslatedNames)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-copy-source", "bucket/key"},
        {"x-amz-metadata-directive", "REPLACE"},
        {"x-amz-storage-class", "COLDLINE"},
        {"x-amz-meta-owner", "analytics"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-goog-copy-source"), "bucket/key");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-metadata-directive"), "REPLACE");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-storage-class"), "COLDLINE");
    EXPECT_EQ(headerOrEmpty(translated, "x-goog-meta-owner"), "analytics");

    EXPECT_EQ(translated.count("x-amz-copy-source"), 0u);
    EXPECT_EQ(translated.count("x-amz-metadata-directive"), 0u);
    EXPECT_EQ(translated.count("x-amz-storage-class"), 0u);
    EXPECT_EQ(translated.count("x-amz-meta-owner"), 0u);
}

/// Everything outside the list is left alone. Server-side encryption is the case that matters:
/// GCS spells CMEK as a single `x-goog-encryption-kms-key-name` with a different value, so renaming
/// the prefix would produce a header GCS ignores just as silently as the one it replaced.
TEST(GCSHeaderTranslation, LeavesEverythingElseAlone)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-server-side-encryption", "aws:kms"},
        {"x-amz-server-side-encryption-aws-kms-key-id", "some-key"},
        {"x-amz-request-payer", "requester"},
        {"content-type", "binary/octet-stream"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-amz-server-side-encryption"), "aws:kms");
    EXPECT_EQ(headerOrEmpty(translated, "x-amz-server-side-encryption-aws-kms-key-id"), "some-key");
    EXPECT_EQ(headerOrEmpty(translated, "x-amz-request-payer"), "requester");
    EXPECT_EQ(headerOrEmpty(translated, "content-type"), "binary/octet-stream");
}

/// `x-amz-metadata-directive` begins with `x-amz-meta` but is not custom metadata. The prefix that
/// selects metadata carries a trailing hyphen, so it must not be swept up by the family rule.
TEST(GCSHeaderTranslation, MetadataDirectiveIsNotCustomMetadata)
{
    Aws::Http::HeaderValueCollection headers{{"x-amz-metadata-directive", "COPY"}};

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(headerOrEmpty(translated, "x-goog-metadata-directive"), "COPY");
    EXPECT_EQ(translated.count("x-goog-meta-data-directive"), 0u);
}

/// `x-amz-api-version` has no GCS counterpart and some GCS requests reject it, so it is dropped
/// rather than renamed. The SDK's own `amz-sdk-*` headers carry no `x-amz-` prefix and stay.
TEST(GCSHeaderTranslation, DropsHeadersWithNoCounterpart)
{
    Aws::Http::HeaderValueCollection headers{
        {"x-amz-api-version", "2006-03-01"},
        {"amz-sdk-invocation-id", "some-id"},
        {"amz-sdk-request", "attempt=1; max=3"},
    };

    const auto translated = DB::S3::translateHeadersToGCS(headers);

    EXPECT_EQ(translated.count("x-amz-api-version"), 0u);
    EXPECT_EQ(translated.count("x-goog-api-version"), 0u);
    EXPECT_EQ(headerOrEmpty(translated, "amz-sdk-invocation-id"), "some-id");
    EXPECT_EQ(headerOrEmpty(translated, "amz-sdk-request"), "attempt=1; max=3");
}

/// The response side recognises exactly what the request side renames, so the two cannot drift.
TEST(GCSHeaderTranslation, RecognisesTheSameNamesComingBack)
{
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-meta-owner"), "x-amz-meta-owner");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-storage-class"), "x-amz-storage-class");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-copy-source"), "x-amz-copy-source");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("x-goog-metadata-directive"), "x-amz-metadata-directive");

    /// HTTP/1.1 preserves the case a server sent. The whole name comes back lower-cased, because the
    /// SDK makes the part after the prefix a key in a case-sensitive map.
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("X-Goog-Meta-Owner"), "x-amz-meta-owner");
    EXPECT_EQ(DB::S3::translateHeaderNameFromGCS("X-GOOG-META-CLICKHOUSE-IDEMPOTENCY-ID"), "x-amz-meta-clickhouse-idempotency-id");

    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-generation").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-hash").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("etag").has_value());
    EXPECT_FALSE(DB::S3::translateHeaderNameFromGCS("x-goog-meta-").has_value());
}

/// A mixed-case name would otherwise be classified as an ordinary header by both `x-amz-` checks,
/// attached to the request after signing, and never reach the rename.
TEST(GCSHeaderTranslation, NormalizesHeaderNames)
{
    DB::HTTPHeaderEntries headers{
        {"X-Amz-Meta-Owner", "analytics"},
        {"X-AMZ-STORAGE-CLASS", "GLACIER"},
        {"Custom-Auth-Token", "KeepTheValue"},
    };

    DB::normalizeHeaderNames(headers);

    EXPECT_EQ(headers[0].name, "x-amz-meta-owner");
    EXPECT_EQ(headers[1].name, "x-amz-storage-class");
    EXPECT_EQ(headers[2].name, "custom-auth-token");
    /// Values are untouched.
    EXPECT_EQ(headers[2].value, "KeepTheValue");
}


namespace
{

/// A stand-in for GCS on localhost: it records the headers of the last request and answers in the
/// `x-goog-` spelling, the way GCS does. Metadata is echoed back as `x-goog-meta-*`; `x-goog-hash`
/// and `x-goog-generation` are GCS-only names we do not translate, so they must not confuse the SDK.
class MockGCSHandler : public Poco::Net::HTTPRequestHandler
{
public:
    MockGCSHandler(std::mutex & mutex_, Poco::Net::MessageHeader & last_request_header_)
        : mutex(mutex_), last_request_header(last_request_header_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        {
            std::lock_guard lock(mutex);
            last_request_header = request;
        }

        response.set("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"");
        response.set("x-goog-meta-owner", "analytics");
        response.set("x-goog-generation", "1700000000000000");
        response.set("x-goog-hash", "crc32c=AAAAAA==");
        response.setContentType("binary/octet-stream");
        response.setContentLength(0);
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.send().flush();
    }

private:
    std::mutex & mutex;
    Poco::Net::MessageHeader & last_request_header;
};

class MockGCSHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    MockGCSHandlerFactory(std::mutex & mutex_, Poco::Net::MessageHeader & last_request_header_)
        : mutex(mutex_), last_request_header(last_request_header_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockGCSHandler(mutex, last_request_header);
    }

private:
    std::mutex & mutex;
    Poco::Net::MessageHeader & last_request_header;
};

class MockGCSServer
{
public:
    MockGCSServer()
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , server(std::make_unique<Poco::Net::HTTPServer>(
              new MockGCSHandlerFactory(mutex, last_request_header), *server_socket, makeMockServerParams()))
    {
        server->start();
    }

    ~MockGCSServer() { server->stop(); }

    UInt16 getPort() const { return server_socket->address().port(); }

    Poco::Net::MessageHeader getLastRequestHeader()
    {
        std::lock_guard lock(mutex);
        return last_request_header;
    }

private:
    std::mutex mutex;
    Poco::Net::MessageHeader last_request_header;
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

/// The API mode is GCS only when the endpoint is Google's and the credentials are empty, so the
/// endpoint has to carry the real name. It is never resolved: the request is routed to the mock
/// through a proxy, and `HTTPConnectionPool` connects to the proxy without consulting the resolver.
std::unique_ptr<DB::S3::Client> makeClientTalkingToMockGCS(UInt16 mock_port, const DB::RemoteHostFilter & remote_host_filter)
{
    auto client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 10,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        "http");

    client_configuration.endpointOverride = "http://storage.googleapis.com";
    client_configuration.per_request_configuration = [mock_port]
    {
        return DB::ProxyConfiguration{
            .host = "127.0.0.1",
            .protocol = DB::ProxyConfiguration::Protocol::HTTP,
            .port = mock_port,
        };
    };

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    return DB::S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        /* access_key_id = */ "",
        /* secret_access_key = */ "",
        /* server_side_encryption_customer_key_base64 = */ "",
        /* sse_kms_config = */ {},
        /* headers = */ {},
        DB::S3::CredentialsConfiguration{
            .no_sign_request = true,
            .forbid_implicit_credentials = true,
        });
}

}

/// The boundary the helpers exist for: a request built by the real client and sent over the real
/// HTTP path has to leave in the `x-goog-` spelling. Dropping the `translateHeadersToGCS` call in
/// `Client::BuildHttpRequest` fails here while every helper test above still passes.
TEST(GCSHeaderTranslation, RequestLeavesInTheGoogleSpelling)
{
    MockGCSServer mock_gcs;
    DB::RemoteHostFilter remote_host_filter;
    auto client = makeClientTalkingToMockGCS(mock_gcs.getPort(), remote_host_filter);
    ASSERT_TRUE(client);

    DB::S3::PutObjectRequest request;
    request.SetBucket("test-bucket");
    request.SetKey("test.txt");
    request.SetMetadata({{"owner", "analytics"}});
    request.SetBody(std::make_shared<Aws::StringStream>("content"));

    const auto outcome = client->PutObject(request);
    ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();

    const auto headers = mock_gcs.getLastRequestHeader();
    EXPECT_EQ(headers.get("x-goog-meta-owner", ""), "analytics");
    EXPECT_FALSE(headers.has("x-amz-meta-owner"));
    EXPECT_FALSE(headers.has("x-amz-api-version"));
}

/// The other half of the boundary: GCS answers in its own spelling, and the SDK parses only the
/// `x-amz-` one. Dropping the response shim in `PocoHTTPClient` loses the metadata here.
TEST(GCSHeaderTranslation, ResponseMetadataReachesTheSDK)
{
    MockGCSServer mock_gcs;
    DB::RemoteHostFilter remote_host_filter;
    auto client = makeClientTalkingToMockGCS(mock_gcs.getPort(), remote_host_filter);
    ASSERT_TRUE(client);

    DB::S3::HeadObjectRequest request;
    request.SetBucket("test-bucket");
    request.SetKey("test.txt");

    const auto outcome = client->HeadObject(request);
    ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();

    const auto & metadata = outcome.GetResult().GetMetadata();
    const auto it = metadata.find("owner");
    ASSERT_NE(it, metadata.end());
    EXPECT_EQ(it->second, "analytics");
}

#endif
