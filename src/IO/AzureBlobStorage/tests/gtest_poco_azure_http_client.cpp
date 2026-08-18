#include <gtest/gtest.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <IO/AzureBlobStorage/PocoHTTPClient.h>

#include <azure/core/context.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/io/body_stream.hpp>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/StreamCopier.h>

#include <fmt/format.h>

namespace
{

struct CapturedRequest
{
    bool has_content_length = false;
    Int64 content_length = -1;
    std::string body;
};

class CapturingRequestHandler : public Poco::Net::HTTPRequestHandler
{
    CapturedRequest & captured;

public:
    explicit CapturingRequestHandler(CapturedRequest & captured_)
        : captured(captured_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        captured.has_content_length = request.hasContentLength();
        captured.content_length = request.hasContentLength() ? request.getContentLength64() : -1;
        captured.body.clear();
        Poco::StreamCopier::copyToString(request.stream(), captured.body);

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        response.setContentLength(2);
        response.send() << "{}";
    }
};

class CapturingRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    CapturedRequest & captured;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new CapturingRequestHandler(captured);
    }

public:
    explicit CapturingRequestHandlerFactory(CapturedRequest & captured_)
        : captured(captured_)
    {
    }
};

}

/// The Key Vault SDK clients do not set `Content-Length` themselves and rely on the transport
/// to compute it from the body stream (the removed curl-based transport did that). A request
/// sent without it has no body framing at all and Azure rejects it with `411 Length Required`.
TEST(PocoAzureHTTPClient, SetsContentLengthFromBodyStream)
{
    CapturedRequest captured;
    Poco::Net::ServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::HTTPServer server(
        new CapturingRequestHandlerFactory(captured), server_socket, new Poco::Net::HTTPServerParams);
    server.start();

    DB::RemoteHostFilter remote_host_filter;
    DB::PocoAzureHTTPClient client(DB::PocoAzureHTTPClientConfiguration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 3,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {},
    });

    const std::string body = R"({"alg":"RSA-OAEP-256","value":"dGVzdA"})";
    const auto url = fmt::format("http://{}/keys/test-key/decrypt", server_socket.address().toString());

    /// No `Content-Length` in the SDK request: the transport must synthesize it.
    {
        Azure::Core::IO::MemoryBodyStream body_stream(reinterpret_cast<const uint8_t *>(body.data()), body.size());
        Azure::Core::Http::Request request(Azure::Core::Http::HttpMethod::Post, Azure::Core::Url(url), &body_stream);
        request.SetHeader("Content-Type", "application/json");

        auto response = client.Send(request, Azure::Core::Context());

        EXPECT_EQ(static_cast<int>(response->GetStatusCode()), 200);
        EXPECT_TRUE(captured.has_content_length);
        EXPECT_EQ(captured.content_length, static_cast<Int64>(body.size()));
        EXPECT_EQ(captured.body, body);
    }

    /// An explicitly set `Content-Length` (e.g. by the blob storage or identity clients)
    /// is passed through unchanged.
    {
        captured = {};
        Azure::Core::IO::MemoryBodyStream body_stream(reinterpret_cast<const uint8_t *>(body.data()), body.size());
        Azure::Core::Http::Request request(Azure::Core::Http::HttpMethod::Post, Azure::Core::Url(url), &body_stream);
        request.SetHeader("Content-Type", "application/json");
        request.SetHeader("Content-Length", std::to_string(body.size()));

        auto response = client.Send(request, Azure::Core::Context());

        EXPECT_EQ(static_cast<int>(response->GetStatusCode()), 200);
        EXPECT_TRUE(captured.has_content_length);
        EXPECT_EQ(captured.content_length, static_cast<Int64>(body.size()));
        EXPECT_EQ(captured.body, body);
    }

    /// A body-carrying method with an empty body still gets `Content-Length: 0`,
    /// like the curl-based transport did.
    {
        captured = {};
        Azure::Core::Http::Request request(Azure::Core::Http::HttpMethod::Post, Azure::Core::Url(url));

        auto response = client.Send(request, Azure::Core::Context());

        EXPECT_EQ(static_cast<int>(response->GetStatusCode()), 200);
        EXPECT_TRUE(captured.has_content_length);
        EXPECT_EQ(captured.content_length, 0);
        EXPECT_TRUE(captured.body.empty());
    }

    /// `GET` never carries a body and must not get a synthesized header.
    {
        captured = {};
        Azure::Core::Http::Request request(Azure::Core::Http::HttpMethod::Get, Azure::Core::Url(url));

        auto response = client.Send(request, Azure::Core::Context());

        EXPECT_EQ(static_cast<int>(response->GetStatusCode()), 200);
        EXPECT_FALSE(captured.has_content_length);
        EXPECT_TRUE(captured.body.empty());
    }

    server.stop();
}

#endif
