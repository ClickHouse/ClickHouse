#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <Common/RemoteHostFilter.h>

#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/stream/ResponseStream.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

namespace
{

/// A one-shot HTTP endpoint that records the header names of the single request it serves. It speaks
/// raw HTTP on purpose: the point of these tests is what actually goes out on the wire, so nothing
/// between `PocoHTTPClient` and the socket may normalise the headers on the way.
class CapturingEndpoint
{
public:
    CapturingEndpoint()
        : socket(Poco::Net::SocketAddress("127.0.0.1", 0))
    {
        socket.setReuseAddress(true);
        thread = std::thread([this] { serveOneRequest(); });
    }

    ~CapturingEndpoint()
    {
        if (thread.joinable())
            thread.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(socket.address().port()) + "/"; }

    /// Joins the serving thread, so the recorded names are published before they are read.
    const std::vector<std::string> & headerNames()
    {
        if (thread.joinable())
            thread.join();
        return header_names;
    }

private:
    void serveOneRequest()
    {
        Poco::Net::StreamSocket peer = socket.acceptConnection();

        std::string head;
        char buffer[1024];
        while (head.find("\r\n\r\n") == std::string::npos)
        {
            int received = peer.receiveBytes(buffer, sizeof(buffer));
            if (received <= 0)
                break;
            head.append(buffer, static_cast<size_t>(received));
        }

        parseHeaderNames(head);

        static const std::string response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        peer.sendBytes(response.data(), static_cast<int>(response.size()));
        peer.close();
    }

    void parseHeaderNames(const std::string & head)
    {
        size_t line_begin = head.find("\r\n");
        if (line_begin == std::string::npos)
            return;
        line_begin += 2;

        while (line_begin < head.size())
        {
            const size_t line_end = head.find("\r\n", line_begin);
            if (line_end == std::string::npos || line_end == line_begin)
                break;

            const size_t colon = head.find(':', line_begin);
            if (colon != std::string::npos && colon < line_end)
                header_names.push_back(head.substr(line_begin, colon - line_begin));

            line_begin = line_end + 2;
        }
    }

    Poco::Net::ServerSocket socket;
    std::thread thread;
    std::vector<std::string> header_names;
};

DB::S3::PocoHTTPClientConfiguration makeConfiguration(const DB::HTTPHeaderEntries & extra_headers)
{
    static const DB::RemoteHostFilter remote_host_filter;

    auto configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 1,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        "http");

    configuration.extra_headers = extra_headers;
    return configuration;
}

/// Sends one GET through the real `PocoHTTPClient` and returns the header names the endpoint saw.
/// `already_signed` mimics the SDK having put the header on the request before signing, which is the
/// state `PocoHTTPClient` must not duplicate.
std::vector<std::string> sendRequest(const DB::HTTPHeaderEntries & extra_headers, const DB::HTTPHeaderEntries & already_signed = {})
{
    CapturingEndpoint endpoint;

    auto configuration = makeConfiguration(extra_headers);
    DB::S3::PocoHTTPClient client(configuration);

    auto request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
        Aws::String(endpoint.url()), Aws::Http::HttpMethod::HTTP_GET);
    request->SetResponseStreamFactory(Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    for (const auto & [name, value] : already_signed)
        request->SetHeaderValue(name, value);

    auto response = client.MakeRequest(request, nullptr, nullptr);
    EXPECT_EQ(response->GetResponseCode(), Aws::Http::HttpResponseCode::OK);

    return endpoint.headerNames();
}

bool contains(const std::vector<std::string> & names, const std::string & name)
{
    return std::find(names.begin(), names.end(), name) != names.end();
}

}

/// A create-only `x-amz-*` header that the request deliberately does not carry -- `UploadPartRequest`
/// and `CompleteMultipartUploadRequest` drop it in `SetAdditionalCustomHeaderValue` -- must not be put
/// back by the post-signing `extra_headers` replay, whatever case the user spelled it in.
TEST(PocoHTTPClientExtraHeaders, DoesNotReplayMixedCaseAmzHeaderAfterSigning)
{
    const auto names = sendRequest({{"X-Amz-Meta-Write-Id", "new"}});
    EXPECT_FALSE(contains(names, "x-amz-meta-write-id"));
    EXPECT_FALSE(contains(names, "X-Amz-Meta-Write-Id"));
}

/// The lowercase spelling of the same header, which already behaved this way.
TEST(PocoHTTPClientExtraHeaders, DoesNotReplayLowerCaseAmzHeaderAfterSigning)
{
    const auto names = sendRequest({{"x-amz-meta-write-id", "new"}});
    EXPECT_FALSE(contains(names, "x-amz-meta-write-id"));
}

/// When the header did make it onto the signed request, it goes out exactly once, from the signed set.
TEST(PocoHTTPClientExtraHeaders, KeepsSignedAmzHeaderExactlyOnce)
{
    const auto names = sendRequest({{"X-Amz-Meta-Write-Id", "new"}}, {{"x-amz-meta-write-id", "new"}});
    EXPECT_EQ(std::count(names.begin(), names.end(), "x-amz-meta-write-id"), 1);
}

/// Non-`x-amz-` extra headers are what the replay loop exists for, and are still forwarded.
TEST(PocoHTTPClientExtraHeaders, ForwardsNonAmzExtraHeader)
{
    const auto names = sendRequest({{"X-Custom-Trace-Id", "42"}});
    EXPECT_TRUE(contains(names, "x-custom-trace-id"));
}

#endif
