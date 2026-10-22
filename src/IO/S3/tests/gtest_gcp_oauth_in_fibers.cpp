#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3 && USE_SILK && USE_SSL

#include <Common/RemoteHostFilter.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>
#include <Common/tests/gtest_ephemeral_certificate.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/tests/TestPocoHTTPServer.h>

#include <Poco/Event.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/StreamCopier.h>

#include <silk/fibers/future.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

/// The S3 clients here rely on the `Poco::Util::ServerApplication` that `gtest_aws_s3_client.cpp` creates for the whole binary.

namespace
{

/// An OAuth token endpoint that holds the first request until `release` is set and counts the requests.
class TokenEndpoint
{
public:
    TokenEndpoint()
        : server_socket(0)
        , server(new Factory(*this), server_socket, makeMockServerParams())
    {
        server.start();
    }

    ~TokenEndpoint()
    {
        release.set();
        server.stop();
    }

    std::string getTokenUri() const { return "http://127.0.0.1:" + std::to_string(server_socket.address().port()) + "/token"; }

    Poco::Event release{/* autoReset = */ false};
    std::atomic<size_t> requests = 0;

private:
    class Handler : public Poco::Net::HTTPRequestHandler
    {
    public:
        explicit Handler(TokenEndpoint & endpoint_) : endpoint(endpoint_) {}

        void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
        {
            std::string body;
            Poco::StreamCopier::copyToString(request.stream(), body);

            ++endpoint.requests;
            endpoint.release.wait();

            const std::string token_json = R"({"access_token": "fiber_token", "expires_in": 3600, "token_type": "Bearer"})";
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            response.setContentType("application/json");
            response.setContentLength(static_cast<std::streamsize>(token_json.size()));
            response.send() << token_json;
        }

    private:
        TokenEndpoint & endpoint;
    };

    class Factory : public Poco::Net::HTTPRequestHandlerFactory
    {
    public:
        explicit Factory(TokenEndpoint & endpoint_) : endpoint(endpoint_) {}
        Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override { return new Handler(endpoint); }

    private:
        TokenEndpoint & endpoint;
    };

    Poco::Net::ServerSocket server_socket;
    Poco::Net::HTTPServer server;
};

std::string makeServiceAccountKey(const std::string & token_uri)
{
    const EphemeralCert cert;
    std::ifstream key_file(cert.key_path);
    std::stringstream private_key;
    private_key << key_file.rdbuf();

    Poco::JSON::Object key;
    key.set("client_email", "fiber-test@example.com");
    key.set("private_key", private_key.str());
    key.set("token_uri", token_uri);
    std::ostringstream key_json;
    key.stringify(key_json);
    return key_json.str();
}

}

/// Concurrent fibers that need the GCP bearer token wait for one refresh without blocking scheduler threads.
/// The token endpoint holds the refresh until every fiber has started. If the refresh held a native mutex across
/// its I/O, the fibers waiting for that mutex would block every scheduler thread, and the rest would never start.
TEST(GCPOAuthInFibers, ConcurrentFibersWaitForOneTokenRefresh)
{
    /// `Silk::FiberMutex` of the client is chosen when the client is constructed.
    initializeFiberSchedulerForTests();

    TokenEndpoint endpoint;

    static const DB::RemoteHostFilter remote_host_filter;
    auto client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 10,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{},
        /* s3_slow_all_threads_after_network_error = */ true,
        /* s3_slow_all_threads_after_retryable_error = */ true,
        /* enable_s3_requests_logging = */ false,
        /* group = */ DB::HTTPConnectionGroupType::AMBER,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {},
        "http");
    client_configuration.google_service_account_key = makeServiceAccountKey(endpoint.getTokenUri());
    DB::S3::PocoHTTPClientGCPOAuth client(client_configuration);

    /// More fibers than scheduler threads, of which there is one per CPU.
    const size_t fiber_count = 2 * std::max(1U, std::thread::hardware_concurrency()) + 1;
    std::atomic<size_t> started = 0;
    Poco::Event all_started(/* autoReset = */ false);
    std::vector<silk::FiberFuture> futures(fiber_count);
    std::vector<std::string> tokens(fiber_count);

    size_t spawned = 0;
    while (spawned < fiber_count && Silk::spawn([&, i = spawned]
        {
            if (++started == fiber_count)
                all_started.set();
            tokens[i] = client.getBearerToken();
            return 0;
        }, futures[spawned]) == 0)
        ++spawned;
    if (spawned != fiber_count)
    {
        /// The started fibers use the locals above, so they finish before the test returns.
        endpoint.release.set();
        for (size_t i = 0; i < spawned; ++i)
            futures[i].wait();
        FAIL() << "Spawned " << spawned << " of " << fiber_count << " fibers";
    }

    EXPECT_TRUE(all_started.tryWait(60'000));
    endpoint.release.set();
    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);

    EXPECT_EQ(endpoint.requests, 1);
    for (const auto & token : tokens)
        EXPECT_EQ(token, "fiber_token");
}

#endif
