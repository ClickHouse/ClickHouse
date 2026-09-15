#include <Common/HTTPConnectionPool.h>
#include <gtest/gtest.h>

#include <IO/S3/Credentials.h>
#include "config.h"


#if USE_AWS_S3

#include <atomic>
#include <cstdlib>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include <base/scope_guard.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/SharedPtr.h>
#include <Poco/StreamChannel.h>
#include <Poco/ThreadPool.h>
#include <Poco/URI.h>

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/memory/AWSMemory.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteSettings.h>
#include <IO/S3Common.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3Settings.h>

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
}

namespace ProfileEvents
{
    extern const Event S3WriteRequestsErrors;
}

/// Parses the `attempt=N` value `S3::setClickhouseAttemptNumber` writes into the `clickhouse-request`
/// header, straight off the wire header a real HTTP server received -- mirrors
/// `S3::getAttemptFromInfo`/`getOrEmpty` (both `static` in `Requests.cpp`, not exported), 1 when the
/// header is missing.
static size_t attemptFromHeader(const Poco::Net::MessageHeader & header)
{
    const std::string & value = header.get("clickhouse-request", "");
    static const std::string key = "attempt=";
    auto pos = value.find(key);
    if (pos == std::string::npos)
        return 1;
    try
    {
        return static_cast<size_t>(std::stol(value.substr(pos + key.size())));
    }
    catch (const std::exception &)
    {
        return 1;
    }
}

static std::shared_ptr<DB::S3::Client> makeTestClient(const DB::S3::URI & uri)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /*s3_max_redirects=*/100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /*s3_slow_all_threads_after_network_error=*/false,
        /*s3_slow_all_threads_after_retryable_error=*/false,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*opt_disk_name=*/{},
        /*request_throttler=*/{},
        uri.uri.getScheme());
    /// Fresh connection per request: this file's servers live on ephemeral ports and die with the test,
    /// and a pooled keep-alive connection can outlive its server (`Connection reset by peer` under `--gtest_repeat`).
    client_configuration.http_keep_alive_timeout = 0;
    client_configuration.endpointOverride = uri.endpoint;
    /// `ClientFactory::create` installs the SDK's actual retry strategy itself from
    /// `client_configuration.retry_strategy`/`s3_slow_all_threads_after_retryable_error` (any
    /// `retryStrategy` set here is overwritten) -- with `s3_slow_all_threads_after_retryable_error`
    /// true it forces `max_retries = 1` regardless of the `RetryStrategy{.max_retries = 0}` passed
    /// above, so the SDK itself retries a retryable error once before `ReadBufferFromS3`'s own
    /// local-retry loop ever sees a failure, and both physical requests carry the same seeded header.
    /// `false` here keeps the SDK to exactly one physical attempt, matching the CAS single-attempt
    /// client's own setup.

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
        /*server_side_encryption_customer_key_base64=*/"",
        DB::S3::ServerSideEncryptionKMSConfig(),
        DB::HTTPHeaderEntries(),
        DB::S3::CredentialsConfiguration{
            .use_environment_credentials = false,
            .use_insecure_imds_request = false,
        });
}

/// Anonymous namespace: these three classes have no counterpart in gtest_aws_s3_client.cpp today, but
/// giving them internal linkage costs nothing and avoids ever silently colliding with a same-named
/// class that file adds later (see the equivalent note in gtest_cas_readbuffer_s3.cpp for what such a
/// collision actually does at link time).
namespace
{

/// Fails the first `fail_first_n` requests with `fail_status` (empty body), then serves `body` with a
/// 200 to every request after. Records every request's header (not just the last) so a caller can
/// check the sequence a local retry produced.
class SequenceRecordingRequestHandler : public Poco::Net::HTTPRequestHandler
{
    std::vector<Poco::Net::MessageHeader> & all_request_headers;
    size_t & requests_seen;
    size_t fail_first_n;
    Poco::Net::HTTPResponse::HTTPStatus fail_status;
    std::string body;

public:
    SequenceRecordingRequestHandler(
        std::vector<Poco::Net::MessageHeader> & all_request_headers_,
        size_t & requests_seen_,
        size_t fail_first_n_,
        Poco::Net::HTTPResponse::HTTPStatus fail_status_,
        std::string body_)
        : all_request_headers(all_request_headers_)
        , requests_seen(requests_seen_)
        , fail_first_n(fail_first_n_)
        , fail_status(fail_status_)
        , body(std::move(body_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        all_request_headers.push_back(request);
        ++requests_seen;

        if (requests_seen <= fail_first_n)
        {
            response.setStatus(fail_status);
            response.send();
            return;
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        auto & out = response.send();
        out << body;
        out.flush();
    }
};

class SequenceRecordingRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    std::vector<Poco::Net::MessageHeader> & all_request_headers;
    size_t & requests_seen;
    size_t fail_first_n;
    Poco::Net::HTTPResponse::HTTPStatus fail_status;
    std::string body;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new SequenceRecordingRequestHandler(all_request_headers, requests_seen, fail_first_n, fail_status, body);
    }

public:
    SequenceRecordingRequestHandlerFactory(
        std::vector<Poco::Net::MessageHeader> & all_request_headers_,
        size_t & requests_seen_,
        size_t fail_first_n_,
        Poco::Net::HTTPResponse::HTTPStatus fail_status_,
        std::string body_)
        : all_request_headers(all_request_headers_)
        , requests_seen(requests_seen_)
        , fail_first_n(fail_first_n_)
        , fail_status(fail_status_)
        , body(std::move(body_))
    {
    }

    ~SequenceRecordingRequestHandlerFactory() override = default;
};

/// Like `TestPocoHTTPServer`, but for driving a real local retry: the first `fail_first_n` requests
/// get `fail_status`, every one after gets `body` with a 200, and every request's header is kept (not
/// just the last). Its only user is the seed test right below -- localized here rather than in the
/// shared `TestPocoHTTPServer.h` header.
class TestPocoHTTPSequenceServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<SequenceRecordingRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    /// A dedicated pool, not `Poco::ThreadPool::defaultPool()` (the `HTTPServer` default): that pool
    /// is shared with every other local-server test in this binary, and `TCPServerDispatcher::enqueue`
    /// (base/poco/Net/src/TCPServerDispatcher.cpp) has an acknowledged-in-comment saturation-check race
    /// when it's shared, which can accept a connection and then close it with no response.
    Poco::ThreadPool thread_pool;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    std::vector<Poco::Net::MessageHeader> all_request_headers;
    size_t requests_seen = 0;

public:
    TestPocoHTTPSequenceServer(size_t fail_first_n, Poco::Net::HTTPResponse::HTTPStatus fail_status, std::string body = {}):
        /// Bind to the loopback address explicitly, not `ServerSocket(0)`'s wildcard `0.0.0.0`: the
        /// latter is not a valid connection target, even though the kernel happens to tolerate a
        /// connect() to it as loopback on Linux.
        server_socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0))),
        handler_factory(new SequenceRecordingRequestHandlerFactory(all_request_headers, requests_seen, fail_first_n, fail_status, std::move(body))),
        server_params(new Poco::Net::HTTPServerParams()),
        thread_pool("TestPocoHTTPSequenceServer"),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, thread_pool, *server_socket, server_params))
    {
        server->start();
    }

    /// Closing the cached client sockets wakes the server workers without Poco's abort notification,
    /// whose unlocked socket shutdown races the worker's own close. Precondition: callers have released
    /// their sessions, otherwise `joinAll` waits for the server's request timeout.
    ~TestPocoHTTPSequenceServer()
    {
        DB::HTTPConnectionPools::instance().dropCache();
        server->stop();
        thread_pool.joinAll();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    const std::vector<Poco::Net::MessageHeader> & getAllRequestHeaders() const
    {
        return all_request_headers;
    }
};

}

/// An unset seed sends `[1, 2]` across a local retry, a seed of 2 sends `[2, 3]` -- a real HTTP round
/// trip through `TestPocoHTTPSequenceServer` is the only way to drive the retry through
/// `ReadBufferFromS3`'s actual success path (the SDK's response stream wraps a real
/// `Poco::Net::HTTPBasicStreamBuf`, which `ReadBufferFromIStream` requires).
TEST(CASIOTestAwsS3Client, ReadBufferFromS3AttemptSeedCarriesAcrossLocalRetry)
{
    for (const auto [seed, first, second] : {std::tuple<size_t, size_t, size_t>{0, 1, 2}, {2, 2, 3}})
    {
        TestPocoHTTPSequenceServer http(/*fail_first_n=*/1, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, "seeded-body");
        DB::S3::URI uri(http.getUrl() + "/seeded-bucket/seeded-key");
        auto client = makeTestClient(uri);
        ASSERT_TRUE(client);

        DB::ReadSettings read_settings;
        read_settings.object_storage_attempt_number = seed;
        DB::S3::S3RequestSettings request_settings;
        request_settings[DB::S3RequestSetting::max_single_read_retries] = 2;
        DB::ReadBufferFromS3 read_buffer(client, uri.bucket, uri.key, /*version_id=*/{}, request_settings, read_settings);

        String content;
        DB::readStringUntilEOF(content, read_buffer);
        EXPECT_EQ(content, "seeded-body");

        const auto & headers = http.getAllRequestHeaders();
        ASSERT_EQ(headers.size(), 2u);
        EXPECT_EQ(attemptFromHeader(headers[0]), first);
        EXPECT_EQ(attemptFromHeader(headers[1]), second);
    }
}

namespace
{

/// Captures what the `S3Client` logger (`Client::log`) writes at ERROR and above. A message logged
/// below Error (e.g. Debug) never reaches the channel at this threshold, so an empty capture proves
/// the site logged below Error rather than merely that this particular text was absent.
class ScopedS3ClientErrorLogCapture
{
public:
    ScopedS3ClientErrorLogCapture()
        : logger(getLogger("S3Client"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("error");
    }

    ~ScopedS3ClientErrorLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    std::string captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream;
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// `shared=true` is load-bearing: `AutoPtr(ptr)` would steal a reference the fixture never owned.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

/// A `Client` whose `PutObject` always fails as though the connection dropped while the response body
/// was being read -- the scenario `Client::doRequestWithRetryNetworkErrors`'s `net_exception_handler`
/// exists for (the comment on that function: "network error happens when XML document is being read
/// from the response body"). Throwing here, through the same virtual `Aws::S3::S3Client::PutObject`
/// slot `Client::PutObject`'s retry loop calls, reaches `net_exception_handler` exactly as a genuine
/// mid-body network failure would, without adding a test seam to production code -- the protected
/// `Client` constructor is already exposed "for testing" (see `RecordingClient` in `gtest_aws_s3_client.cpp`).
class NetworkFailingClient : public DB::S3::Client
{
public:
    NetworkFailingClient(
        size_t max_redirects_,
        DB::S3::ServerSideEncryptionKMSConfig sse_kms_config_,
        const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider_,
        const DB::S3::PocoHTTPClientConfiguration & client_configuration_,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy sign_payloads_,
        const DB::S3::ClientSettings & client_settings_)
        : DB::S3::Client(max_redirects_, std::move(sse_kms_config_), credentials_provider_, client_configuration_, sign_payloads_, client_settings_)
    {
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest &) const override
    {
        ++attempts;
        throw Poco::TimeoutException("mock timeout reading the response body");
    }

    mutable size_t attempts = 0;
};

std::shared_ptr<NetworkFailingClient> makeNetworkFailingClient(std::shared_ptr<Aws::Client::RetryStrategy> retry_strategy)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/"us-east-1",
        remote_host_filter,
        /*s3_max_redirects=*/100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /*s3_slow_all_threads_after_network_error=*/false,
        /*s3_slow_all_threads_after_retryable_error=*/false,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*opt_disk_name=*/{},
        /*request_throttler=*/{});
    /// `PutObject` never reaches the wire (it is overridden below), so the endpoint is irrelevant --
    /// only the installed retry strategy, which is what `usesSingleAttemptRetryStrategy` inspects.
    client_configuration.retryStrategy = std::move(retry_strategy);

    DB::S3::ClientSettings client_settings{
        .use_virtual_addressing = true,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };

    Aws::Auth::AWSCredentials credentials("ACCESS_KEY_ID", "SECRET_ACCESS_KEY");
    auto credentials_provider = DB::S3::getCredentialsProvider(
        client_configuration,
        credentials,
        DB::S3::CredentialsConfiguration{.use_environment_credentials = false, .use_insecure_imds_request = false});

    return std::make_shared<NetworkFailingClient>(
        /*max_redirects_=*/100,
        DB::S3::ServerSideEncryptionKMSConfig{},
        credentials_provider,
        client_configuration,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        client_settings);
}

}

/// The client's own retry strategy still says "do not retry" (`SingleAttemptRetryStrategy::ShouldRetry`
/// always false, tested directly against `DoesNotRetryPreconditionFailed`/`SingleAttemptRetryStrategyRefusesAndCounts`
/// in `gtest_aws_s3_client.cpp`); `usesSingleAttemptRetryStrategy` is a separate, purely descriptive
/// check of which strategy is installed, tested directly here.
TEST(CASIOTestAwsS3Client, UsesSingleAttemptRetryStrategyIdentifiesTheInstalledStrategy)
{
    auto single_attempt_client = makeNetworkFailingClient(std::make_shared<DB::S3::SingleAttemptRetryStrategy>());
    EXPECT_TRUE(single_attempt_client->usesSingleAttemptRetryStrategy());

    DB::S3::PocoHTTPClientConfiguration::RetryStrategy zero_retries{.max_retries = 0};
    auto ordinary_client = makeNetworkFailingClient(std::make_shared<DB::S3::Client::RetryStrategy>(zero_retries));
    EXPECT_FALSE(ordinary_client->usesSingleAttemptRetryStrategy());
}

/// A client carrying the `SingleAttemptRetryStrategy` (the CAS conditional-write client, see
/// `S3ObjectStorage::getSingleAttemptClient`) is owned by an outer retry loop that resolves the outcome
/// and reissues; its one failed attempt is not terminal, so the network-error log site must not reach
/// Error.
TEST(CASIOTestAwsS3Client, NetworkErrorLogsDebugForSingleAttemptStrategy)
{
    using ProfileEvents::global_counters;
    const auto errors_before = global_counters[ProfileEvents::S3WriteRequestsErrors].load();

    auto client = makeNetworkFailingClient(std::make_shared<DB::S3::SingleAttemptRetryStrategy>());
    DB::S3::PutObjectRequest request;

    /// Call through the `DB::S3::Client&` interface, exactly as production code (which only ever
    /// holds a `Client`, never `NetworkFailingClient`) does: `NetworkFailingClient::PutObject` hides
    /// `Client::PutObject(PutObjectRequest&)` -- the retry-loop wrapper under test -- from lookup on
    /// the derived type, so calling through the base is what makes this test exercise that wrapper
    /// rather than the override directly.
    const DB::S3::Client & base_client = *client;
    ScopedS3ClientErrorLogCapture log_capture;
    const auto outcome = base_client.PutObject(request);

    EXPECT_FALSE(outcome.IsSuccess());
    EXPECT_EQ(outcome.GetError().GetErrorType(), Aws::S3::S3Errors::NETWORK_CONNECTION);
    EXPECT_EQ(client->attempts, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::S3WriteRequestsErrors].load() - errors_before, 1u);
    EXPECT_TRUE(log_capture.captured().empty());
}

/// `max_retries = 0` on the ORDINARY strategy is a supported user configuration (`s3_retry_attempts`)
/// with no outer retry loop: its one failed attempt IS the final answer, so it must keep logging at
/// Error -- this is exactly the case a signal keyed on `max_retries == 0` alone would misclassify.
TEST(CASIOTestAwsS3Client, NetworkErrorLogsErrorForOrdinaryZeroRetryStrategy)
{
    using ProfileEvents::global_counters;
    const auto errors_before = global_counters[ProfileEvents::S3WriteRequestsErrors].load();

    DB::S3::PocoHTTPClientConfiguration::RetryStrategy zero_retries{.max_retries = 0};
    auto client = makeNetworkFailingClient(std::make_shared<DB::S3::Client::RetryStrategy>(zero_retries));
    DB::S3::PutObjectRequest request;

    /// See the comment in `NetworkErrorLogsDebugForSingleAttemptStrategy`: calling through the base
    /// is what reaches `Client::PutObject`'s retry-loop wrapper rather than the override directly.
    const DB::S3::Client & base_client = *client;
    ScopedS3ClientErrorLogCapture log_capture;
    const auto outcome = base_client.PutObject(request);

    EXPECT_FALSE(outcome.IsSuccess());
    EXPECT_EQ(outcome.GetError().GetErrorType(), Aws::S3::S3Errors::NETWORK_CONNECTION);
    EXPECT_EQ(client->attempts, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::S3WriteRequestsErrors].load() - errors_before, 1u);
    EXPECT_NE(log_capture.captured().find("Network error on S3 request, attempt 1 of 1"), std::string::npos);
}

#endif
