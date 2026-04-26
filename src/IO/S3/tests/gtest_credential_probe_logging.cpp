#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <atomic>
#include <memory>
#include <string>

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/SharedPtr.h>
#include <Poco/Util/ServerApplication.h>

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

#include <Common/Logger.h>
#include <Common/RemoteHostFilter.h>
#include <IO/Expect404ResponseScope.h>
#include <IO/ExpectCredentialProbe4xxScope.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>


/// See gtest_aws_s3_client.cpp for rationale.
[[maybe_unused]] static Poco::Util::ServerApplication app;


namespace
{

/// Mock handler returning a fixed status code on every request.
class FixedStatusHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit FixedStatusHandler(int status_code_) : status_code(status_code_) {}

    void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(static_cast<Poco::Net::HTTPResponse::HTTPStatus>(status_code));
        response.setReason("Bad Request");
        response.send();
    }

private:
    int status_code;
};

class FixedStatusHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit FixedStatusHandlerFactory(int status_code_) : status_code(status_code_) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new FixedStatusHandler(status_code);
    }

private:
    int status_code;
};

class FixedStatusHTTPServer
{
public:
    explicit FixedStatusHTTPServer(int status_code)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , handler_factory(new FixedStatusHandlerFactory(status_code))
        , server_params(new Poco::Net::HTTPServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl() const
    {
        return "http://" + server_socket->address().toString();
    }

private:
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<FixedStatusHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

/// Captures every Poco log message at PRIO_ERROR or above so the test can
/// assert whether `LOG_ERROR(log, "Response status: ...")` was emitted by
/// `PocoHTTPClient::makeRequestInternalImpl`.
class CountingLogChannel : public Poco::Channel
{
public:
    void log(const Poco::Message & msg) override
    {
        if (msg.getPriority() <= Poco::Message::PRIO_ERROR
            && msg.getText().find("Response status: ") != std::string::npos)
        {
            error_count.fetch_add(1, std::memory_order_relaxed);
        }
    }

    size_t getErrorCount() const { return error_count.load(std::memory_order_relaxed); }

private:
    std::atomic<size_t> error_count{0};
};

struct LoggerSpy
{
    Poco::AutoPtr<CountingLogChannel> channel;
    Poco::Channel * previous_channel = nullptr;
    Poco::Logger * logger = nullptr;

    explicit LoggerSpy(const std::string & logger_name)
        : channel(new CountingLogChannel())
        , logger(&Poco::Logger::get(logger_name))
    {
        /// Hold a reference to the existing channel so that `setChannel` below does
        /// not free it; restore it in the destructor.
        previous_channel = logger->getChannel();
        if (previous_channel)
            previous_channel->duplicate();
        logger->setChannel(channel.get());
    }

    ~LoggerSpy()
    {
        logger->setChannel(previous_channel);
        if (previous_channel)
            previous_channel->release();
    }
};

DB::S3::PocoHTTPClientConfiguration makeClientConfiguration(const std::string & url)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        /* force_region = */ "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 0,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    client_configuration.endpointOverride = url;
    return client_configuration;
}

size_t makeRequestAndCountErrorLogs(const std::string & url, std::function<void()> wrap)
{
    LoggerSpy spy("AWSClient");

    DB::S3::PocoHTTPClient client{makeClientConfiguration(url)};

    auto request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
        Aws::String(url), Aws::Http::HttpMethod::HTTP_GET);

    wrap();
    client.MakeRequest(request, /* readLimiter = */ nullptr, /* writeLimiter = */ nullptr);

    return spy.channel->getErrorCount();
}

}


/// Without any scope active, a 4xx response from the upstream HTTP server
/// should be logged as `LOG_ERROR("Response status: 4XX, ...")`.
TEST(CredentialProbeLogging, NoScopeFourHundredLogsError)
{
    FixedStatusHTTPServer server{400};
    size_t count = makeRequestAndCountErrorLogs(server.getUrl(), [] {});
    EXPECT_GE(count, 1u);
}

/// When `ExpectCredentialProbe4xxScope` is active, a 4xx response is expected
/// during AWS credential-provider probing (IMDSv2 token negotiation, STS
/// web-identity refresh) and should NOT be logged at error level.
TEST(CredentialProbeLogging, WithCredentialProbeScopeFourHundredDoesNotLogError)
{
    FixedStatusHTTPServer server{400};
    std::optional<DB::ExpectCredentialProbe4xxScope> scope;
    size_t count = makeRequestAndCountErrorLogs(server.getUrl(), [&] { scope.emplace(); });
    EXPECT_EQ(count, 0u);
}

/// `Expect404ResponseScope` only suppresses HTTP 404. A 400 response with
/// only the 404 scope active must still log at error level (proves the
/// existing 404-scope is unchanged).
TEST(CredentialProbeLogging, WithFourOhFourScopeOnlyFourHundredLogsError)
{
    FixedStatusHTTPServer server{400};
    std::optional<DB::Expect404ResponseScope> scope;
    size_t count = makeRequestAndCountErrorLogs(server.getUrl(), [&] { scope.emplace(); });
    EXPECT_GE(count, 1u);
}

/// With both scopes active, the credential-probe scope handles the 4xx
/// suppression independently of the 404-only scope.
TEST(CredentialProbeLogging, WithBothScopesFourHundredDoesNotLogError)
{
    FixedStatusHTTPServer server{400};
    std::optional<DB::Expect404ResponseScope> scope_404;
    std::optional<DB::ExpectCredentialProbe4xxScope> scope_4xx;
    size_t count = makeRequestAndCountErrorLogs(server.getUrl(), [&] { scope_404.emplace(); scope_4xx.emplace(); });
    EXPECT_EQ(count, 0u);
}

/// 5xx responses during credential probing represent genuine infrastructure
/// outages (e.g., regional STS down). They must still log at error level
/// even when `ExpectCredentialProbe4xxScope` is active, because the scope is
/// bounded to the 4xx range.
TEST(CredentialProbeLogging, WithCredentialProbeScopeFiveOhThreeStillLogsError)
{
    FixedStatusHTTPServer server{503};
    std::optional<DB::ExpectCredentialProbe4xxScope> scope;
    size_t count = makeRequestAndCountErrorLogs(server.getUrl(), [&] { scope.emplace(); });
    EXPECT_GE(count, 1u);
}

#endif
