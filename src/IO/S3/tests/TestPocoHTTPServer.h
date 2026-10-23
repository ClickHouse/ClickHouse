#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/URI.h>
#include <Poco/AutoPtr.h>
#include <Poco/SharedPtr.h>
#include <fmt/format.h>

/// Keep-alive is disabled so a handler thread exits right after sending the response: it does
/// not park on the connection until the keep-alive timeout (starving the shared default
/// `Poco::ThreadPool`, capacity 16 for the whole test binary), and teardown does not need
/// `stopAll(/* abortCurrent */ true)`, whose socket shutdown races with the handler thread
/// closing the same socket at the end of `HTTPServerConnection::run` (reported by TSan).
inline Poco::AutoPtr<Poco::Net::HTTPServerParams> makeMockServerParams()
{
    Poco::AutoPtr<Poco::Net::HTTPServerParams> params(new Poco::Net::HTTPServerParams());
    params->setKeepAlive(false);
    return params;
}

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
    Poco::Net::MessageHeader & last_request_header;

public:
    explicit MockRequestHandler(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        last_request_header = request;
        response.send();
    }
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    Poco::Net::MessageHeader & last_request_header;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler(last_request_header);
    }

public:
    explicit HTTPRequestHandlerFactory(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    ~HTTPRequestHandlerFactory() override = default;
};

class TestPocoHTTPServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<HTTPRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    Poco::Net::MessageHeader last_request_header;

public:
    TestPocoHTTPServer():
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new HTTPRequestHandlerFactory(last_request_header)),
        server_params(makeMockServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    ~TestPocoHTTPServer()
    {
        server->stop();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_header;
    }
};

/// A handler that fails the first `num_failures` requests with an internal server error, so a
/// client retries them, and answers the rest with 200. The last received header and body are
/// recorded for assertions. Used to reproduce the SDK retrying an upload whose body was already
/// consumed by a previous attempt: the retry path signs the body again, which must find the
/// stream in a readable state.
class MockRequestHandlerWithFailures : public Poco::Net::HTTPRequestHandler
{
    std::atomic<size_t> & request_count;
    size_t num_failures;
    Poco::Net::MessageHeader & last_request_header;
    std::string & last_request_body;

public:
    MockRequestHandlerWithFailures(
        std::atomic<size_t> & request_count_,
        size_t num_failures_,
        Poco::Net::MessageHeader & last_request_header_,
        std::string & last_request_body_)
        : request_count(request_count_)
        , num_failures(num_failures_)
        , last_request_header(last_request_header_)
        , last_request_body(last_request_body_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        last_request_header = request;

        /// The body arrives as a plain PUT payload; read it for assertions.
        last_request_body.assign(std::istreambuf_iterator<char>(request.stream()), std::istreambuf_iterator<char>());

        if (request_count.fetch_add(1) < num_failures)
        {
            response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            auto & out = response.send();
            out << "<Error><Code>InternalError</Code><Message>injected failure</Message></Error>";
            out.flush();
            return;
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        auto & out = response.send();
        out.flush();
    }
};

class HTTPRequestHandlerFactoryWithFailures : public Poco::Net::HTTPRequestHandlerFactory
{
    std::atomic<size_t> request_count = 0;
    size_t num_failures;
    Poco::Net::MessageHeader & last_request_header;
    std::string & last_request_body;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandlerWithFailures(request_count, num_failures, last_request_header, last_request_body);
    }

public:
    explicit HTTPRequestHandlerFactoryWithFailures(
        size_t num_failures_, Poco::Net::MessageHeader & last_request_header_, std::string & last_request_body_)
        : num_failures(num_failures_)
        , last_request_header(last_request_header_)
        , last_request_body(last_request_body_)
    {
    }

    ~HTTPRequestHandlerFactoryWithFailures() override = default;

    size_t requestCount() const { return request_count.load(); }
};

/// An HTTP server that fails the first `num_failures` requests with a retryable error (500), then
/// answers successfully. See `MockRequestHandlerWithFailures`.
class TestPocoHTTPServerWithFailures
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<HTTPRequestHandlerFactoryWithFailures> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    Poco::Net::MessageHeader last_request_header;
    std::string last_request_body;

public:
    explicit TestPocoHTTPServerWithFailures(size_t num_failures)
        : server_socket(std::make_unique<Poco::Net::ServerSocket>(0))
        , handler_factory(new HTTPRequestHandlerFactoryWithFailures(num_failures, last_request_header, last_request_body))
        , server_params(makeMockServerParams())
        , server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    ~TestPocoHTTPServerWithFailures()
    {
        server->stop();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    size_t requestCount() const
    {
        return handler_factory->requestCount();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_header;
    }

    const std::string & getLastRequestBody() const
    {
        return last_request_body;
    }
};

struct StsRequestInfo
{
    Poco::Net::MessageHeader headers;
    Poco::URI::QueryParameters query_params;
};

class MockStsRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MockStsRequestHandler(std::optional<StsRequestInfo> & last_request_info_, std::string role_access_key_, std::string role_secret_key_)
        : last_request_info(last_request_info_)
        , role_access_key(std::move(role_access_key_))
        , role_secret_key(std::move(role_secret_key_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        last_request_info.emplace();
        last_request_info->headers = request;

        /// The parameters arrive form-encoded in the POST body, as the AWS SDK sends them.
        Poco::Net::HTMLForm form(request, request.stream());
        last_request_info->query_params.assign(form.begin(), form.end());

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        auto & out = response.send();

        std::string result_xml = fmt::format(R"(
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
<AssumeRoleResult>
    <Credentials>
        <AccessKeyId>{}</AccessKeyId>
        <SecretAccessKey>{}</SecretAccessKey>
        <SessionToken>session_token</SessionToken>
        <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
</AssumeRoleResult>
</AssumeRoleResponse>)", role_access_key, role_secret_key);
        out << result_xml;
        out.flush();
    }
private:
    std::optional<StsRequestInfo> & last_request_info;
    std::string role_access_key;
    std::string role_secret_key;
};

class StsHTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    std::optional<StsRequestInfo> & last_request_info;
    std::string role_access_key;
    std::string role_secret_key;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockStsRequestHandler(last_request_info, role_access_key, role_secret_key);
    }
public:
    explicit StsHTTPRequestHandlerFactory(std::optional<StsRequestInfo> & last_request_info_, std::string role_access_key_, std::string role_secret_key_)
        : last_request_info(last_request_info_)
        , role_access_key(std::move(role_access_key_))
        , role_secret_key(std::move(role_secret_key_))
    {
    }

    ~StsHTTPRequestHandlerFactory() override = default;
};

class TestPocoHTTPStsServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<StsHTTPRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    std::optional<StsRequestInfo> last_request_info;

public:
    TestPocoHTTPStsServer(std::string role_access_key, std::string role_secret_key):
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new StsHTTPRequestHandlerFactory(last_request_info, std::move(role_access_key), std::move(role_secret_key))),
        server_params(makeMockServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    ~TestPocoHTTPStsServer()
    {
        server->stop();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    void resetLastRequest()
    {
        last_request_info.reset();
    }

    bool hasLastRequest() const
    {
        return last_request_info.has_value();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_info->headers;
    }

    const auto & getLastQueryParams() const
    {
        return last_request_info->query_params;
    }
};
