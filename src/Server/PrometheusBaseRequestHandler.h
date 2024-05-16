#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{
class IServer;
class WriteBufferFromHTTPServerResponse;
struct PrometheusRequestHandlerConfig;
using PrometheusRequestHandlerConfigPtr = std::shared_ptr<const PrometheusRequestHandlerConfig>;

/// Base class for PrometheusRequestHandler and KeeperPrometheusRequestHandler.
class PrometheusBaseRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusBaseRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_);
    ~PrometheusBaseRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_) override;

protected:
    /// Writes the current metrics to the response in the Prometheus format.
    virtual void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) { handlerNotFound(request, response); }

    /// Throws an exception that there is no handler for that path.
    void handlerNotFound(HTTPServerRequest & request, HTTPServerResponse & response);

    /// Returns the write buffer used for the current HTTP response.
    WriteBuffer & getOutputStream(HTTPServerResponse & response);

    /// Writes the current exception to the response.
    void trySendExceptionToClient(const String & exception_message, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);

    /// Overridden in derived classes to do extra work after handling an exception in handleRequest().
    virtual void onException() {}

    IServer & server;
    const PrometheusRequestHandlerConfigPtr config;
    const LoggerPtr log;
    String http_method;
    bool send_stacktrace = false;

private:
    /// Calls onException() in a try-catch block.
    void tryCallOnException();

    std::unique_ptr<WriteBufferFromHTTPServerResponse> out;
    ProfileEvents::Event write_event;
};

}
