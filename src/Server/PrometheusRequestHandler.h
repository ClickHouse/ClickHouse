#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{
class AsynchronousMetrics;
class IServer;
class PrometheusMetricsWriter;
class WriteBufferFromHTTPServerResponse;

/// Handles requests for prometheus protocols (expose_metrics, remote_write, remote-read).
class PrometheusRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfig & config_,
                             const AsynchronousMetrics & async_metrics_, std::shared_ptr<PrometheusMetricsWriter> metrics_writer_);
    ~PrometheusRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_) override;

private:
    /// Creates an internal implementation based on which PrometheusRequestHandlerConfig::Type is used.
    void createImpl();

    /// Returns the write buffer used for the current HTTP response.
    WriteBufferFromHTTPServerResponse & getOutputStream(HTTPServerResponse & response);

    /// Finalizes the output stream and sends the response to the client.
    void finalizeResponse(HTTPServerResponse & response);
    void tryFinalizeResponse(HTTPServerResponse & response);

    /// Writes the current exception to the response.
    void trySendExceptionToClient(const String & exception_message, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);

    /// Calls onException() in a try-catch block.
    void tryCallOnException();

    IServer & server;
    const PrometheusRequestHandlerConfig config;
    const AsynchronousMetrics & async_metrics;
    const std::shared_ptr<PrometheusMetricsWriter> metrics_writer;
    const LoggerPtr log;

    class Impl;
    class ImplWithContext;
    class ExposeMetricsImpl;
    class RemoteWriteImpl;
    class RemoteReadImpl;
    std::unique_ptr<Impl> impl;

    String http_method;
    bool send_stacktrace = false;
    std::unique_ptr<WriteBufferFromHTTPServerResponse> write_buffer_from_response;
    bool response_finalized = false;
    ProfileEvents::Event write_event;
};

}
