#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{
class AsynchronousMetrics;
class IServer;
class PrometheusMetricsWriter;
class WriteBufferFromHTTPServerResponseBase;

/// Handles requests for prometheus protocols (expose_metrics, remote_write, remote-read).
class PrometheusRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusRequestHandler(
        IServer & server_,
        const PrometheusRequestHandlerConfig & config_,
        const AsynchronousMetrics & async_metrics_,
        std::shared_ptr<PrometheusMetricsWriter> metrics_writer_,
        std::unordered_map<String, String> response_headers_ = {});
    ~PrometheusRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponseBase & response) override;

private:
    /// Creates an internal implementation based on which PrometheusRequestHandlerConfig::Type is used.
    void createImpl();

    /// Returns the write buffer used for the current HTTP response.
    WriteBufferFromHTTPServerResponseBase & getOutputStream(HTTPServerResponseBase & response);

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
    class QueryAPIImpl;
    std::unique_ptr<Impl> impl;

    String http_method;
    std::unique_ptr<WriteBufferFromHTTPServerResponseBase> write_buffer_from_response;
    bool send_stacktrace = false;
    std::unordered_map<String, String> response_headers;
};

}
