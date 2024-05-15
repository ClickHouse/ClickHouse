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
    virtual void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) = 0;

    /// Returns the write buffer used for the current HTTP response.
    WriteBuffer & getOutputStream(HTTPServerResponse & response);

    IServer & server;
    const PrometheusRequestHandlerConfigPtr config;
    const LoggerPtr log;
    String http_method;

private:
    std::unique_ptr<WriteBufferFromHTTPServerResponse> out;
    ProfileEvents::Event write_event;
};

}
