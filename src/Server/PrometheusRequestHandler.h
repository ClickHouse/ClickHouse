#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{
class IServer;
class AsynchronousMetrics;
class PrometheusMetricsWriter;
using PrometheusMetricsWriterPtr = std::shared_ptr<const PrometheusMetricsWriter>;
struct PrometheusRequestHandlerConfig;
using PrometheusRequestHandlerConfigPtr = std::shared_ptr<const PrometheusRequestHandlerConfig>;

class PrometheusRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_, const PrometheusMetricsWriterPtr & metrics_writer_);
    ~PrometheusRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

protected:
    IServer & server;
    const PrometheusRequestHandlerConfigPtr config;
    const AsynchronousMetrics & async_metrics;
    const PrometheusMetricsWriterPtr metrics_writer;
    const LoggerPtr log;
};

}
