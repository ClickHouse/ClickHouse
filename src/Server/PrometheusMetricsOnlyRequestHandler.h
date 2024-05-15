#pragma once

#include <Server/PrometheusBaseRequestHandler.h>


namespace DB
{
class AsynchronousMetrics;
class PrometheusMetricsWriter;

/// The class uses PrometheusMetricsWriter to generate a response containing
/// the current metrics of ClickHouse in the Prometheus format.
class PrometheusMetricsOnlyRequestHandler : public PrometheusBaseRequestHandler
{
public:
    PrometheusMetricsOnlyRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_);

protected:
    void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) override;
    virtual std::unique_ptr<PrometheusMetricsWriter> createMetricsWriter() const;

    const AsynchronousMetrics & async_metrics;
};


using PrometheusRequestHandler = PrometheusMetricsOnlyRequestHandler;


/// The class uses KeeperPrometheusMetricsWriter to generate a response containing
/// the current metrics of ClickHouse Keeper in the Prometheus format.
class KeeperPrometheusRequestHandler : public PrometheusMetricsOnlyRequestHandler
{
public:
    using PrometheusMetricsOnlyRequestHandler::PrometheusMetricsOnlyRequestHandler;

protected:
    std::unique_ptr<PrometheusMetricsWriter> createMetricsWriter() const override;
};

}
