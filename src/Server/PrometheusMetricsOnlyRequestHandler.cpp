#include <Server/PrometheusMetricsOnlyRequestHandler.h>

#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{

PrometheusMetricsOnlyRequestHandler::PrometheusMetricsOnlyRequestHandler(
    IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_)
    : PrometheusBaseRequestHandler(server_, config_)
    , async_metrics(async_metrics_)
{
}

void PrometheusMetricsOnlyRequestHandler::handleMetrics(HTTPServerRequest & /* request */, HTTPServerResponse & response)
{
    chassert(config->metrics);
    const auto & metrics_config = *config->metrics;

    auto metrics_writer = createMetricsWriter();

    response.setContentType("text/plain; version=0.0.4; charset=UTF-8");
    auto & out = getOutputStream(response);

    if (metrics_config.send_events)
        metrics_writer->writeEvents(out);

    if (metrics_config.send_metrics)
        metrics_writer->writeMetrics(out);

    if (metrics_config.send_asynchronous_metrics)
        metrics_writer->writeAsynchronousMetrics(out, async_metrics);

    if (metrics_config.send_errors)
        metrics_writer->writeErrors(out);
}


std::unique_ptr<PrometheusMetricsWriter> PrometheusMetricsOnlyRequestHandler::createMetricsWriter() const
{
    return std::make_unique<PrometheusMetricsWriter>();
}


std::unique_ptr<PrometheusMetricsWriter> KeeperPrometheusRequestHandler::createMetricsWriter() const
{
    return std::make_unique<KeeperPrometheusMetricsWriter>();
}

}
