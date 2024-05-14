#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/IServer.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandlerConfig.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace DB
{

PrometheusRequestHandler::PrometheusRequestHandler(
    IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_, const PrometheusMetricsWriterPtr & metrics_writer_)
    : server(server_)
    , config(config_)
    , async_metrics(async_metrics_)
    , metrics_writer(metrics_writer_)
    , log(getLogger("PrometheusRequestHandler"))
{
}

PrometheusRequestHandler::~PrometheusRequestHandler() = default;

void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    chassert(config->metrics);
    const auto & metrics_config = *config->metrics;

    try
    {
        /// In order to make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        auto keep_alive_timeout = config->keep_alive_timeout;
        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout, write_event);
        try
        {
            if (metrics_config.send_events)
                metrics_writer->writeEvents(wb);

            if (metrics_config.send_metrics)
                metrics_writer->writeMetrics(wb);

            if (metrics_config.send_asynchronous_metrics)
                metrics_writer->writeAsynchronousMetrics(wb, async_metrics);

            if (metrics_config.send_errors)
                metrics_writer->writeErrors(wb);

            wb.finalize();
        }
        catch (...)
        {
            wb.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

}
