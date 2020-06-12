#include "PrometheusRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>


namespace DB
{

void PrometheusRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        auto wb = WriteBufferFromHTTPServerResponse(request, response, keep_alive_timeout);
        metrics_writer.write(wb);
        wb.finalize();
    }
    catch (...)
    {
        tryLogCurrentException("PrometheusRequestHandler");
    }
}

void addPrometheusHandlerFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server, AsynchronousMetrics & async_metrics)
{
    /// We check that prometheus handler will be served on current (default) port.
    /// Otherwise it will be created separately, see below.
    if (server.config().has("prometheus") && server.config().getInt("prometheus.port", 0) == 0)
    {
        auto prometheus_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
            server, PrometheusMetricsWriter(server.config(), "prometheus", async_metrics));
        prometheus_handler->attachStrictPath(server.config().getString("prometheus.endpoint", "/metrics"))->allowGetAndHeadRequest();
        factory.addHandler(prometheus_handler.release());
    }
}

}
