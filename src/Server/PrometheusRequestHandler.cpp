#include "PrometheusRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerRequestFilter.h>


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

Poco::Net::HTTPRequestHandlerFactory * createPrometheusHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & config_prefix)
{
    return addFiltersFromConfig(new HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>(
        server, PrometheusMetricsWriter(server.config(), config_prefix + ".handler", async_metrics)), server.config(), config_prefix);
}

}
