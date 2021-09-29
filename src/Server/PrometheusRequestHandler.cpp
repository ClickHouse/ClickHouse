#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{
void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        if (request.isSecure())
        {
            size_t hsts_max_age = config.getUInt64("hsts_max_age", 0);

            if (hsts_max_age > 0)
                response.add("Strict-Transport-Security", "max-age=" + std::to_string(hsts_max_age));
        }

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);
        try
        {
            metrics_writer.write(wb);
            wb.finalize();
        }
        catch (...)
        {
            wb.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException("PrometheusRequestHandler");
    }
}

HTTPRequestHandlerFactoryPtr
createPrometheusHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & config_prefix)
{
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
        server, PrometheusMetricsWriter(server.config(), config_prefix + ".handler", async_metrics));
    factory->addFiltersFromConfig(server.config(), config_prefix);
    return factory;
}

}
