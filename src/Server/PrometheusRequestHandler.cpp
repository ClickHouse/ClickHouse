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
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);

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
createPrometheusHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics,
    const std::string & config_prefix)
{
    PrometheusMetricsWriter writer(config, config_prefix + ".handler", async_metrics);
    auto creator = [&server, writer]() -> std::unique_ptr<PrometheusRequestHandler>
    {
        return std::make_unique<PrometheusRequestHandler>(server, writer);
    };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusMainHandlerFactory(
    IServer & server, const Poco::Util::AbstractConfiguration & config, AsynchronousMetrics & async_metrics, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    PrometheusMetricsWriter writer(config, "prometheus", async_metrics);
    auto creator = [&server, writer]() -> std::unique_ptr<PrometheusRequestHandler>
    {
        return std::make_unique<PrometheusRequestHandler>(server, writer);
    };

    auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    handler->attachStrictPath(config.getString("prometheus.endpoint", "/metrics"));
    handler->allowGetAndHeadRequest();
    factory->addHandler(handler);
    return factory;
}
}
