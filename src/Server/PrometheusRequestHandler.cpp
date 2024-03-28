#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include "Server/PrometheusMetricsWriter.h"

#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{
void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    try
    {
        /// In order to make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, write_event);
        try
        {
            metrics_writer->write(wb);
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

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics,
    const std::string & config_prefix)
{
    auto writer = std::make_shared<PrometheusMetricsWriter>(config, config_prefix + ".handler", async_metrics);
    auto creator = [writer]() -> std::unique_ptr<PrometheusRequestHandler> { return std::make_unique<PrometheusRequestHandler>(writer); };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusMainHandlerFactory(
    const Poco::Util::AbstractConfiguration & config, PrometheusMetricsWriterPtr metrics_writer, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    auto creator = [metrics_writer] { return std::make_unique<PrometheusRequestHandler>(metrics_writer); };

    auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    handler->attachStrictPath(config.getString("prometheus.endpoint", "/metrics"));
    handler->allowGetAndHeadRequest();
    factory->addHandler(handler);
    return factory;
}
}
