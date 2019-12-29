#pragma once

#include "IServer.h"
#include "PrometheusMetricsWriter.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace DB
{

class PrometheusRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;
    const PrometheusMetricsWriter & metrics_writer;

public:
    explicit PrometheusRequestHandler(IServer & server_, PrometheusMetricsWriter & metrics_writer_)
        : server(server_)
        , metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};


template <typename HandlerType>
class PrometeusRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer & server;
    std::string endpoint_path;
    PrometheusMetricsWriter metrics_writer;

public:
    PrometeusRequestHandlerFactory(IServer & server_, const AsynchronousMetrics & async_metrics_)
        : server(server_)
        , endpoint_path(server_.config().getString("prometheus.endpoint", "/metrics"))
        , metrics_writer(server_.config(), "prometheus", async_metrics_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
            && request.getURI() == endpoint_path)
            return new HandlerType(server, metrics_writer);

        return nullptr;
    }
};

using PrometeusHandlerFactory = PrometeusRequestHandlerFactory<PrometheusRequestHandler>;

}
