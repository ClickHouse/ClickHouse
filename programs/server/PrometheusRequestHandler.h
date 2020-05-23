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
    explicit PrometheusRequestHandler(IServer & server_, const PrometheusMetricsWriter & metrics_writer_)
        : server(server_)
        , metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
