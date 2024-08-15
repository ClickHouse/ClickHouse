#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

#include "PrometheusMetricsWriter.h"

namespace DB
{

class IServer;

class PrometheusRequestHandler : public HTTPRequestHandler
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

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
