#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

#include "PrometheusMetricsWriter.h"

namespace DB
{

class IServer;

class PrometheusRequestHandler : public HTTPRequestHandler
{
private:
    PrometheusMetricsWriterPtr metrics_writer;

public:
    explicit PrometheusRequestHandler(PrometheusMetricsWriterPtr metrics_writer_) : metrics_writer(std::move(metrics_writer_)) { }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
