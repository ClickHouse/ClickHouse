#pragma once

#include <Core/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Server/PrometheusMetricsOnlyRequestHandler.h>
#include "config.h"


namespace DB
{
class HTMLForm;
class Session;
class Credentials;

/// Handles requests from Prometheus including both metrics ("/metrics") and API protocols (remote write, remote read, query).
class PrometheusRequestHandler : public PrometheusMetricsOnlyRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusRequestHandler() override;

protected:
    void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void onException() override;

private:
    bool authenticateUserAndMakeContext(HTTPServerRequest & request, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response);
    void makeContext(HTTPServerRequest & request);

    void wrapHandler(HTTPServerRequest & request, HTTPServerResponse & response, bool authenticate, std::function<void()> && func);

#if !USE_PROMETHEUS_PROTOBUFS
    [[noreturn]]
#endif
    void handleRemoteWriteImpl(HTTPServerRequest & request, HTTPServerResponse & response);

    const Settings & default_settings;
    std::unique_ptr<HTMLForm> params;
    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextMutablePtr context;
};

}
