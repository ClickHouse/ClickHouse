#pragma once

#include <Interpreters/InterserverCredentials.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>
#include <Common/OpenTelemetryTraceContext.h>

#include <Poco/Logger.h>

#include <memory>
#include <string>


namespace CurrentMetrics
{
    extern const Metric InterserverConnection;
}

namespace DB
{

class IServer;
class WriteBufferFromHTTPServerResponse;

class InterserverIOHTTPHandler : public HTTPRequestHandler
{
public:
    explicit InterserverIOHTTPHandler(IServer & server_)
        : server(server_)
        , log(getLogger("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    using OutputPtr = std::shared_ptr<WriteBufferFromHTTPServerResponse>;

    IServer & server;
    LoggerPtr log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(HTTPServerRequest & request, HTTPServerResponse & response, OutputPtr used_output);

    /// Continues the caller's trace when the request carries W3C `traceparent`/`tracestate` headers:
    /// returns a holder whose root span is the `SERVER` span of this request, or null without the header.
    /// A malformed header is logged and ignored; the request is served regardless.
    OpenTelemetry::TracingContextHolderPtr startTracingContext(const HTTPServerRequest & request) const;

    std::pair<String, bool> checkAuthentication(HTTPServerRequest & request) const;
};

}
