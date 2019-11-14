#pragma once

#include "../IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>

#include <Common/CurrentMetrics.h>
#include <Common/HTMLForm.h>

#include <DataStreams/HTTPOutputStreams.h>


namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

template <typename QueryParamExtractor>
class HTTPQueryRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPQueryRequestHandler(const IServer & server_, const QueryParamExtractor & extractor_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    const IServer & server;
    Poco::Logger * log;
    QueryParamExtractor extractor;

    /// It is the name of the server that will be sent in an http-header X-ClickHouse-Server-Display-Name.
    String server_display_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    HTTPResponseBufferPtr createResponseOut(HTTPServerRequest & request, HTTPServerResponse & response);

    void processQuery(
        Context & context, Poco::Net::HTTPServerRequest & request, HTMLForm & params,
        Poco::Net::HTTPServerResponse & response, HTTPResponseBufferPtr & response_out);
};

}
