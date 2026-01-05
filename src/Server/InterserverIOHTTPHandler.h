#pragma once

#include <Interpreters/InterserverCredentials.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>

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

class InterserverIOHTTPHandler : public HTTPRequestHandler
{
public:
    explicit InterserverIOHTTPHandler(IServer & server_)
        : server(server_)
        , log(getLogger("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponseBase & response) override;

private:
    using OutputPtr = std::shared_ptr<WriteBufferFromHTTPServerResponseBase>;

    IServer & server;
    LoggerPtr log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(HTTPServerRequest & request, HTTPServerResponseBase & response, OutputPtr used_output);

    std::pair<String, bool> checkAuthentication(HTTPServerRequest & request) const;
};

}
