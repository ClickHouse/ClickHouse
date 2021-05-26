#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/InterserverCredentials.h>

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
        , log(&Poco::Logger::get("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    struct Output
    {
        std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
    };

    IServer & server;
    Poco::Logger * log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output);

    std::pair<String, bool> checkAuthentication(HTTPServerRequest & request) const;
};

}
