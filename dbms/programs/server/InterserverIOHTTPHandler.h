#pragma once

#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>

#include <Common/CurrentMetrics.h>

#include "IServer.h"


namespace CurrentMetrics
{
    extern const Metric InterserverConnection;
}

namespace DB
{

class InterserverIOHTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit InterserverIOHTTPHandler(IServer & server_)
        : server(server_)
        , log(&Poco::Logger::get("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    IServer & server;
    Poco::Logger * log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
