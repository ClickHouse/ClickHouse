#pragma once

#include "Server.h"
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric InterserverConnection;
}

namespace DB
{

class InterserverIOHTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    InterserverIOHTTPHandler(IServer & server_)
        : server(server_)
        , log(&Logger::get("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    IServer & server;
    Logger * log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
