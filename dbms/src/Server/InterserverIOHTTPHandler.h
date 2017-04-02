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
    InterserverIOHTTPHandler(Server & server_)
        : server(server_)
        , log(&Logger::get("InterserverIOHTTPHandler"))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Server & server;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};
    Logger * log;

     void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
