#pragma once

#include <memory>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric InterserverConnection;
}

namespace DB
{

class IServer;
class WriteBufferFromHTTPServerResponse;

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
    struct Output
    {
        std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
    };

    IServer & server;
    Poco::Logger * log;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};

    void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, Output & used_output);

    std::pair<String, bool> checkAuthentication(Poco::Net::HTTPServerRequest & request) const;
};

}
