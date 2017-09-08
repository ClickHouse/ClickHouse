#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with "Ok.\n". Used for availability checks.
class PingRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;

public:
    explicit PingRequestHandler(IServer & server_) : server(server_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
