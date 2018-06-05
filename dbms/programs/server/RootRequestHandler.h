#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with custom string. Can be used for browser.
class RootRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;

public:
    explicit RootRequestHandler(IServer & server_) : server(server_)
    {
    }

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
