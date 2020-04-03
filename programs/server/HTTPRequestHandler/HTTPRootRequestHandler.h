#pragma once

#include "../IServer.h"
#include "../HTTPHandlerFactory.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with custom string. Can be used for browser.
class HTTPRootRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPRootRequestHandler(const IServer & server_) : server(server_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    const IServer & server;
};

}
