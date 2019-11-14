#pragma once

#include "../IServer.h"
#include "../HTTPHandlerFactory.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with "Ok.\n". Used for availability checks.
class HTTPPingRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPPingRequestHandler(const IServer & server_) : server(server_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    const IServer & server;
};

}
