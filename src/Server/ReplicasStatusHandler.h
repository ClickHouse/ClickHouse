#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

class Context;

/// Replies "Ok.\n" if all replicas on this server don't lag too much. Otherwise output lag information.
class ReplicasStatusHandler : public Poco::Net::HTTPRequestHandler
{
private:
    Context & context;

public:
    explicit ReplicasStatusHandler(IServer & server_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;
};


}
