#pragma once

#include "../HTTPHandlerFactory.h"
#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

class Context;

/// Replies "Ok.\n" if all replicas on this server don't lag too much. Otherwise output lag information.
class HTTPReplicasStatusRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPReplicasStatusRequestHandler(Context & context_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Context & context;
};


}
