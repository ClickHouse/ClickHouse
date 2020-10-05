#pragma once

#include <Poco/Net/HTTPServer.h>
#include "IRoutineServer.h"

namespace DB
{

class HTTPServer : public IRoutineServer, public Poco::Net::HTTPServer
{
public:
    HTTPServer(
        Poco::Net::HTTPRequestHandlerFactory::Ptr pFactory,
        Poco::ThreadPool & threadPool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr pParams);

    void start() override;

    void stop() override;

    int currentConnections() const override;
};

}
