#pragma once

#include <Poco/Net/TCPServer.h>
#include "IRoutineServer.h"

namespace DB
{

class TCPServer : public IRoutineServer, public Poco::Net::TCPServer
{
public:
    TCPServer(
        Poco::Net::TCPServerConnectionFactory::Ptr pFactory,
        Poco::ThreadPool& threadPool,
        const Poco::Net::ServerSocket& socket,
        Poco::Net::TCPServerParams::Ptr pParams);

    void start() override;

    void stop() override;

    int currentConnections() const override;
};

}
