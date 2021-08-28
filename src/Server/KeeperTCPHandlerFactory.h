#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class IServer;

class KeeperTCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
public:
    KeeperTCPHandlerFactory(IServer & server_, bool secure);
    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;

private:
    IServer & server;
    Poco::Logger * log;
};

}
