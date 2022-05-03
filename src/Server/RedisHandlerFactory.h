#pragma once

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>

namespace DB
{

class RedisHandlerFactory : public TCPServerConnectionFactory
{
private:
    Poco::Logger * log;

public:
    explicit RedisHandlerFactory(IServer & server_);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
};
}
