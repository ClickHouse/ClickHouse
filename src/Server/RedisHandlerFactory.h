#pragma once

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/Logger.h>

namespace DB
{

class RedisHandlerFactory : public TCPServerConnectionFactory
{
public:
    explicit RedisHandlerFactory(IServer & server_);

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;

private:
    IServer & server;
    LoggerPtr log;
};

}
