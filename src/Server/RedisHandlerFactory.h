#pragma once

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Logger.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include "RedisProtocolMapping.h"

namespace DB
{
    class RedisHandlerFactory : public TCPServerConnectionFactory
    {
    public:
        explicit RedisHandlerFactory(IServer &_server);
        Poco::Net::TCPServerConnection* createConnection(const Poco::Net::StreamSocket &socket, TCPServer &tcp_server) override;

    private:
        void parse_config();

        IServer &server;
        Poco::Logger *logger;
        RedisProtocol::ConfigPtr config;
    };
}
