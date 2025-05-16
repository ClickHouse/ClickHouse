#pragma once

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Logger.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

namespace DB {
    class RedisHandlerFactory : public TCPServerConnectionFactory {
    public:
        explicit RedisHandlerFactory(IServer &_server);
        Poco::Net::TCPServerConnection* createConnection(const Poco::Net::StreamSocket &socket, TCPServer &tcp_server) override;

    private:
        IServer &server;
        Poco::Logger *logger;
    };
}
