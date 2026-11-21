#pragma once

#include <atomic>

#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <base/types.h>

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
    std::atomic<UInt64> last_connection_id = 0;
};

}
