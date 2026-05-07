#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

class IServer;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(
        IServer & server,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        UInt64 connection_id_);

    void run() override;

private:
    TCPServer & tcp_server;
    LoggerPtr log;
    UInt64 connection_id;
};

}
