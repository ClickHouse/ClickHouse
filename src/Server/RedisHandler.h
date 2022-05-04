#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include <Common/config.h>
#include <Common/logger_useful.h>
#include "IServer.h"


namespace CurrentMetrics
{
extern const Metric RedisConnection;
}

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** Redis serialization protocol implementation.
 * For more info see https://redis.io/docs/reference/protocol-spec/
 */
class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(const Poco::Net::StreamSocket & socket_, TCPServer & tcp_server_);

    void run() final;

private:
    Poco::Logger * log = &Poco::Logger::get("RedisHandler");

    TCPServer & tcp_server;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::RedisConnection};
};

}
