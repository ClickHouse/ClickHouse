#pragma once

#include <Access/Common/AuthenticationData.h>
#include <Storages/IKVStorage.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include <Common/config.h>
#include <Common/logger_useful.h>

#include "IServer.h"
#include "RedisProtocol.hpp"

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif


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
    RedisHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_);

    void run() final;

private:
    void makeSecureConnection();

    Int64 db = 0;
    IKVStoragePtr table_ptr;

    RedisProtocol::AuthenticationManager authentication_manager;
    bool authenticated = false;

    Poco::Logger * log = &Poco::Logger::get("RedisHandler");

    IServer & server;
    TCPServer & tcp_server;
#if USE_SSL
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
#endif
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;

    std::unique_ptr<Session> session;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::RedisConnection};
};

}
