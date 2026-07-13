#pragma once

#include <map>
#include <memory>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Server/IServer.h>
#include <Server/RedisProtocolMapping.h>
#include <base/types.h>
#include <Common/Logger.h>

namespace DB
{

class Session;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RedisProtocol::ConfigPtr config_);

    void run() final;

private:
    /// Returns false when the client has asked to close the connection.
    bool processRequest();

    void initDB(UInt32 db_);
    void checkDBSet() const;

    IServer & server;
    TCPServer & tcp_server;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::unique_ptr<Session> session;

    RedisProtocol::ConfigPtr config;
    UInt32 db = RedisProtocol::DB_MAX_NUM;
    std::map<UInt32, RedisProtocol::MappingPtr> redis_clickhouse_mapping;

    LoggerPtr log = getLogger("RedisHandler");
};

}
