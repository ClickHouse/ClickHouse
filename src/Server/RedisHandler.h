#pragma once

#include <memory>
#include <Poco/Logger.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include "IO/ReadBufferFromPocoSocket.h"
#include "IO/WriteBufferFromPocoSocket.h"
#include "IServer.h"
#include "Server/TCPServer.h"
#include "RedisProtocolMapping.h"
#include "base/types.h"

namespace DB
{

class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RedisProtocol::ConfigPtr config_);

    void run() final;

private:

    bool processRequest();

    void initDB(UInt32 db_);
    void isDBSet() const;

    std::vector<std::string> getValueByKey(const String & key);

    IServer & server;
    TCPServer & tcp_server;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::unique_ptr<Session> session;

    RedisProtocol::ConfigPtr config;
    UInt32 db = RedisProtocol::DB_MAX_NUM;
    std::map<UInt32, RedisProtocol::MappingPtr> redis_click_house_mapping;

    Poco::Logger * log = &Poco::Logger::get("RedisHandler");
};

}
