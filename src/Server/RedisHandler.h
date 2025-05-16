#pragma once

#include <memory>
#include <Poco/Logger.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include "IO/ReadBufferFromPocoSocket.h"
#include "IO/WriteBufferFromPocoSocket.h"
#include "IServer.h"
#include "Server/TCPServer.h"


namespace DB 
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection 
{
public:
    RedisHandler(IServer& server_, TCPServer& tcp_server_, const Poco::Net::StreamSocket& socket_); 

    void run() final;

private:

    void process_request();

    IServer & server;
    TCPServer & tcp_server;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::unique_ptr<Session> session;
    
    Int64 db = 0;

    Poco::Logger * log = &Poco::Logger::get("RedisHandler");
};

}
