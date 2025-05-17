#include "RedisHandler.h"
#include <memory>
#include <Poco/Exception.h>
#include <Poco/Format.h>
#include <Poco/Net/TCPServerConnection.h>
#include "Common/logger_useful.h"
#include "Common/setThreadName.h"
#include "IO/ReadBufferFromPocoSocket.h"
#include "IO/WriteBufferFromPocoSocket.h"
#include "RedisProtocol.h"
#include "base/scope_guard.h"

namespace DB 
{

RedisHandler::RedisHandler(IServer& server_, TCPServer& tcp_server_, const Poco::Net::StreamSocket& socket_) 
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
}

void RedisHandler::run()
{
    setThreadName("RedisHandler");
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

    try 
    {
        while(tcp_server.isOpen()) 
        {
            process_request();
        }
    } 
    catch (const Poco::Exception & e) 
    {
        log->log(e);
        RedisProtocol::ErrorResponse resp(e.message());
        resp.serialize(*out);
    } 
}

void RedisHandler::process_request()
{
    SCOPE_EXIT(out->next());
    RedisProtocol::RedisRequest req;
    req.deserialize(*in);
    switch(req.getCommand()) 
    {
        case RedisProtocol::CommandType::PING:
            LOG_DEBUG(log, "PING request");
            RedisProtocol::Writer writer(out.get());
            writer.writeSimpleString(RedisProtocol::Message::PONG);
            return;
        case RedisProtocol::CommandType::SELECT:
            LOG_DEBUG(log, "SELECT request");
            RedisProtocol::SelectRequest select_request(req);
            select_request.deserialize(*in);
            
            auto db = select_request.getDB();

            return;

    }
    // unreachable
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Command handler for {} was not implemented", toString(req.getCommand())
    );
}

}
