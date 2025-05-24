#include <cstddef>
#include <memory>
#include <Poco/Exception.h>
#include <Poco/Format.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Util/LayeredConfiguration.h>

#include "RedisHandler.h"
#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "Common/setThreadName.h"
#include "Interpreters/ClientInfo.h"
#include "Interpreters/Session.h"
#include "Server/RedisProtocolRequest.h"
#include "Server/RedisProtocolResponse.h"
#include "base/scope_guard.h"

namespace DB 
{

RedisHandler::RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RedisProtocol::Config & config_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_), config(config_)
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
        while(tcp_server.isOpen() && process_request()) {}
    } 
    catch (const Poco::Exception & e) 
    {
        log->log(e);
        RedisProtocol::ErrorResponse resp(e.message());
        resp.serialize(*out);
    } 
}

bool RedisHandler::process_request()
{
    SCOPE_EXIT(out->next());
    RedisProtocol::RedisRequest req;
    req.deserialize(*in);
    switch(req.getCommand()) 
    {
        case RedisProtocol::CommandType::AUTH:
        {
            LOG_DEBUG(log, "AUTH request");
            // TODO add authentication

            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::ECHO:
        {
            LOG_DEBUG(log, "ECHO request");
            RedisProtocol::EchoRequest echo_request(req);
            echo_request.deserialize(*in);

            RedisProtocol::SimpleStringResponse resp(echo_request.getCommandInput());
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::PING:
        {
            LOG_DEBUG(log, "PING request");
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::PONG);
            resp.serialize(*out);
            return true;
        }
        case RedisProtocol::CommandType::QUIT:
        {
            LOG_DEBUG(log, "QUIT request");
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return false;
        }
        case RedisProtocol::CommandType::SELECT:
        {
            LOG_DEBUG(log, "SELECT request");
            RedisProtocol::SelectRequest select_request(req);
            select_request.deserialize(*in);

            auto selected_db = select_request.getDB();
            if (config.redis_click_house_mapping.find(selected_db) == config.redis_click_house_mapping.end())
            {
                RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NO_SUCH_DB);
                resp.serialize(*out);
            }

            db = selected_db;
            RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
            resp.serialize(*out);
            return true;
        }
    }
}

}
