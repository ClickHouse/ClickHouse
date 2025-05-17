#include "RedisHandlerFactory.h"
#include "RedisHandler.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Util/LayeredConfiguration.h>
#include "Common/logger_useful.h"

namespace DB{
    RedisHandlerFactory::RedisHandlerFactory(IServer &_server) : 
        server(_server), logger(&Poco::Logger::get("RedisHandlerFactory")) {

        }

    Poco::Net::TCPServerConnection* RedisHandlerFactory::createConnection(const Poco::Net::StreamSocket &socket, TCPServer &tcp_server) {
        LOG_TRACE(logger, "Redis connection. Address: {}", socket.peerAddress().toString());
        return new RedisHandler(server, tcp_server, socket);
    }
}
