#include <memory>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/TCPServer.h>
#include "Common/AsyncTaskExecutor.h"
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/config_version.h>
#include "Interpreters/Session.h"
#include <Server/MongoDBHandler.h>

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#   include <Poco/Net/SSLManager.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

MongoDBHandler::MongoDBHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    Int32 connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , connection_id(connection_id_)
{
}




void MongoDBHandler::run()
{
    LOG_INFO(log, "MONGODB has new connection");
    setThreadName(this->handler_name);
    ThreadStatus thread_status;

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGODB);
    SCOPE_EXIT({ session.reset(); });
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    session->setClientConnectionId(connection_id);

    char c;
    while (tcp_server.isOpen()) {
        if (!in->read(c)) {
            break;
        }
        out->write(c);
        out->next();
    }
    out->finalize();
}

}
