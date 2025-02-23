#include "MongoHandler.h"
#include <memory>
#include <Core/Settings.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Poco/Util/LayeredConfiguration.h>
#include "Common/Exception.h"
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include "Processors/QueryPlan/IQueryPlanStep.h"

#include <Core/Mongo/Handler.h>
#include <Core/Mongo/MongoProtocol.h>

#include <bson/bson.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool allow_settings_after_format_in_insert;
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_query_size;
extern const SettingsBool implicit_select;
}

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

MongoHandler::MongoHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    Int32 connection_id_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , connection_id(connection_id_)
    , read_event(read_event_)
    , write_event(write_event_)
{
    changeIO(socket());
}

void MongoHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket, read_event);
    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket, write_event);
    message_transport = std::make_shared<MongoProtocol::MessageTransport>(in.get(), out.get());
}

void MongoHandler::run()
{
    setThreadName("PostgresHandler");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGO);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    try
    {
        while (tcp_server.isOpen())
        {
            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;

            auto header_bytes = message_transport->receive<MongoProtocol::Header>();
            auto executor = std::make_shared<MongoProtocol::QueryExecutor>(session, socket().peerAddress());
            MongoProtocol::handle(*header_bytes, message_transport, executor);
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
}

}
