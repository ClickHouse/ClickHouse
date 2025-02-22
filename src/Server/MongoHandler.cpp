#include "MongoHandler.h"
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
    //const char* msg1 = "I\1\0\0\5\0\0\0sH3f\1\0\0\0";
    //const char* msg2 = "\10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1\0\0\0%\1\0\0\10helloOk\0\1\10ismaster\0\1\3topologyVersion\0-\0\0\0\7processId\0g\257\264\205\352\214\273t_{g-\22counter\0\0\0\0\0\0\0\0\0\0\20maxBsonObjectSize\0\0\0\0\1\20maxMessageSizeBytes\0\0l\334\2\20maxWriteBatchSize\0\240\206\1\0\tlocalTime\0\1<Y\6\225\1\0\0\20logicalSessionTimeoutMinutes\0\36\0\0\0\20connectionId\0\3\0\0\0\20minWireVersion\0\0\0\0\0\20maxWireVersion\0\31\0\0\0\10readOnly\0\0\1ok\0\0\0\0\0\0\0\360?\0";

    setThreadName("PostgresHandler");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGO);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    try
    {
        if (!startup())
            return;

        while (tcp_server.isOpen())
        {
            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;

            std::cerr << "want to read new header\n";
            auto header_bytes = message_transport->receive<MongoProtocol::Header>();
            MongoProtocol::handle(*header_bytes, message_transport, session);
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
}

bool MongoHandler::startup()
{
    session->authenticate("default", "123", socket().peerAddress());
    //Int32 payload_size;
    //Int32 info;
    /*
    const auto & user_name = start_up_msg->user;
    authentication_manager.authenticate(user_name, *session, *message_transport, socket().peerAddress());

    try
    {
        session->makeSessionContext();
        session->sessionContext()->setDefaultFormat("PostgreSQLWire");
        if (!start_up_msg->database.empty())
            session->sessionContext()->setCurrentDatabase(start_up_msg->database);
    }
    catch (const Exception & exc)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "XX000", exc.message()),
            true);
        throw;
    }

    sendParameterStatusData(*start_up_msg);

    message_transport->send(
        PostgreSQLProtocol::Messaging::BackendKeyData(connection_id, secret_key), true);
*/
    LOG_DEBUG(log, "Successfully finished Startup stage");
    return true;
}

void MongoHandler::cancelRequest()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CancelRequest> msg
        = message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::CancelRequest>(8);

    String query = fmt::format("KILL QUERY WHERE query_id = 'postgres:{:d}:{:d}'", msg->process_id, msg->secret_key);
    ReadBufferFromString replacement(query);

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId("");
    executeQuery(replacement, *out, true, query_context, {});
}


void MongoHandler::processQuery()
{
}

}
