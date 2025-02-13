#include "MongoHandler.h"
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include "Common/Exception.h"
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>

#include <Core/MongoProtocol.h>

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
    message_transport = std::make_shared<PostgreSQLProtocol::Messaging::MessageTransport>(in.get(), out.get());
}

std::string bsonToJson(const std::string& bsonData) {
    bson_t b;
    //bson_error_t error;

    if (!bson_init_static(&b, reinterpret_cast<const uint8_t*>(bsonData.data()), bsonData.size())) {
        throw std::runtime_error("Failed to initialize BSON data");
    }

    char* json_str = bson_as_canonical_extended_json(&b, nullptr);
    if (!json_str) {
      // Try to get the error message and throw it
      //char *err_msg = bson_error_to_string(&error, "bson_as_canonical_extended_json() failed: ");
      //std::string error_message(err_msg);
      //bson_free(err_msg);
      throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect bson");
    }


    std::string json(json_str);
    bson_free(json_str);

    return json;
}

void MongoHandler::run()
{
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
            auto header_bytes = message_transport->receive<MongoProtocol::Header>();
            std::cerr << "recieved from message " << header_bytes->message_length << ' ' << header_bytes->request_id << ' ' <<  header_bytes->operation_code << ' ' << header_bytes->response_to << '\n';
            if (header_bytes->operation_code == static_cast<int>(MongoProtocol::OperationCode::OP_QUERY))
            {
                std::cerr << "OP QUERY\n";

                auto content_bytes = message_transport->receive<MongoProtocol::OperationQuery>();
                std::cerr << "query " << content_bytes->size_query << ' ' << content_bytes->query << '\n';

                std::cerr << "decoded query " << bsonToJson(content_bytes->query) << '\n';
            }
        }
    }
    catch (const Poco::Exception &exc)
    {
        log->log(exc);
    }

}

bool MongoHandler::startup()
{
    return true;
}

void MongoHandler::cancelRequest()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CancelRequest> msg =
        message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::CancelRequest>(8);

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
