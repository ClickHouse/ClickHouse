#include "MySQLHandler.h"

#include <limits>
#include <ext/scope_guard.h>
#include <Columns/ColumnVector.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Core/MySQLProtocol.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/copyData.h>
#include <Interpreters/executeQuery.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Storages/IStorage.h>
#include <boost/algorithm/string/replace.hpp>
#include <regex>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

#if USE_SSL
#    include <Poco/Crypto/CipherFactory.h>
#    include <Poco/Crypto/RSAKey.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{

using namespace MySQLProtocol;

#if USE_SSL
using Poco::Net::SecureStreamSocket;
using Poco::Net::SSLManager;
#endif

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES;
    extern const int SUPPORT_IS_DISABLED;
}

MySQLHandler::MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_,
    bool ssl_enabled, size_t connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("MySQLHandler"))
    , connection_context(server.context())
    , connection_id(connection_id_)
    , auth_plugin(new MySQLProtocol::Authentication::Native41())
{
    server_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;
    if (ssl_enabled)
        server_capability_flags |= CLIENT_SSL;
}

void MySQLHandler::run()
{
    connection_context.makeSessionContext();
    connection_context.setDefaultFormat("MySQLWire");

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    packet_sender = std::make_shared<PacketSender>(*in, *out, connection_context.mysql.sequence_id);

    try
    {
        Handshake handshake(server_capability_flags, connection_id, VERSION_STRING + String("-") + VERSION_NAME, auth_plugin->getName(), auth_plugin->getAuthPluginData());
        packet_sender->sendPacket<Handshake>(handshake, true);

        LOG_TRACE(log, "Sent handshake");

        HandshakeResponse handshake_response;
        finishHandshake(handshake_response);
        connection_context.mysql.client_capabilities = handshake_response.capability_flags;
        if (handshake_response.max_packet_size)
            connection_context.mysql.max_packet_size = handshake_response.max_packet_size;
        if (!connection_context.mysql.max_packet_size)
            connection_context.mysql.max_packet_size = MAX_PACKET_LENGTH;

        LOG_TRACE(log,
            "Capabilities: {}, max_packet_size: {}, character_set: {}, user: {}, auth_response length: {}, database: {}, auth_plugin_name: {}",
            handshake_response.capability_flags,
            handshake_response.max_packet_size,
            static_cast<int>(handshake_response.character_set),
            handshake_response.username,
            handshake_response.auth_response.length(),
            handshake_response.database,
            handshake_response.auth_plugin_name);

        client_capability_flags = handshake_response.capability_flags;
        if (!(client_capability_flags & CLIENT_PROTOCOL_41))
            throw Exception("Required capability: CLIENT_PROTOCOL_41.", ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES);

        authenticate(handshake_response.username, handshake_response.auth_plugin_name, handshake_response.auth_response);

        try
        {
            if (!handshake_response.database.empty())
                connection_context.setCurrentDatabase(handshake_response.database);
            connection_context.setCurrentQueryId("");
        }
        catch (const Exception & exc)
        {
            log->log(exc);
            packet_sender->sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
        }

        OK_Packet ok_packet(0, handshake_response.capability_flags, 0, 0, 0);
        packet_sender->sendPacket(ok_packet, true);

        while (true)
        {
            packet_sender->resetSequenceId();
            PacketPayloadReadBuffer payload = packet_sender->getPayload();

            char command = 0;
            payload.readStrict(command);

            // For commands which are executed without MemoryTracker.
            LimitReadBuffer limited_payload(payload, 10000, true, "too long MySQL packet.");

            LOG_DEBUG(log, "Received command: {}. Connection id: {}.",
                static_cast<int>(static_cast<unsigned char>(command)), connection_id);

            try
            {
                switch (command)
                {
                    case COM_QUIT:
                        return;
                    case COM_INIT_DB:
                        comInitDB(limited_payload);
                        break;
                    case COM_QUERY:
                        comQuery(payload);
                        break;
                    case COM_FIELD_LIST:
                        comFieldList(limited_payload);
                        break;
                    case COM_PING:
                        comPing();
                        break;
                    default:
                        throw Exception(Poco::format("Command %d is not implemented.", command), ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            catch (const NetException & exc)
            {
                log->log(exc);
                throw;
            }
            catch (const Exception & exc)
            {
                log->log(exc);
                packet_sender->sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
            }
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
}

/** Reads 3 bytes, finds out whether it is SSLRequest or HandshakeResponse packet, starts secure connection, if it is SSLRequest.
 *  Reading is performed from socket instead of ReadBuffer to prevent reading part of SSL handshake.
 *  If we read it from socket, it will be impossible to start SSL connection using Poco. Size of SSLRequest packet payload is 32 bytes, thus we can read at most 36 bytes.
 */
void MySQLHandler::finishHandshake(MySQLProtocol::HandshakeResponse & packet)
{
    size_t packet_size = PACKET_HEADER_SIZE + SSL_REQUEST_PAYLOAD_SIZE;

    /// Buffer for SSLRequest or part of HandshakeResponse.
    char buf[packet_size];
    size_t pos = 0;

    /// Reads at least count and at most packet_size bytes.
    auto read_bytes = [this, &buf, &pos, &packet_size](size_t count) -> void {
        while (pos < count)
        {
            int ret = socket().receiveBytes(buf + pos, packet_size - pos);
            if (ret == 0)
            {
                throw Exception("Cannot read all data. Bytes read: " + std::to_string(pos) + ". Bytes expected: 3.", ErrorCodes::CANNOT_READ_ALL_DATA);
            }
            pos += ret;
        }
    };
    read_bytes(3); /// We can find out whether it is SSLRequest of HandshakeResponse by first 3 bytes.

    size_t payload_size = unalignedLoad<uint32_t>(buf) & 0xFFFFFFu;
    LOG_TRACE(log, "payload size: {}", payload_size);

    if (payload_size == SSL_REQUEST_PAYLOAD_SIZE)
    {
        finishHandshakeSSL(packet_size, buf, pos, read_bytes, packet);
    }
    else
    {
        /// Reading rest of HandshakeResponse.
        packet_size = PACKET_HEADER_SIZE + payload_size;
        WriteBufferFromOwnString buf_for_handshake_response;
        buf_for_handshake_response.write(buf, pos);
        copyData(*packet_sender->in, buf_for_handshake_response, packet_size - pos);
        ReadBufferFromString payload(buf_for_handshake_response.str());
        payload.ignore(PACKET_HEADER_SIZE);
        packet.readPayload(payload);
        packet_sender->sequence_id++;
    }
}

void MySQLHandler::authenticate(const String & user_name, const String & auth_plugin_name, const String & initial_auth_response)
{
    try
    {
        // For compatibility with JavaScript MySQL client, Native41 authentication plugin is used when possible (if password is specified using double SHA1). Otherwise SHA256 plugin is used.
        auto user = connection_context.getAccessControlManager().read<User>(user_name);
        const DB::Authentication::Type user_auth_type = user->authentication.getType();
        if (user_auth_type != DB::Authentication::DOUBLE_SHA1_PASSWORD && user_auth_type != DB::Authentication::PLAINTEXT_PASSWORD && user_auth_type != DB::Authentication::NO_PASSWORD)
        {
            authPluginSSL();
        }

        std::optional<String> auth_response = auth_plugin_name == auth_plugin->getName() ? std::make_optional<String>(initial_auth_response) : std::nullopt;
        auth_plugin->authenticate(user_name, auth_response, connection_context, packet_sender, secure_connection, socket().peerAddress());
    }
    catch (const Exception & exc)
    {
        LOG_ERROR(log, "Authentication for user {} failed.", user_name);
        packet_sender->sendPacket(ERR_Packet(exc.code(), "00000", exc.message()), true);
        throw;
    }
    LOG_INFO(log, "Authentication for user {} succeeded.", user_name);
}

void MySQLHandler::comInitDB(ReadBuffer & payload)
{
    String database;
    readStringUntilEOF(database, payload);
    LOG_DEBUG(log, "Setting current database to {}", database);
    connection_context.setCurrentDatabase(database);
    packet_sender->sendPacket(OK_Packet(0, client_capability_flags, 0, 0, 1), true);
}

void MySQLHandler::comFieldList(ReadBuffer & payload)
{
    ComFieldList packet;
    packet.readPayload(payload);
    String database = connection_context.getCurrentDatabase();
    StoragePtr table_ptr = DatabaseCatalog::instance().getTable({database, packet.table});
    for (const NameAndTypePair & column: table_ptr->getColumns().getAll())
    {
        ColumnDefinition column_definition(
            database, packet.table, packet.table, column.name, column.name, CharacterSet::binary, 100, ColumnType::MYSQL_TYPE_STRING, 0, 0
        );
        packet_sender->sendPacket(column_definition);
    }
    packet_sender->sendPacket(OK_Packet(0xfe, client_capability_flags, 0, 0, 0), true);
}

void MySQLHandler::comPing()
{
    packet_sender->sendPacket(OK_Packet(0x0, client_capability_flags, 0, 0, 0), true);
}

static bool isFederatedServerSetupSetCommand(const String & query);
static bool isFederatedServerSetupSelectVarCommand(const String & query);

void MySQLHandler::comQuery(ReadBuffer & payload)
{
    String query = String(payload.position(), payload.buffer().end());

    // This is a workaround in order to support adding ClickHouse to MySQL using federated server.
    // As Clickhouse doesn't support these statements, we just send OK packet in response.
    if (isFederatedServerSetupSetCommand(query))
    {
        packet_sender->sendPacket(OK_Packet(0x00, client_capability_flags, 0, 0, 0), true);
    }
    else
    {
        String replacement_query = "SELECT ''";
        bool should_replace = false;
        bool with_output = false;

        // Translate query from MySQL to ClickHouse.
        // Required parameters when setup:
        // * max_allowed_packet, default 64MB, https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet
        if (isFederatedServerSetupSelectVarCommand(query))
        {
            should_replace = true;
            replacement_query = "SELECT 67108864 AS max_allowed_packet";
        }

        // This is a workaround in order to support adding ClickHouse to MySQL using federated server.
        if (0 == strncasecmp("SHOW TABLE STATUS LIKE", query.c_str(), 22))
        {
            should_replace = true;
            replacement_query = boost::replace_all_copy(query, "SHOW TABLE STATUS LIKE ", show_table_status_replacement_query);
        }

        ReadBufferFromString replacement(replacement_query);

        Context query_context = connection_context;

        executeQuery(should_replace ? replacement : payload, *out, true, query_context,
            [&with_output](const String &, const String &, const String &, const String &)
            {
                with_output = true;
            }
        );

        if (!with_output)
            packet_sender->sendPacket(OK_Packet(0x00, client_capability_flags, 0, 0, 0), true);
    }
}

void MySQLHandler::authPluginSSL()
{
    throw Exception("ClickHouse was built without SSL support. Try specifying password using double SHA1 in users.xml.", ErrorCodes::SUPPORT_IS_DISABLED);
}

void MySQLHandler::finishHandshakeSSL([[maybe_unused]] size_t packet_size, [[maybe_unused]] char * buf, [[maybe_unused]] size_t pos, [[maybe_unused]] std::function<void(size_t)> read_bytes, [[maybe_unused]] MySQLProtocol::HandshakeResponse & packet)
{
    throw Exception("Client requested SSL, while it is disabled.", ErrorCodes::SUPPORT_IS_DISABLED);
}

#if USE_SSL
MySQLHandlerSSL::MySQLHandlerSSL(IServer & server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, size_t connection_id_, RSA & public_key_, RSA & private_key_)
    : MySQLHandler(server_, socket_, ssl_enabled, connection_id_)
    , public_key(public_key_)
    , private_key(private_key_)
{}

void MySQLHandlerSSL::authPluginSSL()
{
    auth_plugin = std::make_unique<MySQLProtocol::Authentication::Sha256Password>(public_key, private_key, log);
}

void MySQLHandlerSSL::finishHandshakeSSL(size_t packet_size, char * buf, size_t pos, std::function<void(size_t)> read_bytes, MySQLProtocol::HandshakeResponse & packet)
{
    read_bytes(packet_size); /// Reading rest SSLRequest.
    SSLRequest ssl_request;
    ReadBufferFromMemory payload(buf, pos);
    payload.ignore(PACKET_HEADER_SIZE);
    ssl_request.readPayload(payload);
    connection_context.mysql.client_capabilities = ssl_request.capability_flags;
    connection_context.mysql.max_packet_size = ssl_request.max_packet_size ? ssl_request.max_packet_size : MAX_PACKET_LENGTH;
    secure_connection = true;
    ss = std::make_shared<SecureStreamSocket>(SecureStreamSocket::attach(socket(), SSLManager::instance().defaultServerContext()));
    in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
    out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
    connection_context.mysql.sequence_id = 2;
    packet_sender = std::make_shared<PacketSender>(*in, *out, connection_context.mysql.sequence_id);
    packet_sender->max_packet_size = connection_context.mysql.max_packet_size;
    packet_sender->receivePacket(packet); /// Reading HandshakeResponse from secure socket.
}

#endif

static bool isFederatedServerSetupSetCommand(const String & query)
{
    static const std::regex expr{
        "(^(SET NAMES(.*)))"
        "|(^(SET character_set_results(.*)))"
        "|(^(SET FOREIGN_KEY_CHECKS(.*)))"
        "|(^(SET AUTOCOMMIT(.*)))"
        "|(^(SET sql_mode(.*)))"
        "|(^(SET SESSION TRANSACTION ISOLATION LEVEL(.*)))"
        , std::regex::icase};
    return 1 == std::regex_match(query, expr);
}

static bool isFederatedServerSetupSelectVarCommand(const String & query)
{
     static const std::regex expr{
         "|(^(SELECT @@(.*)))"
         "|(^((/\\*(.*)\\*/)([ \t]*)(SELECT([ \t]*)@@(.*))))"
         "|(^((/\\*(.*)\\*/)([ \t]*)(SHOW VARIABLES(.*))))"
         , std::regex::icase};
     return 1 == std::regex_match(query, expr);
}

const String MySQLHandler::show_table_status_replacement_query("SELECT"
                                                               " name AS Name,"
                                                               " engine AS Engine,"
                                                               " '10' AS Version,"
                                                               " 'Dynamic' AS Row_format,"
                                                               " 0 AS Rows,"
                                                               " 0 AS Avg_row_length,"
                                                               " 0 AS Data_length,"
                                                               " 0 AS Max_data_length,"
                                                               " 0 AS Index_length,"
                                                               " 0 AS Data_free,"
                                                               " 'NULL' AS Auto_increment,"
                                                               " metadata_modification_time AS Create_time,"
                                                               " metadata_modification_time AS Update_time,"
                                                               " metadata_modification_time AS Check_time,"
                                                               " 'utf8_bin' AS Collation,"
                                                               " 'NULL' AS Checksum,"
                                                               " '' AS Create_options,"
                                                               " '' AS Comment"
                                                               " FROM system.tables"
                                                               " WHERE name LIKE ");

}
