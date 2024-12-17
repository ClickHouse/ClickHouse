#include "MySQLHandler.h"

#include <limits>
#include <optional>
#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsPreparedStatements.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Server/TCPServer.h>
#include <Storages/IStorage.h>
#include <base/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Common/setThreadName.h>

#if USE_SSL
#    include <Poco/Crypto/RSAKey.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>

#endif

namespace DB
{
namespace Setting
{
    extern const SettingsBool prefer_column_name_to_alias;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsSeconds send_timeout;
}

using namespace MySQLProtocol;
using namespace MySQLProtocol::Generic;
using namespace MySQLProtocol::ProtocolText;
using namespace MySQLProtocol::ConnectionPhase;
using namespace MySQLProtocol::PreparedStatements;

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
    extern const int UNSUPPORTED_METHOD;
}

static const size_t PACKET_HEADER_SIZE = 4;
static const size_t SSL_REQUEST_PAYLOAD_SIZE = 32;

static bool checkShouldReplaceQuery(const String & query, const String & prefix)
{
    return query.length() >= prefix.length()
        && std::equal(prefix.begin(), prefix.end(), query.begin(), [](char a, char b) { return std::tolower(a) == std::tolower(b); });
}

static bool isFederatedServerSetupSetCommand(const String & query)
{
    re2::RE2::Options regexp_options;
    regexp_options.set_case_sensitive(false);
    static const re2::RE2 expr(
        "(^(SET NAMES(.*)))"
        "|(^(SET character_set_results(.*)))"
        "|(^(SET FOREIGN_KEY_CHECKS(.*)))"
        "|(^(SET AUTOCOMMIT(.*)))"
        "|(^(SET sql_mode(.*)))"
        "|(^(SET @@(.*)))"
        "|(^(SET SESSION TRANSACTION ISOLATION LEVEL(.*)))", regexp_options);
    assert(expr.ok());
    return re2::RE2::FullMatch(query, expr);
}

/// Always return an empty set with appropriate column definitions for SHOW WARNINGS queries
/// See also: https://dev.mysql.com/doc/refman/8.0/en/show-warnings.html
static String showWarningsReplacementQuery([[maybe_unused]] const String & query)
{
    return "SELECT '' AS Level, 0::UInt32 AS Code, '' AS Message WHERE false";
}

static String showCountWarningsReplacementQuery([[maybe_unused]] const String & query)
{
    return "SELECT 0::UInt64 AS `@@session.warning_count`";
}

/// Replace "[query(such as SHOW VARIABLES...)]" into "".
static String selectEmptyReplacementQuery(const String & query)
{
    std::ignore = query;
    return "select ''";
}

/// Replace "SHOW TABLE STATUS LIKE 'xx'" into "SELECT ... FROM system.tables WHERE name LIKE 'xx'".
static String showTableStatusReplacementQuery(const String & query)
{
    const String prefix = "SHOW TABLE STATUS LIKE ";
    if (query.size() > prefix.size())
    {
        String suffix = query.data() + prefix.length();
        return (
            "SELECT"
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
            " WHERE name LIKE "
            + suffix);
    }
    return query;
}

static std::optional<String> setSettingReplacementQuery(const String & query, const String & mysql_setting, const String & clickhouse_setting)
{
    const String prefix = "SET " + mysql_setting;
    // if (query.length() >= prefix.length() && boost::iequals(std::string_view(prefix), std::string_view(query.data(), 3)))
    if (checkShouldReplaceQuery(query, prefix))
        return "SET " + clickhouse_setting + String(query.data() + prefix.length());
    return std::nullopt;
}

/// Replace "KILL QUERY [connection_id]" into "KILL QUERY WHERE query_id LIKE 'mysql:[connection_id]:xxx'".
static String killConnectionIdReplacementQuery(const String & query)
{
    const String prefix = "KILL QUERY ";
    if (query.size() > prefix.size())
    {
        String suffix = query.data() + prefix.length();
        static const re2::RE2 expr("^[0-9]");
        if (re2::RE2::FullMatch(suffix, expr))
        {
            String replacement = fmt::format("KILL QUERY WHERE query_id LIKE 'mysql:{}:%'", suffix);
            return replacement;
        }
    }
    return query;
}

MySQLHandler::MySQLHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    bool ssl_enabled, uint32_t connection_id_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getLogger("MySQLHandler"))
    , connection_id(connection_id_)
    , auth_plugin(new MySQLProtocol::Authentication::Native41())
    , read_event(read_event_)
    , write_event(write_event_)
{
    server_capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;
    if (ssl_enabled)
        server_capabilities |= CLIENT_SSL;

    queries_replacements.emplace("SHOW WARNINGS", showWarningsReplacementQuery);
    queries_replacements.emplace("SHOW COUNT(*) WARNINGS", showCountWarningsReplacementQuery);
    queries_replacements.emplace("KILL QUERY", killConnectionIdReplacementQuery);
    queries_replacements.emplace("SHOW TABLE STATUS LIKE", showTableStatusReplacementQuery);
    queries_replacements.emplace("SHOW VARIABLES", selectEmptyReplacementQuery);
    settings_replacements.emplace("SQL_SELECT_LIMIT", "limit");
    settings_replacements.emplace("NET_WRITE_TIMEOUT", "send_timeout");
    settings_replacements.emplace("NET_READ_TIMEOUT", "receive_timeout");
}

MySQLHandler::~MySQLHandler() = default;

void MySQLHandler::run()
{
    setThreadName("MySQLHandler");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MYSQL);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    const Settings & settings = server.context()->getSettingsRef();
    socket().setReceiveTimeout(settings[Setting::receive_timeout]);
    socket().setSendTimeout(settings[Setting::send_timeout]);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket(), read_event);
    out = std::make_shared<WriteBufferFromPocoSocket>(socket(), write_event);
    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(*in, *out, sequence_id);

    try
    {
        Handshake handshake(server_capabilities, connection_id, VERSION_STRING + String("-") + VERSION_NAME,
            auth_plugin->getName(), auth_plugin->getAuthPluginData(), CharacterSet::utf8_general_ci);
        packet_endpoint->sendPacket<Handshake>(handshake, true);

        LOG_TRACE(log, "Sent handshake");

        HandshakeResponse handshake_response;
        finishHandshake(handshake_response);
        client_capabilities = handshake_response.capability_flags;
        max_packet_size = handshake_response.max_packet_size ? handshake_response.max_packet_size : MAX_PACKET_LENGTH;

        LOG_TRACE(log,
            "Capabilities: {}, max_packet_size: {}, character_set: {}, user: {}, auth_response length: {}, database: {}, auth_plugin_name: {}",
            handshake_response.capability_flags,
            handshake_response.max_packet_size,
            static_cast<int>(handshake_response.character_set),
            handshake_response.username,
            handshake_response.auth_response.length(),
            handshake_response.database,
            handshake_response.auth_plugin_name);

        if (!(client_capabilities & CLIENT_PROTOCOL_41))
            throw Exception(ErrorCodes::MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES, "Required capability: CLIENT_PROTOCOL_41.");

        authenticate(handshake_response.username, handshake_response.auth_plugin_name, handshake_response.auth_response);

        try
        {
            session->makeSessionContext();
            session->sessionContext()->setDefaultFormat("MySQLWire");
            if (!handshake_response.database.empty())
                session->sessionContext()->setCurrentDatabase(handshake_response.database);
        }
        catch (const Exception & exc)
        {
            log->log(exc);
            packet_endpoint->sendPacket(ERRPacket(exc.code(), "00000", exc.message()), true);
        }

        OKPacket ok_packet(0, handshake_response.capability_flags, 0, 0, 0);
        packet_endpoint->sendPacket(ok_packet, true);

        while (tcp_server.isOpen())
        {
            packet_endpoint->resetSequenceId();
            MySQLPacketPayloadReadBuffer payload = packet_endpoint->getPayload();

            while (!in->poll(1000000))
                if (!tcp_server.isOpen())
                    return;
            char command = 0;
            payload.readStrict(command);

            // For commands which are executed without MemoryTracker.
            LimitReadBuffer limited_payload(payload, 10000, /* trow_exception */ true, /* exact_limit */ {}, "too long MySQL packet.");

            LOG_DEBUG(log, "Received command: {}. Connection id: {}.",
                static_cast<int>(static_cast<unsigned char>(command)), connection_id);

            if (!tcp_server.isOpen())
                return;
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
                        comQuery(payload, false);
                        break;
                    case COM_FIELD_LIST:
                        comFieldList(limited_payload);
                        break;
                    case COM_PING:
                        comPing();
                        break;
                    case COM_STMT_PREPARE:
                        comStmtPrepare(payload);
                        break;
                    case COM_STMT_EXECUTE:
                        comStmtExecute(payload);
                        break;
                    case COM_STMT_CLOSE:
                        comStmtClose(payload);
                        break;
                    default:
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Command {} is not implemented.", command);
                }
            }
            catch (const NetException & exc)
            {
                log->log(exc);
                throw;
            }
            catch (...)
            {
                tryLogCurrentException(log, "MySQLHandler: Cannot read packet: ");
                packet_endpoint->sendPacket(ERRPacket(getCurrentExceptionCode(), "00000", getCurrentExceptionMessage(false)), true);
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
void MySQLHandler::finishHandshake(MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    size_t packet_size = PACKET_HEADER_SIZE + SSL_REQUEST_PAYLOAD_SIZE;

    /// Buffer for SSLRequest or part of HandshakeResponse.
    char buf[packet_size];
    size_t pos = 0;

    /// Reads at least count and at most packet_size bytes.
    auto read_bytes = [this, &buf, &pos, &packet_size](size_t count) -> void {
        while (pos < count)
        {
            int ret = socket().receiveBytes(buf + pos, static_cast<uint32_t>(packet_size - pos));
            if (ret == 0)
            {
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data. Bytes read: {}. Bytes expected: 3", std::to_string(pos));
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
        copyData(*packet_endpoint->in, buf_for_handshake_response, packet_size - pos);
        ReadBufferFromString payload(buf_for_handshake_response.str());
        payload.ignore(PACKET_HEADER_SIZE);
        packet.readPayloadWithUnpacked(payload);
        packet_endpoint->sequence_id++;
    }
}

void MySQLHandler::authenticate(const String & user_name, const String & auth_plugin_name, const String & initial_auth_response)
{
    try
    {
        const auto user_authentication_types = session->getAuthenticationTypesOrLogInFailure(user_name);

        for (const auto user_authentication_type : user_authentication_types)
        {
            // For compatibility with JavaScript MySQL client, Native41 authentication plugin is used when possible
            // (if password is specified using double SHA1). Otherwise, SHA256 plugin is used.
            if (user_authentication_type == DB::AuthenticationType::SHA256_PASSWORD)
            {
                authPluginSSL();
            }
        }

        std::optional<String> auth_response = auth_plugin_name == auth_plugin->getName() ? std::make_optional<String>(initial_auth_response) : std::nullopt;
        auth_plugin->authenticate(user_name, *session, auth_response, packet_endpoint, secure_connection, socket().peerAddress());
    }
    catch (const Exception & exc)
    {
        LOG_ERROR(log, "Authentication for user {} failed.", user_name);
        packet_endpoint->sendPacket(ERRPacket(exc.code(), "00000", exc.message()), true);
        throw;
    }
    LOG_DEBUG(log, "Authentication for user {} succeeded.", user_name);
}

void MySQLHandler::comInitDB(ReadBuffer & payload)
{
    String database;
    readStringUntilEOF(database, payload);
    LOG_DEBUG(log, "Setting current database to {}", database);
    session->sessionContext()->setCurrentDatabase(database);
    packet_endpoint->sendPacket(OKPacket(0, client_capabilities, 0, 0, 1), true);
}

void MySQLHandler::comFieldList(ReadBuffer & payload)
{
    ComFieldList packet;
    packet.readPayloadWithUnpacked(payload);
    const auto session_context = session->sessionContext();
    String database = session_context->getCurrentDatabase();
    StoragePtr table_ptr = DatabaseCatalog::instance().getTable({database, packet.table}, session_context);
    auto metadata_snapshot = table_ptr->getInMemoryMetadataPtr();
    for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAll())
    {
        ColumnDefinition column_definition(
            database, packet.table, packet.table, column.name, column.name, CharacterSet::binary, 100, ColumnType::MYSQL_TYPE_STRING, 0, 0, true
        );
        packet_endpoint->sendPacket(column_definition);
    }
    packet_endpoint->sendPacket(OKPacket(0xfe, client_capabilities, 0, 0, 0), true);
}

void MySQLHandler::comPing()
{
    packet_endpoint->sendPacket(OKPacket(0x0, client_capabilities, 0, 0, 0), true);
}

void MySQLHandler::comQuery(ReadBuffer & payload, bool binary_protocol)
{
    String query = String(payload.position(), payload.buffer().end());

    // This is a workaround in order to support adding ClickHouse to MySQL using federated server.
    // As Clickhouse doesn't support these statements, we just send OK packet in response.
    if (isFederatedServerSetupSetCommand(query))
    {
        packet_endpoint->sendPacket(OKPacket(0x00, client_capabilities, 0, 0, 0), true);
    }
    else
    {
        String replacement_query;
        bool should_replace = false;
        bool with_output = false;

        // Queries replacements
        for (auto const & [query_to_replace, replacement_fn] : queries_replacements)
        {
            if (checkShouldReplaceQuery(query, query_to_replace))
            {
                should_replace = true;
                replacement_query = replacement_fn(query);
                break;
            }
        }

        // Settings replacements
        if (!should_replace)
        {
            for (auto const & [mysql_setting, clickhouse_setting] : settings_replacements)
            {
                const auto replacement_query_opt = setSettingReplacementQuery(query, mysql_setting, clickhouse_setting);
                if (replacement_query_opt.has_value())
                {
                    should_replace = true;
                    replacement_query = replacement_query_opt.value();
                    break;
                }
            }
        }

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("mysql:{}:{}", connection_id, toString(UUIDHelpers::generateV4())));

        /// --- Workaround for Bug 56173. Can be removed when the analyzer is on by default.
        auto settings = query_context->getSettingsCopy();
        settings[Setting::prefer_column_name_to_alias] = true;
        query_context->setSettings(settings);

        /// Update timeouts
        socket().setReceiveTimeout(settings[Setting::receive_timeout]);
        socket().setSendTimeout(settings[Setting::send_timeout]);

        CurrentThread::QueryScope query_scope{query_context};

        std::atomic<size_t> affected_rows {0};
        auto prev = query_context->getProgressCallback();
        query_context->setProgressCallback([&, my_prev = prev](const Progress & progress)
        {
            if (my_prev)
                my_prev(progress);

            affected_rows += progress.written_rows;
        });

        FormatSettings format_settings;
        format_settings.mysql_wire.client_capabilities = client_capabilities;
        format_settings.mysql_wire.max_packet_size = max_packet_size;
        format_settings.mysql_wire.sequence_id = &sequence_id;
        format_settings.mysql_wire.binary_protocol = binary_protocol;

        auto set_result_details = [&with_output](const QueryResultDetails & details)
        {
            if (details.format)
            {
                if (*details.format != "MySQLWire")
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MySQL protocol does not support custom output formats");

                with_output = true;
            }
        };

        if (should_replace)
        {
            ReadBufferFromString replacement(replacement_query);
            executeQuery(replacement, *out, false, query_context, set_result_details, QueryFlags{}, format_settings);
        }
        else
            executeQuery(payload, *out, false, query_context, set_result_details, QueryFlags{}, format_settings);


        if (!with_output)
            packet_endpoint->sendPacket(OKPacket(0x00, client_capabilities, affected_rows, 0, 0), true);
    }
}

void MySQLHandler::comStmtPrepare(DB::ReadBuffer & payload)
{
    String statement;
    readStringUntilEOF(statement, payload);

    auto statement_id_opt = emplacePreparedStatement(std::move(statement));
    if (statement_id_opt.has_value())
        packet_endpoint->sendPacket(PreparedStatementResponseOK(statement_id_opt.value(), 0, 0, 0), true);
    else
        packet_endpoint->sendPacket(ERRPacket(), true);
}

void MySQLHandler::comStmtExecute(ReadBuffer & payload)
{
    uint32_t statement_id;
    payload.readStrict(reinterpret_cast<char *>(&statement_id), 4);

    auto statement_opt = getPreparedStatement(statement_id);
    if (statement_opt.has_value())
        MySQLHandler::comQuery(statement_opt.value(), true);
    else
        packet_endpoint->sendPacket(ERRPacket(), true);
};

void MySQLHandler::comStmtClose(ReadBuffer & payload)
{
    uint32_t statement_id;
    payload.readStrict(reinterpret_cast<char *>(&statement_id), 4);

    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
    // No response packet is sent back to the client.
    erasePreparedStatement(statement_id);
};

std::optional<UInt32> MySQLHandler::emplacePreparedStatement(String statement)
{
    static constexpr size_t MAX_PREPARED_STATEMENTS = 10'000;
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    if (prepared_statements.size() > MAX_PREPARED_STATEMENTS) /// Shouldn't happen in reality as COM_STMT_CLOSE cleans up the elements
    {
        LOG_ERROR(log, "Too many prepared statements");
        current_prepared_statement_id = 0;
        prepared_statements.clear();
        return {};
    }

    uint32_t statement_id = current_prepared_statement_id;
    ++current_prepared_statement_id;

    // Key collisions should not happen here, as we remove the elements from the map with COM_STMT_CLOSE,
    // and we have quite a big range of available identifiers with 32-bit unsigned integer
    if (prepared_statements.contains(statement_id))
    {
        LOG_ERROR(
            log,
            "Failed to store a new statement `{}` with id {}; it is already taken by `{}`",
            statement,
            statement_id,
            prepared_statements.at(statement_id));
        return {};
    }

    prepared_statements.emplace(statement_id, statement);
    return std::make_optional(statement_id);
};

std::optional<ReadBufferFromString> MySQLHandler::getPreparedStatement(UInt32 statement_id)
{
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    if (!prepared_statements.contains(statement_id))
    {
        LOG_ERROR(log, "Could not find prepared statement with id {}", statement_id);
        return {};
    }
    // Temporary workaround as we work only with queries that do not bind any parameters atm
    return std::make_optional<ReadBufferFromString>(prepared_statements.at(statement_id));
}

void MySQLHandler::erasePreparedStatement(UInt32 statement_id)
{
    std::lock_guard<std::mutex> lock(prepared_statements_mutex);
    prepared_statements.erase(statement_id);
}

void MySQLHandler::authPluginSSL()
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "ClickHouse was built without SSL support. Try specifying password using double SHA1 in users.xml.");
}

void MySQLHandler::finishHandshakeSSL(
    [[maybe_unused]] size_t packet_size, [[maybe_unused]] char * buf, [[maybe_unused]] size_t pos,
    [[maybe_unused]] std::function<void(size_t)> read_bytes, [[maybe_unused]] MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Client requested SSL, while it is disabled.");
}

#if USE_SSL
MySQLHandlerSSL::MySQLHandlerSSL(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    bool ssl_enabled,
    uint32_t connection_id_,
    RSA & public_key_,
    RSA & private_key_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : MySQLHandler(server_, tcp_server_, socket_, ssl_enabled, connection_id_, read_event_, write_event_)
    , public_key(public_key_)
    , private_key(private_key_)
{}

void MySQLHandlerSSL::authPluginSSL()
{
    auth_plugin = std::make_unique<MySQLProtocol::Authentication::Sha256Password>(public_key, private_key, log);
}

void MySQLHandlerSSL::finishHandshakeSSL(
    size_t packet_size, char *buf, size_t pos, std::function<void(size_t)> read_bytes,
    MySQLProtocol::ConnectionPhase::HandshakeResponse & packet)
{
    read_bytes(packet_size); /// Reading rest SSLRequest.
    SSLRequest ssl_request;
    ReadBufferFromMemory payload(buf, pos);
    payload.ignore(PACKET_HEADER_SIZE);
    ssl_request.readPayloadWithUnpacked(payload);
    client_capabilities = ssl_request.capability_flags;
    max_packet_size = ssl_request.max_packet_size ? ssl_request.max_packet_size : MAX_PACKET_LENGTH;
    secure_connection = true;

    ss = std::make_shared<SecureStreamSocket>(SecureStreamSocket::attach(socket(), SSLManager::instance().defaultServerContext()));
    ss->setReceiveTimeout(socket().getReceiveTimeout());
    ss->setSendTimeout(socket().getSendTimeout());

    in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
    out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
    sequence_id = 2;
    packet_endpoint = std::make_shared<MySQLProtocol::PacketEndpoint>(*in, *out, sequence_id);
    packet_endpoint->receivePacket(packet); /// Reading HandshakeResponse from secure socket.
}

#endif
}
