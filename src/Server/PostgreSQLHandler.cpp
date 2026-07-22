#include <memory>
#include <optional>
#include <Server/PostgreSQLHandler.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServer.h>
#include <boost/algorithm/string/trim.hpp>
#include <array>
#include <cstring>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Core/PostgreSQLProtocol.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ParserCopyQuery.h>
#include <Core/Settings.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserQuery.h>
#include <fmt/format.h>
#include <Formats/FormatFactory.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/PostgreSQLArrayText.h>

#if USE_SSL
#    include <Server/CertificateReloader.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#endif

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool implicit_select;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
    extern const int OPENSSL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

namespace
{

/// Presents the payload of a `COPY ... FROM STDIN` as one continuous stream. PostgreSQL `CopyData`
/// frame boundaries are transport-only: a client may split one logical row (or even one multi-byte
/// character) across several frames and may pack many rows into one, so the input format must parse
/// the concatenation of all frames, not each frame in isolation. Each refill pulls the next
/// `CopyData` frame from the connection; `CopyDone` ends the stream.
class CopyInDataReadBuffer : public ReadBuffer
{
public:
    explicit CopyInDataReadBuffer(PostgreSQLProtocol::Messaging::MessageTransport & transport_)
        : ReadBuffer(nullptr, 0), transport(transport_)
    {
    }

private:
    bool nextImpl() override
    {
        /// End-of-stream must be sticky: `ReadBuffer::eof` may probe `next` again after the stream has
        /// ended (e.g. the parallel-parsing segmentator does), and by then the client is already waiting
        /// for `CommandComplete` and sends nothing more - reading the socket again would deadlock.
        if (received_copy_done)
            return false;

        while (true)
        {
            /// Push out anything buffered on the write side before blocking on the client.
            transport.flush();
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = transport.receiveMessageType();
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA)
            {
                current_frame = std::move(transport.receive<PostgreSQLProtocol::Messaging::CopyInData>()->query);

                /// An empty frame is legal and does not mean end-of-stream; wait for the next message.
                if (current_frame.empty())
                    continue;

                working_buffer = Buffer(current_frame.data(), current_frame.data() + current_frame.size());
                return true;
            }
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION)
            {
                transport.receive<PostgreSQLProtocol::Messaging::CopyDone>();
                received_copy_done = true;
                return false;
            }
            /// A `CopyFail` is a normal client-side abort of the copy (libpq sends it when its local data
            /// source errors or the copy is cancelled), not a protocol violation. Record the abort and throw
            /// to unwind the staging loop (the payload is staged in full before anything reaches the insert
            /// pipeline, so an abort leaves no partial rows behind); the COPY_FROM handler recognizes the
            /// abort via `clientAborted`, sends a clean `ErrorResponse` and returns the query as handled so
            /// the run loop follows with `ReadyForQuery`, keeping the connection alive. A plain rethrow
            /// instead would propagate out of `processQuery` to the run loop and drop the connection before
            /// `ReadyForQuery`, which a driver such as psycopg2 reports as a lost connection.
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_FAILURE)
            {
                auto copy_fail = transport.receive<PostgreSQLProtocol::Messaging::CopyFail>();
                received_copy_done = true;
                client_aborted = true;
                abort_reason = copy_fail->message;
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "COPY FROM STDIN aborted by the client: {}", abort_reason);
            }
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Received incorrect message type - expected {} or {}, got {}",
                PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA,
                PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION,
                message_type);
        }
    }

public:
    /// Whether the client aborted the copy with a `CopyFail` message, and the reason it sent. Used by the
    /// COPY_FROM handler to turn the abort into a clean `ErrorResponse` instead of a connection teardown.
    bool clientAborted() const { return client_aborted; }
    const String & abortReason() const { return abort_reason; }

private:
    PostgreSQLProtocol::Messaging::MessageTransport & transport;
    String current_frame;
    bool received_copy_done = false;
    bool client_aborted = false;
    String abort_reason;
};

/// Some PostgreSQL drivers issue session-management commands during connection
/// setup or teardown that have no ClickHouse equivalent, for example `RESET ALL`
/// and `UNLISTEN *` sent by the Skunk driver. Instead of failing such a command
/// with a syntax error, ClickHouse accepts it as a no-op and replies with a
/// `CommandComplete` carrying the matching PostgreSQL command tag. See issue
/// https://github.com/ClickHouse/ClickHouse/issues/12476.
///
/// Returns the command tag to report if `query` is such a no-op command, or
/// std::nullopt if the query must be executed normally. None of the recognized
/// keywords (`UNLISTEN`, `RESET`, `DISCARD`) is a valid ClickHouse statement
/// start, so there are no false positives.
///
/// This is applied only in the simple-query (`Q`) protocol path, consistent with the other
/// driver-compatibility no-ops (`BEGIN` / `COMMIT` / `SET application_name`): drivers emit these
/// session-management commands as plain, unparameterized statements, so there is no reason to run
/// them through the extended `Parse` / `Bind` / `Execute` flow, and the extended path deliberately
/// performs no such driver-specific rewriting.
std::optional<String> classifyNoOpDriverCommand(const String & query)
{
    /// Only treat the packet as a no-op when it consists of a single statement. A simple-query
    /// packet may contain several `;`-separated statements; if we shortcut on the leading keyword
    /// we would acknowledge the whole packet and silently skip the rest (e.g. `RESET ALL; SELECT 1`
    /// or, worse, `RESET ALL; DROP TABLE t`). An interior `;` — anything other than trailing
    /// whitespace after it — means there is more than one statement, so bail out and let the normal
    /// multi-statement splitter handle it.
    if (const size_t semicolon = query.find(';'); semicolon != String::npos)
    {
        for (size_t i = semicolon + 1; i < query.size(); ++i)
        {
            const char c = query[i];
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\f' && c != '\v')
                return std::nullopt;
        }
    }

    /// Enough to cover the longest recognized command plus its argument: a PostgreSQL identifier
    /// is at most 63 bytes, and a qualified `RESET extension.setting` name consists of two of them.
    /// If the normalized prefix fills this budget completely, the statement may continue beyond it,
    /// so we could not verify that nothing trails the argument — treat it as not recognized.
    static constexpr size_t max_prefix_len = 160;
    String prefix = PostgreSQLProtocol::Messaging::CommandComplete::extractNormalizedPrefix(query, max_prefix_len);
    if (prefix.size() == max_prefix_len)
        return std::nullopt;

    /// The multi-statement guard above rejected any statement after an interior `;`, so the only
    /// `;` that can remain is a single trailing one (with trailing whitespace already collapsed).
    /// Drop it so it is not mistaken for a command argument below.
    while (!prefix.empty() && (prefix.back() == ';' || prefix.back() == ' '))
        prefix.pop_back();

    /// `extractNormalizedPrefix` already uppercased the text and collapsed runs of
    /// whitespace to single spaces, so a keyword is a run of 'A'..'Z'. Take the next
    /// such run, skipping a leading space; this also stops at a `;` or `*`.
    size_t pos = 0;
    const auto take_word = [&]() -> String
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        const size_t start = pos;
        while (pos < prefix.size() && prefix[pos] >= 'A' && prefix[pos] <= 'Z')
            ++pos;
        return prefix.substr(start, pos - start);
    };
    const auto has_more = [&]() -> bool
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        return pos < prefix.size();
    };

    /// An argument in the normalized prefix: an identifier — a letter or `_` followed by letters,
    /// digits, `_` or `$` (the text is already uppercased). With `allow_dots`, a qualified name
    /// such as `EXTENSION.SETTING` (for `RESET`) is a single token as well.
    const auto take_identifier = [&](bool allow_dots) -> String
    {
        while (pos < prefix.size() && prefix[pos] == ' ')
            ++pos;
        const size_t start = pos;
        if (pos < prefix.size() && ((prefix[pos] >= 'A' && prefix[pos] <= 'Z') || prefix[pos] == '_'))
        {
            ++pos;
            while (pos < prefix.size()
                && ((prefix[pos] >= 'A' && prefix[pos] <= 'Z') || (prefix[pos] >= '0' && prefix[pos] <= '9')
                    || prefix[pos] == '_' || prefix[pos] == '$' || (allow_dots && prefix[pos] == '.')))
                ++pos;
        }
        return prefix.substr(start, pos - start);
    };

    /// Not `const`: the early returns below move it out, and `performance-no-automatic-move`
    /// (clang-tidy) rejects returning a `const` local because constness prevents the move.
    String command = take_word();

    /// The keyword must end at a word boundary: `RESET1FOO` must not be taken for `RESET`.
    if (pos < prefix.size() && prefix[pos] != ' ')
        return std::nullopt;

    /// Accept the connection-cleanup commands the Skunk driver actually sends: `RESET { name | ALL }`
    /// and `UNLISTEN { channel | * }`, reporting the bare keyword as the tag. `LISTEN` and `NOTIFY`
    /// are deliberately NOT accepted here: unlike `UNLISTEN` (idempotent unsubscribe-all cleanup),
    /// they are application-visible PostgreSQL pub/sub operations, and this handler never delivers a
    /// `NotificationResponse`, so acknowledging them would turn an unsupported feature into a silent
    /// false success instead of a plain error. Issue #12476 only asks for `UNLISTEN *` / `RESET ALL`.
    ///
    /// Accept exactly one argument — an identifier (for `RESET` possibly a qualified
    /// `extension.setting` name; `ALL` is itself covered as an identifier), or `*` for `UNLISTEN` —
    /// and require the statement to end right after it, so that malformed variants such as a bare
    /// `RESET`, `RESET foo bar` or `UNLISTEN * garbage` are not acknowledged as success but fall
    /// through to the normal error path. Valid forms that no driver is known to emit — quoted
    /// identifiers and multi-word variants such as `RESET SESSION AUTHORIZATION` — likewise fall
    /// through, as before this change.
    if (command == "UNLISTEN" || command == "RESET")
    {
        String arg;
        if (command == "UNLISTEN" && has_more() && prefix[pos] == '*')
        {
            arg = "*";
            ++pos;
        }
        else
        {
            arg = take_identifier(/* allow_dots = */ command == "RESET");
        }
        if (arg.empty() || has_more())
            return std::nullopt;
        return command;
    }

    if (command == "DISCARD")
    {
        /// PostgreSQL accepts only `DISCARD { ALL | PLANS | SEQUENCES | TEMP | TEMPORARY }`
        /// (with `TEMPORARY` normalized to `TEMP`). Reject a bare `DISCARD` or an unknown
        /// subcommand such as `DISCARD FOO` instead of claiming success for a command we were
        /// never asked to emulate.
        String arg = take_word();
        if (arg == "TEMPORARY")
            arg = "TEMP";
        if ((arg == "ALL" || arg == "PLANS" || arg == "SEQUENCES" || arg == "TEMP") && !has_more())
            return command + " " + arg;
        return std::nullopt;
    }

    return std::nullopt;
}

}

PostgreSQLHandler::PostgreSQLHandler(
    const Poco::Net::StreamSocket & socket_,
#if USE_SSL
    const std::string & prefix_,
#endif
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    bool secure_required_,
    Int32 connection_id_,
    VectorWithMemoryTracking<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
#if USE_SSL
    , config(server_.config())
    , prefix(prefix_)
#endif
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , secure_required(secure_required_)
    , connection_id(connection_id_)
    , read_event(read_event_)
    , write_event(write_event_)
    , authentication_manager(auth_methods_)
    , prepared_statements_manager(std::nullopt)
{
    changeIO(socket());

#if USE_SSL
    params.privateKeyFile = config.getString(prefix + Poco::Net::SSLManager::CFG_PRIV_KEY_FILE, "");
    params.certificateFile = config.getString(prefix + Poco::Net::SSLManager::CFG_CERTIFICATE_FILE, params.privateKeyFile);
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        params.caLocation = config.getString(prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, "");
        if (params.caLocation.empty())
        {
            auto ctx = Poco::Net::SSLManager::instance().defaultServerContext();
            params.caLocation = ctx->getCAPaths().caLocation;
        }

        params.verificationMode = Poco::Net::SSLManager::VAL_VER_MODE;
        if (config.hasProperty(prefix + Poco::Net::SSLManager::CFG_VER_MODE))
        {
            std::string mode = config.getString(prefix + Poco::Net::SSLManager::CFG_VER_MODE);
            params.verificationMode = Poco::Net::Utility::convertVerificationMode(mode);
        }

        params.verificationDepth = config.getInt(prefix + Poco::Net::SSLManager::CFG_VER_DEPTH, Poco::Net::SSLManager::VAL_VER_DEPTH);
        params.loadDefaultCAs
            = config.getBool(prefix + Poco::Net::SSLManager::CFG_ENABLE_DEFAULT_CA, Poco::Net::SSLManager::VAL_ENABLE_DEFAULT_CA);
        params.cipherList = config.getString(prefix + Poco::Net::SSLManager::CFG_CIPHER_LIST, Poco::Net::SSLManager::VAL_CIPHER_LIST);
        params.cipherList
            = config.getString(prefix + Poco::Net::SSLManager::CFG_CYPHER_LIST, params.cipherList); // for backwards compatibility

        bool require_tlsv1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1, false);
        bool require_tlsv1_1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_1, false);
        bool require_tlsv1_2 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_2, false);
        if (require_tlsv1_2)
            usage = Poco::Net::Context::TLSV1_2_SERVER_USE;
        else if (require_tlsv1_1)
            usage = Poco::Net::Context::TLSV1_1_SERVER_USE;
        else if (require_tlsv1)
            usage = Poco::Net::Context::TLSV1_SERVER_USE;
        else
            usage = Poco::Net::Context::SERVER_USE;

        params.dhParamsFile = config.getString(prefix + Poco::Net::SSLManager::CFG_DH_PARAMS_FILE, "");
        params.ecdhCurve = config.getString(prefix + Poco::Net::SSLManager::CFG_ECDH_CURVE, "");

        std::string disabled_protocols_list = config.getString(prefix + Poco::Net::SSLManager::CFG_DISABLE_PROTOCOLS, "");
        Poco::StringTokenizer dp_tok(
            disabled_protocols_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
        disabled_protocols = 0;
        for (const auto & token : dp_tok)
        {
            if (token == "sslv2")
                disabled_protocols |= Poco::Net::Context::PROTO_SSLV2;
            else if (token == "sslv3")
                disabled_protocols |= Poco::Net::Context::PROTO_SSLV3;
            else if (token == "tlsv1")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1;
            else if (token == "tlsv1_1")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_1;
            else if (token == "tlsv1_2")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_2;
        }

        extended_verification = config.getBool(prefix + Poco::Net::SSLManager::CFG_EXTENDED_VERIFICATION, false);
        prefer_server_ciphers = config.getBool(prefix + Poco::Net::SSLManager::CFG_PREFER_SERVER_CIPHERS, false);
    }
#endif
}

void PostgreSQLHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket, read_event);
    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket, write_event);
    message_transport = std::make_shared<PostgreSQLProtocol::Messaging::MessageTransport>(in.get(), out.get());
}

void PostgreSQLHandler::run()
{
    DB::setThreadName(ThreadName::POSTGRES_HANDLER);

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::POSTGRESQL);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    try
    {
        if (!startup())
            return;

        while (tcp_server.isOpen())
        {
            if (!is_query_in_progress)
                message_transport->send(PostgreSQLProtocol::Messaging::ReadyForQuery(), true);

            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = message_transport->receiveMessageType();
            if (!tcp_server.isOpen())
                return;
            switch (message_type)
            {
                case PostgreSQLProtocol::Messaging::FrontMessageType::QUERY:
                    processQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE:
                    LOG_DEBUG(log, "Client closed the connection");
                    return;
                case PostgreSQLProtocol::Messaging::FrontMessageType::PARSE:
                    is_query_in_progress = true;
                    processParseQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::BIND:
                    is_query_in_progress = true;
                    processBindQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::EXECUTE:
                    processExecuteQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::SYNC:
                    is_query_in_progress = false;
                    processSyncQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::DESCRIBE:
                    processDescribeQuery();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::FLUSH:
                    message_transport->send(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR,
                            "0A000",
                            "ClickHouse doesn't support extended query mechanism"),
                        true);
                    LOG_ERROR(log, "Client tried to access via extended query protocol");
                    message_transport->dropMessage();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::CLOSE:
                    processCloseQuery();
                    message_transport->flush();
                    break;
                default:
                    message_transport->send(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR,
                            "0A000",
                            "Command is not supported"),
                        true);
                    LOG_ERROR(log, "Command is not supported. Command code {:d}", static_cast<Int32>(message_type));
                    message_transport->dropMessage();
            }
        }
    }
    catch (const Poco::Exception &exc)
    {
        log->log(exc);
    }

}

bool PostgreSQLHandler::startup()
{
    Int32 payload_size = 0;
    Int32 info = 0;
    establishSecureConnection(payload_size, info);

    if (static_cast<PostgreSQLProtocol::Messaging::FrontMessageType>(info) == PostgreSQLProtocol::Messaging::FrontMessageType::CANCEL_REQUEST)
    {
        LOG_DEBUG(log, "Client issued request canceling");
        cancelRequest();
        return false;
    }

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> start_up_msg = receiveStartupMessage(payload_size);
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

    LOG_DEBUG(log, "Successfully finished Startup stage");
    return true;
}

void PostgreSQLHandler::establishSecureConnection(Int32 & payload_size, Int32 & info)
{
    bool was_secure_connection = false;
    bool was_encryption_req = true;
    readBinaryBigEndian(payload_size, *in);
    readBinaryBigEndian(info, *in);

    switch (static_cast<PostgreSQLProtocol::Messaging::FrontMessageType>(info))
    {
        case PostgreSQLProtocol::Messaging::FrontMessageType::SSL_REQUEST:
            LOG_DEBUG(log, "Client requested SSL");
            if (ssl_enabled)
            {
                was_secure_connection = true;
                makeSecureConnectionSSL();
            }
            else
                message_transport->send('N', true);
            break;
        case PostgreSQLProtocol::Messaging::FrontMessageType::GSSENC_REQUEST:
            LOG_DEBUG(log, "Client requested GSSENC");
            message_transport->send('N', true);
            break;
        default:
            was_encryption_req = false;
    }
    if (was_encryption_req)
    {
        readBinaryBigEndian(payload_size, *in);
        readBinaryBigEndian(info, *in);
    }

    if (secure_required && !was_secure_connection)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "XX000", "SSL connection required."),
            true);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "SSL connection required.");
    }
}

#if USE_SSL
void PostgreSQLHandler::makeSecureConnectionSSL()
{
    message_transport->send('S', true);
    Poco::Net::Context::Ptr ctx;
    if (!params.privateKeyFile.empty() && !params.certificateFile.empty())
    {
        ctx = Poco::Net::SSLManager::instance().getCustomServerContext(prefix);
        if (!ctx)
        {
            ctx = new Poco::Net::Context(usage, params);
            ctx->disableProtocols(disabled_protocols);
            ctx->enableExtendedCertificateVerification(extended_verification);
            if (prefer_server_ciphers)
                ctx->preferServerCiphers();
            CertificateReloader::instance().tryLoad(config, ctx->sslContext(), prefix);
            ctx = Poco::Net::SSLManager::instance().setCustomServerContext(prefix, ctx);
        }
    }
    else
    {
        ctx = Poco::Net::SSLManager::instance().defaultServerContext();
    }
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(Poco::Net::SecureStreamSocket::attach(socket(), ctx));
    changeIO(*ss);
}
#else
void PostgreSQLHandler::makeSecureConnectionSSL() {}
#endif

void PostgreSQLHandler::sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message)
{
    auto & parameters = start_up_message.parameters;

    if (parameters.contains("application_name"))
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("application_name", parameters["application_name"]));
    if (parameters.contains("client_encoding"))
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("client_encoding", parameters["client_encoding"]));
    else
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("client_encoding", "UTF8"));

    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("server_version", VERSION_STRING));
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("server_encoding", "UTF8"));
    message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("DateStyle", "ISO"));
    message_transport->flush();
}

void PostgreSQLHandler::cancelRequest()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CancelRequest> msg =
        message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::CancelRequest>(8);

    String query = fmt::format("KILL QUERY WHERE query_id = 'postgres:{:d}:{:d}'", msg->process_id, msg->secret_key);
    auto replacement = std::make_unique<ReadBufferFromOwnString>(std::move(query));

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId("");
    executeQuery(std::move(replacement), *out, query_context, {});
}

inline std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> PostgreSQLHandler::receiveStartupMessage(int payload_size)
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> message;
    try
    {
        message = message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::StartupMessage>(payload_size - 8);
    }
    catch (const Exception &)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "08P01", "Can't correctly handle Startup message"),
            true);
        throw;
    }

    LOG_DEBUG(log, "Successfully received Startup message");
    return message;
}

bool PostgreSQLHandler::processCopyQuery(const String & query)
{
    ParserCopyQuery parser_copy;
    ASTPtr copy_query_parsed;

    try
    {
        copy_query_parsed = parseQuery(parser_copy, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        copy_query_parsed.reset();
    }


    /// PostgreSQL binary `COPY` uses its own wire format (a `PGCOPY\n\377\r\n\0` header, per-tuple field
    /// counts and per-field length framing). We do not implement it, and `CopyInResponse` / `CopyOutResponse`
    /// always advertise the text format code, so emitting ClickHouse's `RowBinary` for `WITH FORMAT binary`
    /// would hand a real PostgreSQL client a payload it cannot parse. Reject it explicitly instead - the text
    /// and CSV formats cover the self-connect use case (ClickHouse reads the result with `pqxx`, which uses
    /// text `COPY`).
    ///
    /// Send an `ErrorResponse` and return (marking the query as handled) rather than throwing: this is an
    /// ordinary "your query failed" error, not a fatal connection error, so the connection must stay open
    /// for the `ReadyForQuery` that the run loop sends next. Throwing would tear the connection down right
    /// after the `ErrorResponse` and before that `ReadyForQuery`, and a driver such as `libpq`/`psycopg2`
    /// (unlike the plain `psql` REPL) that is still completing the command cycle then hits EOF and reports
    /// "server closed the connection unexpectedly" instead of surfacing this message.
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->format == ASTCopyQuery::Formats::Binary)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "0A000",
                "PostgreSQL binary COPY format is not supported; use the text or CSV format"),
            true);
        return true;
    }

    /// A `COPY` we cannot faithfully serve is rejected here rather than silently mis-served. This covers a
    /// copy endpoint other than the client stream (only `COPY ... TO STDOUT` and `COPY ... FROM STDIN` are
    /// implemented - a file path or `PROGRAM '...'` would make us drive the wrong side of the protocol), and
    /// a data-formatting option we cannot honor (a non-default `DELIMITER`, a non-default `NULL` marker, a
    /// `HEADER`, or any option we do not interpret): honoring only the format while dropping such options
    /// would stream output that does not match what the client requested. The parser records the reason in
    /// `unsupported_option`. Like the binary rejection above, this is sent as an ordinary `ErrorResponse`
    /// (not thrown) so the connection stays open for the following `ReadyForQuery`.
    if (copy_query_parsed && !copy_query_parsed->as<ASTCopyQuery>()->unsupported_option.empty())
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "0A000",
                fmt::format("PostgreSQL COPY with {} is not supported",
                            copy_query_parsed->as<ASTCopyQuery>()->unsupported_option)),
            true);
        return true;
    }

    /* The Postgres protocol for a copy query is different from simple queries such as SELECT.
     * In the case of a COPY FROM request, the server sends CopyInResponse - a sign of readiness to receive data from the client.
     * The client then sends CopyInData until all data has been sent.
     * After this, the server sends a CommandComplete response.
     * For more detailes see https://www.dolthub.com/blog/2024-09-17-tabular-data-imports/
     */
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->type == ASTCopyQuery::QueryType::COPY_FROM)
    {
        auto * copy_query = copy_query_parsed->as<ASTCopyQuery>();
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        /// PostgreSQL's CSV convention is that an empty unquoted field means NULL (a quoted empty string
        /// stays an empty string), while ClickHouse's CSV default marker is `\N`. Apply the marker the
        /// client asked for - PostgreSQL's default, or an explicit `NULL '\N'` - so that nullable values
        /// are read back faithfully.
        if (copy_query->format == ASTCopyQuery::Formats::CSV)
            query_context->setSetting("format_csv_null_representation", copy_query->csv_null_marker);

        /// Initialize the emulated `pg_catalog` views before the copy, mirroring `processQuery`, so that the
        /// `COPY` path sees the same catalog surface as ordinary queries on a fresh connection.
        if (should_init_system_tables)
        {
            initializeSystemTables(query_context);
            should_init_system_tables = false;
        }

        QueryScope query_scope = QueryScope::create(query_context);

        String columns_to_insert;
        if (!copy_query->column_names.empty())
        {
            for (const auto & column_name : copy_query->column_names)
                columns_to_insert += fmt::format("{}, ", column_name);
            columns_to_insert.pop_back();
            columns_to_insert.pop_back();
            columns_to_insert = "(" + columns_to_insert + ")";
        }

        /// `table_name` is already rendered as valid SQL by the parser (each part of a compound
        /// `database.table` name separately backquoted), so it must not be wrapped in backquotes again.
        auto [ast, io] = executeQuery(fmt::format("INSERT INTO {} {} FROM INFILE 'psql_copy'", copy_query->table_name, columns_to_insert), query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pushing());

        String format = toString(copy_query->format);

        const Settings & settings = query_context->getSettingsRef();

        message_transport->send(PostgreSQLProtocol::Messaging::CopyInResponse(), true);

        /// `CopyData` frame boundaries carry no meaning: a single logical row may arrive split across
        /// several frames (a client is free to chunk the payload however it likes), and one frame may
        /// hold many rows. So the whole `COPY` payload is parsed by a single input format reading the
        /// concatenation of all frames as one stream - never one parser per frame, which would drop a
        /// partial trailing row of every frame.
        ///
        /// The payload is staged in full before any of it is fed to the insert pipeline. Sinks such as
        /// `MergeTreeSink` commit parts while the insert streams (`consume` / `finishDelayedChunk`), so
        /// feeding the pipeline as frames arrive would let a client abort (`CopyFail`) after the first
        /// flushed block leave partial rows visible even though the `COPY` reports failure - and a client
        /// retry would then duplicate them. Staging moves the commit boundary after a successful
        /// `CopyDone`: an aborted copy never touches the insert pipeline at all. The staging buffer is
        /// allocated under the query scope, so the query's memory limits account for it.
        CopyInDataReadBuffer copy_in_stream(*message_transport);
        String staged_data;
        try
        {
            while (!copy_in_stream.eof())
            {
                staged_data.append(copy_in_stream.position(), copy_in_stream.available());
                copy_in_stream.position() += copy_in_stream.available();
            }
        }
        catch (...)
        {
            /// A client-initiated abort (`CopyFail`) is an ordinary "your copy failed" error, not a fatal
            /// connection error: reply with an `ErrorResponse` and report the query as handled so the run
            /// loop follows with `ReadyForQuery` and the connection stays usable (mirroring the binary and
            /// unsupported-option rejections above). Nothing has been pushed to the insert pipeline, so the
            /// target table is untouched. Any other error is a genuine failure - rethrow it.
            if (copy_in_stream.clientAborted())
            {
                message_transport->send(
                    PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "57014",
                        fmt::format("COPY FROM STDIN aborted by the client: {}", copy_in_stream.abortReason())),
                    true);
                return true;
            }
            throw;
        }

        /// The executor is created only after the payload is staged in full: creating it earlier would
        /// leave an unfinished pushing executor behind on the abort return path above.
        auto executor = std::make_unique<PushingPipelineExecutor>(io.pipeline);

        ReadBufferFromString staged_stream(staged_data);
        auto format_ptr = FormatFactory::instance().getInput(
            format,
            staged_stream,
            io.pipeline.getHeader(),
            query_context,
            settings[Setting::max_insert_block_size],
            std::nullopt,
            nullptr,
            nullptr,
            false,
            CompressionMethod::None,
            false,
            settings[Setting::max_insert_block_size_bytes],
            settings[Setting::min_insert_block_size_rows],
            settings[Setting::min_insert_block_size_bytes]);

        executor->start();
        try
        {
            while (true)
            {
                auto chunk = format_ptr->generate();
                if (chunk.empty())
                    break;

                executor->push(std::move(chunk));
            }
            executor->finish();
        }
        catch (...)
        {
            executor->cancel();
            throw;
        }

        auto command = PostgreSQLProtocol::Messaging::CommandComplete::Command::COPY;
        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
        return true;
    }

    /* In the case of a COPY TO request, the server calculates the number of columns and then sends it to the client in CopyOutResponse.
     * After this, the server sends the data in a CopyOutData message, and when the data runs out, it sends a CopyCompletionResponse.
     * For more detailes see https://www.dolthub.com/blog/2024-09-17-tabular-data-imports/
     */
    if (copy_query_parsed && copy_query_parsed->as<ASTCopyQuery>()->type == ASTCopyQuery::QueryType::COPY_TO)
    {
        auto * copy_query = copy_query_parsed->as<ASTCopyQuery>();
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        /// PostgreSQL's CSV convention is that an empty unquoted field means NULL (a quoted empty string is
        /// written as `""`), while ClickHouse's CSV default marker is `\N`. Apply the marker the client
        /// asked for - PostgreSQL's default, or an explicit `NULL '\N'` - so that nullable values are
        /// streamed in the form the client expects.
        if (copy_query->format == ASTCopyQuery::Formats::CSV)
            query_context->setSetting("format_csv_null_representation", copy_query->csv_null_marker);

        /// Lazily create the emulated `pg_catalog` views before running the copied query, exactly as
        /// `processQuery` does for ordinary queries. Otherwise `COPY (SELECT * FROM pg_namespace) TO STDOUT`
        /// on a fresh connection would fail with `UNKNOWN_TABLE` while a plain `SELECT * FROM pg_namespace`
        /// succeeds.
        if (should_init_system_tables)
        {
            initializeSystemTables(query_context);
            should_init_system_tables = false;
        }

        QueryScope query_scope = QueryScope::create(query_context);

        String columns_to_select = "*";
        if (!copy_query->column_names.empty())
        {
            columns_to_select.clear();
            for (const auto & column_name : copy_query->column_names)
                columns_to_select += fmt::format("{}, ", column_name);
            columns_to_select.pop_back();
            columns_to_select.pop_back();
        }

        /// `COPY (query) TO STDOUT` streams the result of an arbitrary query (this is how libpq/pqxx read
        /// result sets); `COPY table TO STDOUT` streams a whole table.
        auto select_query = copy_query->subquery.empty()
            ? fmt::format("SELECT {} FROM {};", columns_to_select, copy_query->table_name)
            : copy_query->subquery;
        auto [ast, io] = executeQuery(select_query, query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pulling());

        /// `Array(...)` columns must be streamed in PostgreSQL array-literal form (`{...}`) rather than
        /// ClickHouse's `[...]`, because libpq/pqxx (and ClickHouse's own `postgresql(...)` source, which
        /// reads the result back with `pqxx::array_parser`) expect the PostgreSQL spelling. The text COPY
        /// output formats (TSV/CSV) have no notion of that, so array columns are pre-rendered here into a
        /// `String` column holding the PostgreSQL literal; the header used for the output format carries
        /// `String` for those columns accordingly. Elements use the `t`/`f` boolean spelling like the rest
        /// of the PostgreSQL wire path. (The binary format is rejected earlier, so this path is text-only.)
        const Block source_header = io.pipeline.getHeader();
        std::vector<UInt8> is_array_column(source_header.columns(), 0);
        Block output_header;
        for (size_t col = 0; col < source_header.columns(); ++col)
        {
            const auto & src = source_header.getByPosition(col);
            if (isArray(src.type))
            {
                is_array_column[col] = 1;
                auto str_type = std::make_shared<DataTypeString>();
                output_header.insert({str_type->createColumn(), str_type, src.name});
            }
            else
                output_header.insert({src.type->createColumn(), src.type, src.name});
        }

        FormatSettings array_settings;
        array_settings.bool_true_representation = "t";
        array_settings.bool_false_representation = "f";

        message_transport->send(PostgreSQLProtocol::Messaging::CopyOutResponse(static_cast<Int32>(source_header.columns())));
        VectorWithMemoryTracking<char> result_buf;
        WriteBufferFromVectorImpl<decltype(result_buf)> output_buffer(result_buf);
        auto format_ptr = FormatFactory::instance().getOutputFormat(toString(copy_query->format), output_buffer, output_header, query_context);
        auto executor = std::make_unique<PullingPipelineExecutor>(io.pipeline);
        Block block;
        Int32 rows_count = 0;
        while (executor->pull(block))
        {
            /// PostgreSQL's COPY protocol expects one CopyData message per row, and libpq/pqxx rely on
            /// this (they do not re-split a message into rows). Serialize each row on its own instead of
            /// formatting the whole block and splitting the result on '\n': the latter only works for the
            /// text/TSV path (where newlines inside values are escaped) and corrupts formats where a single
            /// row is not one physical line - e.g. a quoted CSV field containing a newline, or the binary
            /// format, which is not newline-delimited at all and would otherwise emit nothing.
            Block materialized = materializeBlock(block);
            for (size_t row = 0, num_rows = materialized.rows(); row < num_rows; ++row)
            {
                output_buffer.restart(DBMS_DEFAULT_BUFFER_SIZE); // This will recreate the moved-out vector.

                Columns row_columns;
                row_columns.reserve(materialized.columns());
                for (size_t col = 0; col < materialized.columns(); ++col)
                {
                    const auto & elem = materialized.getByPosition(col);
                    if (is_array_column[col])
                    {
                        auto str_column = ColumnString::create();
                        WriteBufferFromOwnString literal;
                        writePostgreSQLArrayText(*elem.column, *elem.type, row, literal, array_settings);
                        str_column->insertData(literal.str().data(), literal.str().size());
                        row_columns.push_back(std::move(str_column));
                    }
                    else
                        row_columns.push_back(elem.column->cut(row, 1));
                }

                format_ptr->write(output_header.cloneWithColumns(row_columns));
                format_ptr->flush();
                output_buffer.finalize();

                message_transport->send(PostgreSQLProtocol::Messaging::CopyOutData(result_buf));
            }
            rows_count += static_cast<Int32>(materialized.rows());
        }
        /// A COPY TO STDOUT must be terminated by CopyDone, then CommandComplete ("COPY n"), then
        /// ReadyForQuery (sent by the caller). libpq/pqxx report an error if CommandComplete is missing.
        message_transport->send(PostgreSQLProtocol::Messaging::CopyCompletionResponse());
        message_transport->send(
            PostgreSQLProtocol::Messaging::CommandComplete(
                PostgreSQLProtocol::Messaging::CommandComplete::Command::COPY, rows_count),
            true);
        return true;
    }

    return false;
}

void PostgreSQLHandler::processQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::Query> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::Query>();

        if (isEmptyQuery(query->query))
        {
            message_transport->send(PostgreSQLProtocol::Messaging::EmptyQueryResponse());
            return;
        }

        bool transaction_control_cond = isTransactionControlQuery(query->query); // clients wrap statements in BEGIN/COMMIT/ROLLBACK etc.
        bool jdbc_cond = query->query.contains("SET extra_float_digits") || query->query.contains("SET application_name"); // jdbc starts with setting this parameter
        if (transaction_control_cond || jdbc_cond)
        {
            message_transport->send(
                PostgreSQLProtocol::Messaging::CommandComplete(
                    PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query->query), 0));
            return;
        }

        /// Accept driver-specific session-management commands (e.g. `RESET ALL`,
        /// `UNLISTEN *`) as no-ops instead of failing them with a syntax error.
        if (auto noop_command_tag = classifyNoOpDriverCommand(query->query))
        {
            message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(std::move(*noop_command_tag)), true);
            return;
        }

        const auto & settings = session->sessionContext()->getSettingsRef();
        std::vector<String> queries;

        if (processPrepareStatement(query->query))
            return;

        if (processDeallocate(query->query))
            return;

        if (processCopyQuery(query->query))
            return;

        pcg64_fast gen{randomSeed()};
        std::uniform_int_distribution<Int32> dis(0, INT32_MAX);

        secret_key = dis(gen);
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        if (should_init_system_tables)
        {
            initializeSystemTables(query_context);
            should_init_system_tables = false;
        }

        if (processExecute(query->query, query_context))
            return;

        auto parse_res = splitMultipartQuery(
            query->query,
            queries,
            settings[Setting::max_query_size],
            settings[Setting::max_parser_depth],
            settings[Setting::max_parser_backtracks],
            settings[Setting::allow_settings_after_format_in_insert],
            settings[Setting::implicit_select]);
        if (!parse_res.second)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse and execute the following part of query: {}", String(parse_res.first));

        for (auto & sql_query : queries)
        {
            secret_key = dis(gen);
            query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

            QueryScope query_scope = QueryScope::create(query_context);

            PostgreSQLProtocol::Messaging::CommandComplete::Command command =
                PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

            UInt64 affected_rows = executeQueryWithTracking(std::move(sql_query), query_context, command);

            message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);
        }

    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

std::function<void(const Progress&)> PostgreSQLHandler::createProgressCallback(
    ContextMutablePtr query_context,
    std::atomic<UInt64>& result_rows,
    std::atomic<UInt64>& written_rows)
{
    auto prev_callback = query_context->getProgressCallback();
    return [&, my_prev = prev_callback](const Progress & progress)
    {
        if (my_prev)
            my_prev(progress);
        result_rows += progress.result_rows;   // For SELECT
        written_rows += progress.written_rows; // For INSERT
    };
}

UInt64 PostgreSQLHandler::executeQueryWithTracking(
    String && sql_query,
    ContextMutablePtr query_context,
    PostgreSQLProtocol::Messaging::CommandComplete::Command command)
{
    // Track affected rows using progress callback (similar to MySQL handler)
    std::atomic<UInt64> result_rows {0};
    std::atomic<UInt64> written_rows {0};
    query_context->setProgressCallback(createProgressCallback(query_context, result_rows, written_rows));

    // Execute query with PostgreSQLWire output format
    auto read_buf = std::make_unique<ReadBufferFromOwnString>(std::move(sql_query));
    executeQuery(std::move(read_buf), *out, query_context, {});

    // Determine affected rows based on command type
    return (command == PostgreSQLProtocol::Messaging::CommandComplete::Command::INSERT)
        ? written_rows.load()
        : result_rows.load();
}

bool PostgreSQLHandler::processPrepareStatement(const String & query)
{
    auto parser = ParserPrepare();
    ASTPtr prepare;
    try
    {
        prepare = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    prepared_statements_manager.addStatement(prepare->as<ASTPreparedStatement>());

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query);
    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
    return true;
}

bool PostgreSQLHandler::processExecute(const String & query, ContextMutablePtr query_context)
{
    auto parser = ParserExecute();
    ASTPtr prepare;
    try
    {
        prepare = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    auto result_query = prepared_statements_manager.getStatement(prepare->as<ASTExecute>());

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(result_query);

    QueryScope query_scope = QueryScope::create(query_context);

    UInt64 affected_rows = executeQueryWithTracking(std::move(result_query), query_context, command);

    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);

    return true;
}

bool PostgreSQLHandler::processDeallocate(const String & query)
{
    auto parser = ParserDeallocate();
    ASTPtr deallocate;
    try
    {
        deallocate = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        return false;
    }

    prepared_statements_manager.deleteStatement(deallocate->as<ASTDeallocate>()->function_name);

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query);
    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
    return true;
}

void PostgreSQLHandler::processParseQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::ParseQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::ParseQuery>();

        auto statement = make_intrusive<ASTPreparedStatement>();
        statement->function_name = query->function_name;
        statement->function_body = query->sql_query;
        prepared_statements_manager.addStatement(statement.get());
        message_transport->send(PostgreSQLProtocol::Messaging::ParseQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

void PostgreSQLHandler::processBindQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::BindQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::BindQuery>();

        prepared_statements_manager.attachBindQuery(std::move(query));
        message_transport->send(PostgreSQLProtocol::Messaging::BindQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

void PostgreSQLHandler::processDescribeQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::DescribeQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::DescribeQuery>();
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

void PostgreSQLHandler::processExecuteQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::ExecuteQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::ExecuteQuery>();

        /// Only the unnamed portal is supported; the corresponding rejection
        /// for `Bind` lives in `PreparedStatemetsManager::attachBindQuery`.
        if (!query->portal_name.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Execute on a named portal is not supported in the PostgreSQL wire protocol, "
                "got portal name '{}'", query->portal_name);

        pcg64_fast gen{randomSeed()};
        std::uniform_int_distribution<Int32> dis(0, INT32_MAX);

        secret_key = dis(gen);
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        if (should_init_system_tables)
        {
            initializeSystemTables(query_context);
            should_init_system_tables = false;
        }

        QueryScope query_scope = QueryScope::create(query_context);
        auto sql_query = prepared_statements_manager.getStatmentFromBind();

        PostgreSQLProtocol::Messaging::CommandComplete::Command command =
            PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

        UInt64 affected_rows = executeQueryWithTracking(std::move(sql_query), query_context, command);

        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, static_cast<Int32>(affected_rows)), true);
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

void PostgreSQLHandler::processCloseQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::CloseQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::CloseQuery>();

        /// 'S' means close a prepared statement, 'P' means close a portal.
        /// Closing a portal must not deallocate the prepared statement,
        /// otherwise a later Bind/Execute on the same statement would fail.
        if (query->close_target == 'S')
        {
            /// If the bind currently references the statement being deallocated,
            /// the bind becomes stale and must be dropped. Closing a *different*
            /// statement must not touch unrelated bind state — otherwise
            /// `Parse s1; Parse s2; Bind(s1); Close('S', 's2'); Execute` would
            /// fail with `Execute without prior Bind`.
            if (prepared_statements_manager.bindReferencesStatement(query->function_name))
                prepared_statements_manager.resetBindQuery();
            /// Per the PostgreSQL wire protocol, `Close` on a non-existent
            /// prepared statement is not an error — it is a silent no-op that
            /// still responds with `CloseComplete`. Using the throwing
            /// `deleteStatement` here would propagate `BAD_ARGUMENTS` out of
            /// the surrounding `try` block, send an `ErrorResponse`, and drop
            /// the connection on a stray `Close`.
            prepared_statements_manager.tryDeleteStatement(query->function_name);
        }
        else if (query->close_target == 'P')
        {
            /// Only the unnamed portal is supported; rejecting named portals
            /// keeps the behaviour consistent with `Bind` and `Execute`.
            if (!query->function_name.empty())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Close on a named portal is not supported in the PostgreSQL wire protocol, "
                    "got portal name '{}'", query->function_name);
            prepared_statements_manager.resetBindQuery();
        }
        else
        {
            /// Per the PostgreSQL protocol only 'S' (prepared statement) and 'P'
            /// (portal) are valid `Close` targets; any other byte indicates a
            /// malformed packet and must not be silently acknowledged.
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Unexpected `Close` target byte {} in the PostgreSQL wire protocol, "
                "expected 'S' (prepared statement) or 'P' (portal)",
                static_cast<int>(static_cast<unsigned char>(query->close_target)));
        }

        /// Acknowledge the `Close` request. Clients that strictly track the
        /// extended-protocol state machine wait for `CloseComplete` before
        /// proceeding.
        message_transport->send(PostgreSQLProtocol::Messaging::CloseQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

void PostgreSQLHandler::processSyncQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::SyncQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::SyncQuery>();

        /// Per PostgreSQL protocol, `Sync` ends the current extended-query cycle
        /// and destroys the unnamed portal. We only support the unnamed portal
        /// (see `attachBindQuery`), so resetting the single bind slot is
        /// equivalent — the next Parse/Bind/Execute pair starts from a clean state.
        prepared_statements_manager.resetBindQuery();
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        throw;
    }
}

bool PostgreSQLHandler::isEmptyQuery(const String & query)
{
    if (query.empty())
        return true;
    /// golang driver pgx sends ";"
    if (query == ";")
        return true;

    Poco::RegularExpression regex(R"(\A\s*\z)");
    return regex.match(query);
}

bool PostgreSQLHandler::isTransactionControlQuery(const String & query)
{
    String normalized = query;
    /// Trim surrounding whitespace and a single trailing semicolon.
    boost::trim(normalized);
    if (!normalized.empty() && normalized.back() == ';')
    {
        normalized.pop_back();
        boost::trim(normalized);
    }
    Poco::toUpperInPlace(normalized);

    /// A transaction-control statement is a single statement. If an internal semicolon remains after the
    /// trailing one has been trimmed, this is a multi-statement simple query such as `BEGIN READ ONLY; SELECT 1`.
    /// Treating it as ignorable transaction-control would silently drop the trailing statement, so let it fall
    /// through to normal processing instead.
    if (normalized.find(';') != String::npos)
        return false;

    static constexpr std::array prefixes = {"BEGIN", "START TRANSACTION", "COMMIT", "END", "ROLLBACK", "ABORT"};
    for (const auto * prefix : prefixes)
    {
        /// Match either the bare keyword or the keyword followed by a separator, so that we do not
        /// swallow unrelated identifiers such as `ENDPOINT` while still accepting `BEGIN READ ONLY`.
        if (normalized == prefix)
            return true;
        const size_t prefix_len = std::strlen(prefix);
        if (normalized.size() > prefix_len && normalized.starts_with(prefix)
            && (normalized[prefix_len] == ' ' || normalized[prefix_len] == '\t'))
            return true;
    }
    return false;
}

Int32 PostgreSQLHandler::parseNumberColumns(const std::vector<char> & output)
{
    Int32 result = 0;
    for (const auto elem : output)
    {
        if (elem == '\n')
            return result;
        if (elem == '\t')
            result++;
    }
    return result;
}

void PostgreSQLHandler::initializeSystemTables(ContextMutablePtr query_context)
{
    /// Create an internal context from the global context (which has full access, bypassing grant checks)
    /// but sharing the same session context, so that temporary views created here
    /// are visible in subsequent user queries.
    auto internal_context = Context::createCopy(server.context());
    internal_context->makeQueryContext();
    internal_context->setCurrentQueryId(fmt::format("postgres-init:{:d}", connection_id));
    internal_context->setSessionContext(query_context->getSessionContext());

    String out_str;
    auto out_buffer = WriteBufferFromString(out_str);

    auto execute_query = [&](const String & query)
    {
        QueryScope query_scope = QueryScope::create(internal_context);
        ReadBufferFromString read_buf(query);
        executeQuery(read_buf, out_buffer, internal_context, {}, QueryFlags{ .internal = true });
    };

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_type AS
SELECT * FROM VALUES(
    'oid UInt32, typnamespace UInt32, typname String, typrelid UInt32, typnotnull UInt8, typtype String, typreceive UInt32, typelem UInt32, typbasetype UInt32, typcategory String',
    (16,   11, 'bool',      0, 0, 'b', 246, 0, 0, 'B'),
    (17,   11, 'bytea',     0, 0, 'b', 248, 0, 0, 'U'),
    (18,   11, 'char',      0, 0, 'b', 245, 0, 0, 'S'),
    (19,   11, 'name',      0, 0, 'b', 244, 0, 0, 'S'),
    (20,   11, 'int8',      0, 0, 'b', 241, 0, 0, 'N'),
    (21,   11, 'int2',      0, 0, 'b', 243, 0, 0, 'N'),
    (23,   11, 'int4',      0, 0, 'b', 242, 0, 0, 'N'),
    (25,   11, 'text',      0, 0, 'b', 247, 0, 0, 'S'),
    (700,  11, 'float4',    0, 0, 'b', 250, 0, 0, 'N'),
    (701,  11, 'float8',    0, 0, 'b', 251, 0, 0, 'N'),
    (1043, 11, 'varchar',   0, 0, 'b', 249, 0, 0, 'S'),
    (1082, 11, 'date',      0, 0, 'b', 252, 0, 0, 'D'),
    (1114, 11, 'timestamp', 0, 0, 'b', 253, 0, 0, 'D')
))");

    /// `pg_namespace`, `pg_class` and `pg_attribute` combine a fixed set of built-in catalog rows (used by
    /// client type introspection) with rows derived from ClickHouse's own `system.databases`, `system.tables`
    /// and `system.columns`, so that databases, tables and columns of this server are visible through the
    /// PostgreSQL catalog. Both namespace (database) and relation (table) OIDs are joined between catalog
    /// views - `fetchPostgreSQLTableStructure` resolves a schema-qualified table through
    /// `pg_namespace.oid` -> `pg_class.relnamespace` and then pulls its columns by `pg_attribute.attrelid` -
    /// so a hash collision would merge two databases or two tables and corrupt schema inference for both. A
    /// truncated hash is not collision-free enough at realistic catalog sizes, so both are assigned a dense,
    /// unique number in shared views (`pg_namespace_oids` per database, `pg_class_oids` per `(database, table)`
    /// pair) via `row_number`. Because those views are unfiltered, the number for a given database or table is
    /// the same in every reference within a query, so the joins always line up. OID ranges are offset
    /// (namespaces into [1e9, 2e9), relations into [2e9, 3e9)) to avoid colliding with the small built-in
    /// OIDs. In addition, tables of the connected database are also exposed under the default `public` schema
    /// (OID 2200), so that clients that do not qualify a table with a schema - which is what
    /// `fetchPostgreSQLTableStructure` does by default - still resolve it in the current database.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class_oids AS
SELECT
    database,
    name,
    toUInt32(row_number() OVER (ORDER BY database, name) + 2000000000) AS oid
FROM system.tables)");
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_namespace_oids AS
SELECT
    name,
    toUInt32(row_number() OVER (ORDER BY name) + 1000000000) AS oid
FROM system.databases)");
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_namespace AS
SELECT oid, nspname FROM VALUES(
    'oid UInt32, nspname String',
    (11,    'pg_catalog'),
    (2200,  'public'),
    (132,   'information_schema'),
    (11519, 'pg_toast'),
    (99,    'pg_temp_1'),
    (100,   'pg_toast_temp_1')
)
UNION ALL
SELECT oid, name AS nspname
FROM pg_namespace_oids
WHERE name NOT IN ('pg_catalog', 'public', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1'))");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class AS
SELECT oid, relname, relnamespace, relkind FROM VALUES(
    'oid UInt32, relname String, relnamespace UInt32, relkind String',
    (1259, '', 0, 'r'),
    (2615, '', 0, 'i'),
    (1247, '', 0, 'r'),
    (3079, '', 0, 'v'),
    (1260, '', 0, 'c'),
    (1255, '', 0, 'f'),
    (3476, '', 0, 'm'),
    (3074, '', 0, 'S')
)
UNION ALL
SELECT
    oids.oid,
    oids.name AS relname,
    ns.oid AS relnamespace,
    'r' AS relkind
FROM pg_class_oids AS oids
INNER JOIN pg_namespace_oids AS ns ON oids.database = ns.name
UNION ALL
SELECT
    oid,
    name AS relname,
    2200 AS relnamespace,
    'r' AS relkind
FROM pg_class_oids
WHERE database = currentDatabase())");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_proc AS
SELECT * FROM VALUES(
    'oid UInt32, proname String',
    (1247, 'boolin'),
    (1248, 'boolout'),
    (1249, 'byteain'),
    (1250, 'byteaout'),
    (1251, 'charin'),
    (1252, 'charout'),
    (1255, 'namein'),
    (1256, 'nameout'),
    (1259, 'int2in'),
    (1260, 'int2out'),
    (1261, 'int4in'),
    (1262, 'int4out'),
    (1265, 'textin'),
    (1266, 'textout'),
    (1286, 'float4in'),
    (1287, 'float4out'),
    (1288, 'float8in'),
    (1289, 'float8out'),
    (1344, 'date_in'),
    (1345, 'date_out'),
    (2022, 'varcharin'),
    (2023, 'varcharout'),
    (1115, 'timestamp_in'),
    (1116, 'timestamp_out')
))");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_range AS
SELECT * FROM VALUES(
    'rngtypid UInt32, rngsubtype UInt32, rngmultitypid UInt32',
    (3904, 23,   3905),
    (3906, 1700, 3907),
    (3910, 1114, 3911),
    (3912, 1184, 3913),
    (3914, 1082, 3915),
    (3926, 21,   3927)
))");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_attribute AS
SELECT atttypid, attrelid, attname, attnum, attisdropped, atttypmod, attnotnull, attndims, attgenerated FROM VALUES(
    'atttypid UInt32, attrelid UInt32, attname String, attnum Int32, attisdropped UInt8, atttypmod Int32, attnotnull String, attndims Int32, attgenerated String',
    (19, 1247, 'typname',      1, 0, -1, 't', 0, ''),
    (26, 1247, 'typnamespace', 2, 0, -1, 't', 0, ''),
    (23, 1247, 'typrelid',     3, 0, -1, 't', 0, ''),
    (16, 1247, 'typnotnull',   4, 0, -1, 't', 0, ''),
    (25, 1247, 'typtype',      5, 0, -1, 't', 0, ''),
    (26, 1247, 'typreceive',   6, 0, -1, 't', 0, ''),
    (26, 1247, 'typelem',      7, 0, -1, 't', 0, ''),
    (26, 1247, 'typbasetype',  8, 0, -1, 't', 0, ''),
    (18, 1247, 'typcategory',  9, 0, -1, 't', 0, '')
)
UNION ALL
SELECT
    /// A non-array column is advertised with the OID of its scalar type; an array column is advertised with
    /// the OID of the corresponding PostgreSQL array type of its element (the innermost non-array type), and
    /// the array dimensions are reported in `attndims` below, exactly as PostgreSQL does. PostgreSQL has
    /// neither unsigned nor >64-bit integers, so the integer types that do not fit into a signed 64-bit
    /// `bigint` (`UInt64` and the 128/256-bit types) are advertised as `numeric` and carry a precision in
    /// `atttypmod` (see below) large enough to hold every value; the counterpart mapping in
    /// `convertPostgreSQLDataType` turns such a `numeric(p, 0)` back into a Decimal (or `Int256` for a
    /// precision above the Decimal256 range) that preserves the range. Only `UInt32`/`Int64`, which fit into
    /// `bigint`, keep OID 20.
    multiIf(cols.ndims > 0,
                multiIf(cols.base IN ('Bool', 'Boolean'), 1000,
                        cols.base IN ('Int8', 'UInt8', 'Int16'), 1005,
                        cols.base IN ('UInt16', 'Int32'), 1007,
                        cols.base IN ('UInt32', 'Int64'), 1016,
                        cols.base IN ('UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256',
                                      'Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256'), 1231,
                        cols.base = 'Float32', 1021,
                        cols.base = 'Float64', 1022,
                        cols.base = 'UUID', 2951,
                        cols.base IN ('Date', 'Date32'), 1182,
                        cols.is_native_timestamp, 1115,
                        cols.base IN ('String', 'FixedString'), 1009,
                        1009),
            cols.base IN ('Bool', 'Boolean'), 16,
            cols.base IN ('Int8', 'UInt8', 'Int16'), 21,
            cols.base IN ('UInt16', 'Int32'), 23,
            cols.base IN ('UInt32', 'Int64'), 20,
            cols.base IN ('UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256',
                          'Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256'), 1700,
            cols.base = 'Float32', 700,
            cols.base = 'Float64', 701,
            cols.base = 'UUID', 2950,
            cols.base IN ('Date', 'Date32'), 1082,
            cols.is_native_timestamp, 1114,
            cols.base IN ('String', 'FixedString'), 25,
            25) AS atttypid,
    oids.oid AS attrelid,
    cols.name AS attname,
    toInt32(cols.position) AS attnum,
    0 AS attisdropped,
    /// For the types advertised as `numeric`, encode precision and scale the way PostgreSQL does -
    /// `((precision << 16) | scale) + 4` - so that `format_type` renders `numeric(p, s)` and schema
    /// inference recovers the exact type. For an array column the modifier applies to the element type, as in
    /// PostgreSQL. `Decimal` uses its own precision/scale; the wide integer types use a scale of 0 and a
    /// precision that spans their whole value range (the 256-bit types share `numeric(78, 0)`, which holds
    /// every 256-bit integer). `convertPostgreSQLDataType` maps such a `numeric(p > 76, 0)` back to `Int256`;
    /// PostgreSQL `numeric` is signed, so a self-connected `UInt256` above the `Int256` maximum is rejected
    /// on the reading side (fail-closed) rather than recovered.
    ///
    /// For `timestamp`, PostgreSQL stores the fractional-second precision (0..6) directly in the modifier;
    /// carry the `DateTime`/`DateTime64` scale there so `format_type` renders `timestamp(p) without time
    /// zone` and schema inference recovers `DateTime` (a 32-bit `DateTime`, p = 0) or `DateTime64(p)`
    /// (p = 1..6) instead of collapsing every timestamp to `DateTime64(6)`. A `DateTime64(0)` is not
    /// advertised as `timestamp` (see `is_native_timestamp`): scale 0 is indistinguishable on the wire from
    /// a 32-bit `DateTime`, which the reader would recover, narrowing its 64-bit range. A `DateTime64` scale
    /// above 6 does not fit PostgreSQL's `timestamp` at all. Both stay on the text fallback (see `atttypid`
    /// above) and read back as `String` with the full value preserved. The same text fallback applies to a
    /// `DateTime`/`DateTime64` with an explicit
    /// time zone (e.g. `DateTime('UTC')`): PostgreSQL's `timestamp without time zone` cannot carry the zone,
    /// and the reader would reconstruct a plain `DateTime`/`DateTime64(p)` whose values are then interpreted
    /// in the server default time zone - silently shifting the stored epochs whenever the zones differ.
    /// Everything else uses -1 ("no modifier").
    multiIf(cols.base IN ('Decimal', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256')
                AND cols.decimal_precision IS NOT NULL AND cols.decimal_scale IS NOT NULL,
                toInt32(assumeNotNull(cols.decimal_precision) * 65536 + assumeNotNull(cols.decimal_scale) + 4),
            cols.base = 'UInt64', toInt32(20 * 65536 + 4),
            cols.base IN ('Int128', 'UInt128'), toInt32(39 * 65536 + 4),
            cols.base IN ('Int256', 'UInt256'), toInt32(78 * 65536 + 4),
            cols.is_native_timestamp,
                toInt32(assumeNotNull(cols.dt_precision)),
            -1) AS atttypmod,
    /// A column is advertised as nullable (`attnotnull = 'f'`) when the value that a self-connected client
    /// materializes can be NULL: a `Nullable`/`LowCardinality(Nullable(...))` scalar, or an array whose
    /// element type is `Nullable(...)` (e.g. `Array(Nullable(Int32))`, whose type does not start with
    /// `Nullable(`). Otherwise `insertPostgreSQLValue` would rewrite a NULL array element to the element
    /// type's default and silently corrupt results.
    if (position(cols.wrappers, 'Nullable(') > 0, 'f', 't') AS attnotnull,
    cols.ndims AS attndims,
    '' AS attgenerated
FROM (
    SELECT
        database, table, name, position, type,
        /// `system.columns.numeric_precision` / `numeric_scale` are only populated for top-level numeric
        /// columns; for a `Decimal` element wrapped in `Array(...)` (or `Nullable(...)`) they are NULL, so
        /// fall back to parsing the precision and scale out of the type name. Only the leading wrappers
        /// are skipped - the same prefix as in `base` below - so a `Decimal` buried in a `Map`/`Tuple`
        /// argument list is not picked up (such columns never reach the `Decimal` branch anyway).
        coalesce(numeric_precision,
                 toUInt64OrNull(extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*Decimal\(([0-9]+), [0-9]+\)'))) AS decimal_precision,
        coalesce(numeric_scale,
                 toUInt64OrNull(extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*Decimal\([0-9]+, ([0-9]+)\)'))) AS decimal_scale,
        extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*([A-Za-z0-9]+)') AS base,
        /// The leading chain of `Nullable(` / `LowCardinality(` / `Array(` wrappers, reused below to count
        /// array dimensions and to detect whether the value (an array element, or the scalar itself) is
        /// nullable. A `Nullable(` can only appear in this chain right before the innermost scalar, so its
        /// presence marks the element as nullable even for `Array(Nullable(T))`.
        extract(type, '^((?:Nullable\(|LowCardinality\(|Array\()*)') AS wrappers,
        /// Count only the leading `Array(` wrappers. An `Array(` nested inside a `Map`/`Tuple` argument
        /// list must not make the column look like a top-level array: such columns are exposed as text.
        toInt32(countSubstrings(wrappers, 'Array(')) AS ndims,
        /// The fractional-second precision of a `DateTime`/`DateTime64` column (0 for `DateTime`).
        /// `system.columns.datetime_precision` is NULL for an element wrapped in `Array(...)`, so fall
        /// back to parsing the scale out of the type name, skipping the same leading wrappers as `base`.
        coalesce(datetime_precision,
                 toUInt64OrNull(extract(type, '^(?:Nullable\(|LowCardinality\(|Array\()*DateTime64\(([0-9]+)')),
                 if (base = 'DateTime', 0, NULL)) AS dt_precision,
        /// Whether the `DateTime`/`DateTime64` carries an explicit time zone argument (a quoted string
        /// inside the type's parentheses, e.g. `DateTime('UTC')`, `DateTime64(3, 'Europe/Berlin')`),
        /// skipping the same leading wrappers as `base`. Such a column stays on the text fallback:
        /// `timestamp without time zone` cannot represent the zone, and advertising it would make the
        /// reading side reinterpret the values in the server default time zone.
        match(type, '^(?:Nullable\(|LowCardinality\(|Array\()*DateTime(64)?\([^)]*\'') AS dt_has_timezone,
        /// Whether the column may be advertised as a native PostgreSQL `timestamp` (rather than the text
        /// fallback). Only a 32-bit `DateTime` (scale 0) and a `DateTime64` with a scale of 1..6 without a
        /// time zone qualify. A `DateTime64(0)` is deliberately excluded: it would be advertised with the
        /// same `timestamp` + scale-0 modifier as a 32-bit `DateTime`, and the reader recovers a scale-0
        /// timestamp as `DateTime`, which would narrow the 64-bit range and corrupt out-of-range values.
        /// It stays on the text fallback and round-trips losslessly as `String`.
        ((base = 'DateTime' AND dt_precision = 0) OR (base = 'DateTime64' AND dt_precision BETWEEN 1 AND 6))
            AND NOT dt_has_timezone AS is_native_timestamp
    FROM system.columns
    /// The data path streams a table with `SELECT * FROM <table>` (see `processCopyQuery`), which omits
    /// `MATERIALIZED` / `ALIAS` / `EPHEMERAL` columns by default. Advertise exactly that column set here, so
    /// the emulated catalog and the `COPY` payload agree - otherwise schema inference sees more columns than
    /// the stream carries and row decoding goes out of sync.
    WHERE default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')
) AS cols
INNER JOIN pg_class_oids AS oids ON cols.database = oids.database AND cols.table = oids.name)");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_enum AS
SELECT * FROM VALUES(
    'oid UInt32, enumtypid UInt32, enumsortorder Float64, enumlabel String',
    (50000, 40000, 1.0, 'sad'),
    (50001, 40000, 2.0, 'ok'),
    (50002, 40000, 3.0, 'happy')
))");
}

}
