#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <Server/PostgreSQLHandler.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/SettingSource.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <Core/PostgreSQLProtocol.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ParserCopyQuery.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>

#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserQuery.h>
#include <fmt/format.h>
#include <Formats/FormatFactory.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

#if USE_SSL
#    include <Common/OpenSSLHelpers.h>
#    include <Server/CertificateReloader.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#    include <openssl/rand.h>
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

namespace ServerSetting
{
    extern const ServerSettingsString default_session_user;
}

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
    extern const int OPENSSL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

namespace
{

UInt32 generateRandomUInt32()
{
    UInt32 secret_key = 0;

#if USE_SSL
    if (RAND_bytes(reinterpret_cast<unsigned char *>(&secret_key), sizeof(secret_key)) != 1)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "RAND_bytes failed: {}", getOpenSSLErrors());
#else
    const int random_fd = ::open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (random_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open /dev/urandom");

    SCOPE_EXIT({ [[maybe_unused]] int err = ::close(random_fd); });

    auto * position = reinterpret_cast<char *>(&secret_key);
    size_t bytes_remaining = sizeof(secret_key);
    while (bytes_remaining > 0)
    {
        ssize_t bytes_read = ::read(random_fd, position, bytes_remaining);
        if (bytes_read == -1)
        {
            if (errno == EINTR)
                continue;

            throw ErrnoException(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read from /dev/urandom");
        }

        if (bytes_read == 0)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of /dev/urandom");

        position += bytes_read;
        bytes_remaining -= bytes_read;
    }
#endif

    return secret_key;
}

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
    std::optional<String> default_session_user_,
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
    , default_session_user(std::move(default_session_user_))
    , read_event(read_event_)
    , write_event(write_event_)
    , authentication_manager(auth_methods_)
    , prepared_statements_manager(std::nullopt)
{
    /// `BackendKeyData` identifies every statement on this connection for cancellation.
    secret_key = generateRandomUInt32();
    query_id_token = generateRandomUInt32();

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
            else if (token == "tlsv1_3")
                disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_3;
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

    /// A `CancelRequest` for this connection arrives on a different connection, so the secret has
    /// to be reachable from the whole server for as long as this one is open.
    server.context()->getProcessList().registerPostgreSQLCancellationKey(connection_id, secret_key, currentQueryId());
    SCOPE_EXIT({ server.context()->getProcessList().unregisterPostgreSQLCancellationKey(connection_id, secret_key); });

    try
    {
        if (!startup())
            return;

        /// Emit `ReadyForQuery` only at explicit protocol boundaries.
        need_ready_for_query = true;

        while (tcp_server.isOpen())
        {
            if (need_ready_for_query)
            {
                message_transport->send(PostgreSQLProtocol::Messaging::ReadyForQuery(), true);
                need_ready_for_query = false;
            }

            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = message_transport->receiveMessageType();
            if (!tcp_server.isOpen())
                return;

            /// After an extended-query error, discard through `Sync` but honor `Terminate`.
            if (ignore_until_sync
                && message_type != PostgreSQLProtocol::Messaging::FrontMessageType::SYNC
                && message_type != PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE)
            {
                message_transport->dropMessage();
                continue;
            }

            switch (message_type)
            {
                case PostgreSQLProtocol::Messaging::FrontMessageType::QUERY:
                    /// A simple query is a complete protocol cycle.
                    processQuery();
                    need_ready_for_query = true;
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE:
                    LOG_DEBUG(log, "Client closed the connection");
                    return;
                case PostgreSQLProtocol::Messaging::FrontMessageType::PARSE:
                    /// Extended-query cycles end only at `Sync`.
                    processParseQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::BIND:
                    processBindQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::EXECUTE:
                    processExecuteQuery();
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::SYNC:
                    /// `Sync` ends the cycle and produces one `ReadyForQuery`.
                    processSyncQuery();
                    need_ready_for_query = true;
                    message_transport->flush();
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::DESCRIBE:
                    processDescribeQuery();
                    message_transport->flush();
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
                    /// Discard the rest of this extended-query cycle.
                    ignore_until_sync = true;
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
                    /// Treat unsupported messages as extended-query errors.
                    ignore_until_sync = true;
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

    /// An empty user name means the default session user: the `default_session_user`
    /// server setting, possibly overridden for this listener in the `protocols` section.
    /// If the resolved name is empty too (explicitly configured to prohibit connections
    /// without a user name), authentication fails on the empty user name below.
    if (start_up_msg->user.empty())
        start_up_msg->user = default_session_user
            ? *default_session_user
            : String(server.context()->getServerSettings()[ServerSetting::default_session_user]);

    const auto & user_name = start_up_msg->user;
    if (user_name.empty())
    {
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Got an empty user name from PostgreSQL startup message");
        session->onAuthenticationFailure(user_name, socket().peerAddress(), exception);
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "28P01", "Invalid user or password"),
            true);
        return false;
    }

    authentication_manager.authenticate(user_name, *session, *message_transport, socket().peerAddress());

    try
    {
        session->makeSessionContext();
        session->sessionContext()->setDefaultFormat("PostgreSQLWire");
        if (!start_up_msg->database.empty())
        {
            /// `database` is a real setting, so enforce its constraints on the startup-message
            /// database too, consistently with `USE`, `SET database = ...` and the HTTP
            /// `?database=...` parameter.
            SettingsChanges database_change;
            database_change.setSetting("database", start_up_msg->database);
            session->sessionContext()->checkSettingsConstraints(database_change, SettingSource::QUERY);
            session->sessionContext()->setCurrentDatabase(start_up_msg->database);
        }
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

String PostgreSQLHandler::queryIdFor(Int32 connection_id_, UInt32 query_id_token_)
{
    /// The random component is a token of its own and never the secret from `BackendKeyData`:
    /// `system.processes` and `system.query_log` expose query IDs verbatim, while the secret
    /// authenticates `CancelRequest`. It still has to be here, because a query ID that another
    /// interface can predict can be occupied to keep a PostgreSQL statement from starting.
    return fmt::format("postgres:{:d}:{:d}", connection_id_, query_id_token_);
}

String PostgreSQLHandler::currentQueryId() const
{
    return queryIdFor(connection_id, query_id_token);
}

void PostgreSQLHandler::assignStatementQueryId(ContextMutablePtr query_context)
{
    /// One statement, one query ID: a query ID may be held by only one query at a time across the
    /// whole server, so an ID that outlived its statement would keep the next one from starting.
    query_id_token = generateRandomUInt32();

    const String query_id = currentQueryId();
    query_context->setCurrentQueryId(query_id);
    /// `CancelRequest` names the connection, so its entry has to follow the current statement.
    server.context()->getProcessList().registerPostgreSQLCancellationKey(connection_id, secret_key, query_id);
}

void PostgreSQLHandler::cancelRequest()
{
    std::unique_ptr<PostgreSQLProtocol::Messaging::CancelRequest> msg =
        message_transport->receiveWithPayloadSize<PostgreSQLProtocol::Messaging::CancelRequest>(8);

    /// The process ID and secret key authenticate this otherwise unauthenticated request.
    /// PostgreSQL exposes no response, so report the outcome only to the log.
    CancellationCode code = server.context()->getProcessList().sendCancelToPostgreSQLQuery(msg->process_id, msg->secret_key);
    LOG_DEBUG(log, "Cancellation request for connection {}: {}", msg->process_id,
        code == CancellationCode::CancelSent ? "sent" : "not sent");
}

inline std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> PostgreSQLHandler::receiveStartupMessage(int payload_size)
{
    /// The declared size is read from the wire before any authentication, and the message is read
    /// into memory in full, so it has to be bounded. PostgreSQL uses the same limit.
    static constexpr Int32 max_startup_message_size = 10000;

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> message;
    try
    {
        if (payload_size < 8 || payload_size > max_startup_message_size)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Startup message declares a size of {} bytes, while it must be between 8 and {} bytes",
                payload_size, max_startup_message_size);

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

/// PostgreSQL clients qualify catalog tables and functions with the `pg_catalog`
/// schema, e.g. `pg_catalog.pg_class` or `pg_catalog.pg_table_is_visible(c.oid)`
/// (psql does so for the `\d` command). ClickHouse has no `pg_catalog` database:
/// the catalog tables are emulated with per-session temporary views
/// (see `initializeSystemTables`) and the functions are registered globally.
/// Removing the qualifier at the token level maps such queries onto them.
/// String literals are left intact - only a `pg_catalog` identifier that is not
/// itself qualified and is followed by a dot and another identifier is removed.
/// PostgreSQL folds unquoted identifiers to lower case, so a bare `PG_CATALOG` names
/// the same schema and is matched case-insensitively; a quoted identifier keeps its
/// case in PostgreSQL, so only the exact `"pg_catalog"` spelling is matched there.
static String removePgCatalogQualifier(const String & query)
{
    static constexpr std::string_view pg_catalog = "pg_catalog";

    /// A fast path for the common case of a query that does not mention the schema at all.
    if (std::search(query.begin(), query.end(), pg_catalog.begin(), pg_catalog.end(),
            [](char a, char b) { return equalsCaseInsensitive(a, b); }) == query.end())
        return query;

    std::vector<Token> tokens;
    Lexer lexer(query.data(), query.data() + query.size());
    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        tokens.push_back(token);

    auto is_pg_catalog = [](const Token & token)
    {
        std::string_view text(token.begin, token.size());
        return (token.type == TokenType::BareWord && equalsCaseInsensitive(text, pg_catalog))
            || (token.type == TokenType::QuotedIdentifier && text == "\"pg_catalog\"");
    };

    auto next_significant = [&](size_t i) -> std::optional<size_t>
    {
        for (size_t j = i + 1; j < tokens.size(); ++j)
            if (tokens[j].isSignificant())
                return j;
        return std::nullopt;
    };

    String result;
    result.reserve(query.size());
    std::optional<size_t> prev_emitted_significant;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const Token & token = tokens[i];
        if (is_pg_catalog(token)
            && (!prev_emitted_significant || tokens[*prev_emitted_significant].type != TokenType::Dot))
        {
            auto dot = next_significant(i);
            if (dot && tokens[*dot].type == TokenType::Dot)
            {
                auto after_dot = next_significant(*dot);
                if (after_dot
                    && (tokens[*after_dot].type == TokenType::BareWord || tokens[*after_dot].type == TokenType::QuotedIdentifier))
                {
                    /// Skip the qualifier and the dot (and anything insignificant in between).
                    i = *dot;
                    continue;
                }
            }
        }
        result.append(token.begin, token.end);
        if (token.isSignificant())
            prev_emitted_significant = i;
    }
    return result;
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
        assignStatementQueryId(query_context);
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

        /// The parser has already quoted each part of `table_name`.
        auto [ast, io] = executeQuery(fmt::format("INSERT INTO {} {} FROM INFILE 'psql_copy'", copy_query->table_name, columns_to_insert), query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pushing());
        auto executor = std::make_unique<PushingPipelineExecutor>(io.pipeline);

        String format;
        switch (copy_query->format)
        {
        case ASTCopyQuery::Formats::TSV:
            format = "TSV";
            break;
        case ASTCopyQuery::Formats::CSV:
            format = "CSV";
            break;
        case ASTCopyQuery::Formats::Binary:
            format = "RowBinary";
            break;
        }

        const Settings & settings = query_context->getSettingsRef();

        message_transport->send(PostgreSQLProtocol::Messaging::CopyInResponse(), true);
        executor->start();
        while (true)
        {
            message_transport->flush();
            PostgreSQLProtocol::Messaging::FrontMessageType message_type = message_transport->receiveMessageType();
            if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA)
            {
                std::unique_ptr<PostgreSQLProtocol::Messaging::CopyInData> data_query =
                    message_transport->receive<PostgreSQLProtocol::Messaging::CopyInData>();

                ReadBufferFromString buf(data_query->query);
                auto format_ptr = FormatFactory::instance().getInput(
                    format,
                    buf,
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
                while (true)
                {
                    auto chunk = format_ptr->generate();
                    if (chunk.empty())
                        break;

                    executor->push(std::move(chunk));
                }
            }
            else if (message_type == PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION)
            {
                message_transport->receive<PostgreSQLProtocol::Messaging::CopyDone>();
                executor->finish();
                break;
            }
            else
            {
                executor->cancel();
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Received incorrect message type - expected {} or {}, got {}", PostgreSQLProtocol::Messaging::FrontMessageType::COPY_DATA, PostgreSQLProtocol::Messaging::FrontMessageType::COPY_COMPLETION, message_type);
            }
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
        assignStatementQueryId(query_context);

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

        auto select_query = fmt::format("SELECT {} FROM {};", columns_to_select, copy_query->table_name);
        auto [ast, io] = executeQuery(select_query, query_context, {}, QueryProcessingStage::Enum::Complete);
        chassert(io.pipeline.pulling());
        message_transport->send(PostgreSQLProtocol::Messaging::CopyOutResponse(static_cast<Int32>(io.pipeline.getHeader().columns())));
        VectorWithMemoryTracking<char> result_buf;
        WriteBufferFromVectorImpl<decltype(result_buf)> output_buffer(result_buf);
        auto format_ptr = FormatFactory::instance().getOutputFormat(toString(copy_query->format), output_buffer, io.pipeline.getHeader(), query_context);
        auto executor = std::make_unique<PullingPipelineExecutor>(io.pipeline);
        Block block;
        while (executor->pull(block))
        {
            output_buffer.restart(DBMS_DEFAULT_BUFFER_SIZE); // This will recreate moved vector
            format_ptr->write(materializeBlock(block));
            format_ptr->flush();
            output_buffer.finalize();
            message_transport->send(PostgreSQLProtocol::Messaging::CopyOutData(result_buf));
            result_buf.clear();
        }
        message_transport->send(PostgreSQLProtocol::Messaging::CopyCompletionResponse(), true);
        return true;
    }

    return false;
}

void PostgreSQLHandler::processQuery()
{
    /// Output position before the currently executing statement. If a statement
    /// fails when nothing has been sent for it yet, the session can be kept alive.
    size_t out_bytes_before_statement = out->count();
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::Query> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::Query>();

        if (isEmptyQuery(query->query))
        {
            message_transport->send(PostgreSQLProtocol::Messaging::EmptyQueryResponse());
            return;
        }

        bool psycopg2_cond = query->query == "BEGIN" || query->query == "COMMIT"; // psycopg2 starts and ends queries with BEGIN/COMMIT commands
        bool jdbc_cond = query->query.contains("SET extra_float_digits") || query->query.contains("SET application_name"); // jdbc starts with setting this parameter
        if (psycopg2_cond || jdbc_cond)
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

        String query_text = removePgCatalogQualifier(query->query);

        const auto & settings = session->sessionContext()->getSettingsRef();
        std::vector<String> queries;

        if (processPrepareStatement(query_text))
            return;

        if (processDeallocate(query_text))
            return;

        if (processCopyQuery(query_text))
            return;

        auto query_context = session->makeQueryContext();
        assignStatementQueryId(query_context);

        if (should_init_system_tables)
        {
            initializeSystemTables(query_context);
            should_init_system_tables = false;
        }

        if (processExecute(query_text, query_context))
            return;

        auto parse_res = splitMultipartQuery(
            query_text,
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
            assignStatementQueryId(query_context);

            QueryScope query_scope = QueryScope::create(query_context);

            PostgreSQLProtocol::Messaging::CommandComplete::Command command =
                PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

            out_bytes_before_statement = out->count();
            UInt64 affected_rows = executeQueryWithTracking(std::move(sql_query), query_context, command);

            message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, affected_rows), true);
        }

    }
    catch (const Exception & e)
    {
        bool nothing_sent_for_failed_statement = out->count() == out_bytes_before_statement;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// A failed query does not terminate the session in PostgreSQL: the server
        /// sends `ErrorResponse` and returns to the `ReadyForQuery` state. This is
        /// only safe while nothing has been sent for the failed statement -
        /// otherwise the output stream may be cut in the middle of a message and
        /// continuing would desynchronize the protocol framing, so in that case
        /// tear the connection down.
        if (nothing_sent_for_failed_statement)
            return;
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

    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, affected_rows), true);

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
        statement->function_body = removePgCatalogQualifier(query->sql_query);
        statement->parameter_types = query->parameter_types;
        prepared_statements_manager.addStatement(statement.get());
        message_transport->send(PostgreSQLProtocol::Messaging::ParseQueryComplete(), true);
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// Keep the connection alive and discard messages through `Sync`.
        ignore_until_sync = true;
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
        /// Keep the connection alive and discard messages through `Sync`.
        ignore_until_sync = true;
    }
}

void PostgreSQLHandler::processDescribeQuery()
{
    try
    {
        std::unique_ptr<PostgreSQLProtocol::Messaging::DescribeQuery> query =
            message_transport->receive<PostgreSQLProtocol::Messaging::DescribeQuery>();

        /// Row layout is unknown until `Execute`, which emits `RowDescription`.
    }
    catch (const Exception & e)
    {
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// Keep the connection alive and discard messages through `Sync`.
        ignore_until_sync = true;
    }
}

void PostgreSQLHandler::processExecuteQuery()
{
    /// Output position before the statement, mirroring `processQuery`: keeping
    /// the session alive after a failure is only safe while nothing has been
    /// sent for the failed statement.
    size_t out_bytes_before_statement = out->count();
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

        auto query_context = session->makeQueryContext();
        assignStatementQueryId(query_context);

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

        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, affected_rows), true);
    }
    catch (const Exception & e)
    {
        bool nothing_sent_for_failed_statement = out->count() == out_bytes_before_statement;
        message_transport->send(
            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR, "2F000", "Query execution failed.\n" + e.displayText()),
            true);
        /// Recovering to `Sync` is only safe while nothing has been sent for the
        /// failed statement - otherwise the output stream may be cut in the
        /// middle of a message and continuing would desynchronize the protocol
        /// framing, so in that case tear the connection down (as `processQuery`
        /// does for the simple-query protocol).
        if (nothing_sent_for_failed_statement)
        {
            ignore_until_sync = true;
            return;
        }
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
            /// The portal retains its `Bind` snapshot after the statement closes.
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
        /// Keep the connection alive and discard messages through `Sync`.
        ignore_until_sync = true;
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
        ignore_until_sync = false;
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

    /// Fixed rows are the namespaces PostgreSQL clients expect to always exist
    /// (their well-known oids are hardcoded in some drivers, e.g. 11 for `pg_catalog`).
    /// The rest of the namespaces are the real databases. An oid identifies an object,
    /// and PostgreSQL clients are allowed to remember one and use it in a later query, so
    /// it is a pure function of the name of the object: a hash of the name - qualified
    /// with the database for a relation - and nothing else. Whatever else is currently
    /// visible - and therefore any unrelated DDL or grant change - cannot renumber an
    /// object that a client already saw.
    /// The oids are also expected to be unique, because clients join `pg_class` to
    /// `pg_namespace` on them. A mapping into a bounded space cannot guarantee both
    /// properties at once, and PostgreSQL gets uniqueness only because it assigns oids
    /// from a persistent counter, which a stateless emulation of the catalog has no
    /// analog of. Stability is the more important of the two - a renumbering is a wrong
    /// answer to a client that cached an oid, while a hash collision merely lists one of
    /// two objects under a wrong schema - so the hash is spread over the whole available
    /// range instead of being corrected: two visible names share an oid only if their
    /// hashes collide, which takes tens of thousands of databases or tables in a single
    /// catalog to become likely at all.
    /// The offset 16384 mirrors PostgreSQL, where oids below 16384 are reserved for the
    /// system, so synthesized oids cannot collide with the well-known ones; namespaces
    /// take the even oids and the tables of `pg_class` the odd ones, so the two
    /// enumerations cannot collide with each other either. The modulo keeps the result
    /// below 2^32, the width of an oid.
    /// `SQL SECURITY INVOKER` makes the view run with the privileges of the session
    /// user. `system.databases` is implicitly SELECTable by every user and hides
    /// the databases the user has no `SHOW` privilege for, so the view exposes
    /// exactly the metadata already visible to that user - definer rights would
    /// bypass this filtering and leak the existence of unrelated databases.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_namespace SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'oid UInt32, nspname String',
    (11,    'pg_catalog'),
    (2200,  'public'),
    (11519, 'pg_toast'),
    (99,    'pg_temp_1'),
    (100,   'pg_toast_temp_1')
)
UNION ALL
SELECT
    toUInt32(16384 + 2 * (sipHash64(name) % 2000000000)) AS oid,
    name AS nspname
FROM system.databases)");

    /// Fixed rows (oid, relkind) are preserved for driver compatibility; they belong
    /// to the `pg_catalog` namespace, which clients such as psql filter out.
    /// The rest are the tables of the current database - the analog of the PostgreSQL
    /// search path - which makes commands like `\d` in psql list the actual tables.
    /// `relam` is the access method: 2 (`heap`) for tables and 0 for views, as in PostgreSQL.
    /// The oid of a relation is a hash of its qualified name - the database and the table
    /// name - and not of the table name alone: a session can switch the current database
    /// with `USE`, and two same-named tables in two databases are different objects that
    /// must not share an oid.
    /// `SQL SECURITY INVOKER` for the same reason as `pg_namespace` above:
    /// `system.tables` hides the tables the session user cannot `SHOW`.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_class SQL SECURITY INVOKER AS
SELECT * FROM VALUES(
    'oid UInt32, relname String, relnamespace UInt32, relowner UInt32, relam UInt32, relkind String',
    (1259, '', 11, 10, 2, 'r'),
    (2615, '', 11, 10, 2, 'i'),
    (1247, '', 11, 10, 2, 'r'),
    (3079, '', 11, 10, 2, 'v'),
    (1260, '', 11, 10, 2, 'c'),
    (1255, '', 11, 10, 2, 'f'),
    (3476, '', 11, 10, 2, 'm'),
    (3074, '', 11, 10, 2, 'S')
)
UNION ALL
SELECT
    toUInt32(16385 + 2 * (sipHash64(database, name) % 2000000000)) AS oid,
    name AS relname,
    toUInt32(16384 + 2 * (sipHash64(currentDatabase()) % 2000000000)) AS relnamespace,
    toUInt32(10) AS relowner,
    toUInt32(if(endsWith(engine, 'View'), 0, 2)) AS relam,
    multiIf(engine = 'MaterializedView', 'm', endsWith(engine, 'View'), 'v', 'r') AS relkind
FROM system.tables
WHERE database = currentDatabase() AND NOT is_temporary)");

    /// Table access methods. Newer psql versions join `pg_am` in the query behind
    /// the `\d` command. ClickHouse table engines have no PostgreSQL equivalent,
    /// so everything is presented as the default `heap` access method.
    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_am AS
SELECT * FROM VALUES(
    'oid UInt32, amname String, amtype String',
    (2, 'heap', 't')
))");

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
SELECT * FROM VALUES(
    'atttypid UInt32, attrelid UInt32, attname String, attnum Int32, attisdropped UInt8',
    (19, 1247, 'typname',      1, 0),
    (26, 1247, 'typnamespace', 2, 0),
    (23, 1247, 'typrelid',     3, 0),
    (16, 1247, 'typnotnull',   4, 0),
    (25, 1247, 'typtype',      5, 0),
    (26, 1247, 'typreceive',   6, 0),
    (26, 1247, 'typelem',      7, 0),
    (26, 1247, 'typbasetype',  8, 0),
    (18, 1247, 'typcategory',  9, 0)
))");

    execute_query(R"(CREATE TEMPORARY VIEW IF NOT EXISTS pg_enum AS
SELECT * FROM VALUES(
    'oid UInt32, enumtypid UInt32, enumsortorder Float64, enumlabel String',
    (50000, 40000, 1.0, 'sad'),
    (50001, 40000, 2.0, 'ok'),
    (50002, 40000, 3.0, 'happy')
))");
}

}
