#include <Server/PostgreSQLHandler.h>
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
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>

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
}

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int OPENSSL_ERROR;
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
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_,
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
        auto ctx = Poco::Net::SSLManager::instance().defaultServerContext();
        params.caLocation = config.getString(prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, ctx->getCAPaths().caLocation);

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
    setThreadName("PostgresHandler");

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
    Int32 payload_size;
    Int32 info;
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
    auto ctx = Poco::Net::SSLManager::instance().defaultServerContext();
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
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(Poco::Net::SecureStreamSocket::attach(socket(), ctx));    changeIO(*ss);
}
#else
void PostgreSQLHandler::makeSecureConnectionSSL() {}
#endif

void PostgreSQLHandler::sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message)
{
    std::unordered_map<String, String> & parameters = start_up_message.parameters;

    if (parameters.find("application_name") != parameters.end())
        message_transport->send(PostgreSQLProtocol::Messaging::ParameterStatus("application_name", parameters["application_name"]));
    if (parameters.find("client_encoding") != parameters.end())
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
    ReadBufferFromString replacement(query);

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId("");
    executeQuery(replacement, *out, true, query_context, {});
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

        bool psycopg2_cond = query->query == "BEGIN" || query->query == "COMMIT"; // psycopg2 starts and ends queries with BEGIN/COMMIT commands
        bool jdbc_cond = query->query.contains("SET extra_float_digits") || query->query.contains("SET application_name"); // jdbc starts with setting this parameter
        if (psycopg2_cond || jdbc_cond)
        {
            message_transport->send(
                PostgreSQLProtocol::Messaging::CommandComplete(
                    PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query->query), 0));
            return;
        }

        const auto & settings = session->sessionContext()->getSettingsRef();
        std::vector<String> queries;

        if (processPrepareStatement(query->query))
            return;

        if (processDeallocate(query->query))
            return;

        pcg64_fast gen{randomSeed()};
        std::uniform_int_distribution<Int32> dis(0, INT32_MAX);

        secret_key = dis(gen);
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

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

        for (const auto & spl_query : queries)
        {
            secret_key = dis(gen);
            query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

            CurrentThread::QueryScope query_scope{query_context};
            ReadBufferFromString read_buf(spl_query);
            executeQuery(read_buf, *out, false, query_context, {});

            PostgreSQLProtocol::Messaging::CommandComplete::Command command =
                PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(spl_query);
            message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
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

    CurrentThread::QueryScope query_scope{query_context};
    ReadBufferFromString read_buf(result_query);
    executeQuery(read_buf, *out, false, query_context, {});

    PostgreSQLProtocol::Messaging::CommandComplete::Command command =
        PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(result_query);
    message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);

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

    prepared_statements_manager.deleteStatement(deallocate->as<ASTDeallocate>());

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

        auto statement = std::make_shared<ASTPreparedStatement>();
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

        pcg64_fast gen{randomSeed()};
        std::uniform_int_distribution<Int32> dis(0, INT32_MAX);

        secret_key = dis(gen);
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        auto sql_query = prepared_statements_manager.getStatmentFromBind();
        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(sql_query);
        executeQuery(read_buf, *out, false, query_context, {});

        PostgreSQLProtocol::Messaging::CommandComplete::Command command = PostgreSQLProtocol::Messaging::CommandComplete::Command::SELECT;
        message_transport->send(PostgreSQLProtocol::Messaging::CommandComplete(command, 0), true);
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

        prepared_statements_manager.resetBindQuery(query->function_name);
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

}
