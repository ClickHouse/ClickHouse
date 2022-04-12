#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include "PostgreSQLHandler.h"
#include <Parsers/parseQuery.h>
#include <Server/TCPServer.h>
#include <Common/setThreadName.h>
#include <base/scope_guard.h>
#include <random>

#include <Common/config_version.h>

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

PostgreSQLHandler::PostgreSQLHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    Int32 connection_id_,
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , connection_id(connection_id_)
    , authentication_manager(auth_methods_)
{
    changeIO(socket());
}

void PostgreSQLHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket);
    out = std::make_shared<WriteBufferFromPocoSocket>(socket);
    message_transport = std::make_shared<PostgreSQLProtocol::Messaging::MessageTransport>(in.get(), out.get());
}

void PostgreSQLHandler::run()
{
    setThreadName("PostgresHandler");
    ThreadStatus thread_status;

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::POSTGRESQL);
    SCOPE_EXIT({ session.reset(); });

    try
    {
        if (!startup())
            return;

        while (tcp_server.isOpen())
        {
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
                    break;
                case PostgreSQLProtocol::Messaging::FrontMessageType::TERMINATE:
                    LOG_DEBUG(log, "Client closed the connection");
                    return;
                case PostgreSQLProtocol::Messaging::FrontMessageType::PARSE:
                case PostgreSQLProtocol::Messaging::FrontMessageType::BIND:
                case PostgreSQLProtocol::Messaging::FrontMessageType::DESCRIBE:
                case PostgreSQLProtocol::Messaging::FrontMessageType::SYNC:
                case PostgreSQLProtocol::Messaging::FrontMessageType::FLUSH:
                case PostgreSQLProtocol::Messaging::FrontMessageType::CLOSE:
                    message_transport->send(
                        PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse(
                            PostgreSQLProtocol::Messaging::ErrorOrNoticeResponse::ERROR,
                            "0A000",
                            "ClickHouse doesn't support extended query mechanism"),
                        true);
                    LOG_ERROR(log, "Client tried to access via extended query protocol");
                    message_transport->dropMessage();
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
    bool was_encryption_req = true;
    readBinaryBigEndian(payload_size, *in);
    readBinaryBigEndian(info, *in);

    switch (static_cast<PostgreSQLProtocol::Messaging::FrontMessageType>(info))
    {
        case PostgreSQLProtocol::Messaging::FrontMessageType::SSL_REQUEST:
            LOG_DEBUG(log, "Client requested SSL");
            if (ssl_enabled)
                makeSecureConnectionSSL();
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
}

#if USE_SSL
void PostgreSQLHandler::makeSecureConnectionSSL()
{
    message_transport->send('S');
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(
        Poco::Net::SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext()));
    changeIO(*ss);
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
        bool jdbc_cond = query->query.find("SET extra_float_digits") != String::npos || query->query.find("SET application_name") != String::npos; // jdbc starts with setting this parameter
        if (psycopg2_cond || jdbc_cond)
        {
            message_transport->send(
                PostgreSQLProtocol::Messaging::CommandComplete(
                    PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(query->query), 0));
            return;
        }

        const auto & settings = session->sessionContext()->getSettingsRef();
        std::vector<String> queries;
        auto parse_res = splitMultipartQuery(query->query, queries,
            settings.max_query_size,
            settings.max_parser_depth,
            settings.allow_settings_after_format_in_insert);
        if (!parse_res.second)
            throw Exception("Cannot parse and execute the following part of query: " + String(parse_res.first), ErrorCodes::SYNTAX_ERROR);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<Int32> dis(0, INT32_MAX);

        for (const auto & spl_query : queries)
        {
            secret_key = dis(gen);
            auto query_context = session->makeQueryContext();
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

bool PostgreSQLHandler::isEmptyQuery(const String & query)
{
    if (query.empty())
        return true;

    Poco::RegularExpression regex(R"(\A\s*\z)");
    return regex.match(query);
}

}
