#include <memory>
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
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <pcg_random.hpp>
#include <Common/CacheBase.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Core/PostgreSQLProtocol.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ParserCopyQuery.h>
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
    extern const int SYNTAX_ERROR;
    extern const int OPENSSL_ERROR;
}

namespace
{

const char * start_dotnet_query = R"(SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
FROM (
    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a
    -- We first do this for the type (innerest-most subquery), and then for its element type
    -- This also returns the array element, range subtype and domain base type as elemtypoid
    SELECT
        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
        , typ.typcategory
    FROM (
        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
            CASE
                WHEN proc.proname='array_recv' THEN typ.typelem
                WHEN typ.typtype='r' THEN rngsubtype
                WHEN typ.typtype='m' THEN (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid)
                WHEN typ.typtype='d' THEN typ.typbasetype
            END AS elemtypoid
            , typ.typcategory
        FROM pg_type AS typ
        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
    ) AS typ
    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
) AS t
JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
WHERE
    (
    typtype IN ('b', 'r', 'm', 'e', 'd') OR -- Base, range, multirange, enum, domain
    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default
    (typtype = 'p' AND typname IN ('record', 'void', 'unknown')) OR -- Some special supported pseudo-types
    (typtype = 'a' AND (  -- Array of...
        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR -- Array of base, range, multirange, enum, domain
        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types
        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default
    )))
ORDER BY CASE
       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types
       WHEN typtype = 'c' THEN 1                        -- Composites after (fields loaded later in 2nd pass)
       WHEN typtype = 'r' THEN 2                        -- Ranges after
       WHEN typtype = 'm' THEN 3                        -- Multiranges after
       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 -- Domains over non-arrays after
       WHEN typtype = 'a' THEN 5                        -- Arrays after
       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  -- Domains over arrays last
END;)";

const char * translated_start_dotnet_query = R"(
        WITH 
            typ AS (
                SELECT
                    typ.oid AS oid,
                    typ.typnamespace AS typnamespace,
                    typ.typname AS typname,
                    typ.typtype AS typtype,
                    typ.typrelid AS typrelid,
                    typ.typnotnull AS typnotnull,
                    typ.typelem AS typelem,
                    cls.relkind AS relkind,
                    -- multiIf(proc.proname = 'array_recv', 'a', typ.typtype) AS typtype,
                    multiIf(
                        proc.proname = 'array_recv', typ.typelem,
                        typ.typtype = 'r', rngsubtype,
                        -- typ.typtype = 'm', (SELECT rngtypid FROM pg_range WHERE rngmultitypid = typ.oid LIMIT 1),
                        -- typ.typtype = 'd', typ.typbasetype,
                        NULL
                    ) AS elemtypoid,
                    typ.typcategory
                FROM pg_type AS typ
                LEFT JOIN pg_class AS cls ON cls.oid = typ.typrelid
                LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
                LEFT JOIN pg_range ON pg_range.rngtypid = typ.oid
            ),
            elemtyp AS (
                SELECT 
                    elemtyp.oid AS elemtyp_oid,
                    elemtyp.typname AS elemtypname,
                    elemtyp.typtype AS elemtyptype,
                    elemcls.relkind AS elemrelkind
                    -- multiIf(elemproc.proname = 'array_recv', 'a', elemtyptype) AS elemtyptype
                FROM pg_type AS elemtyp
                LEFT JOIN pg_class AS elemcls ON elemcls.oid = elemtyp.typrelid
                LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
            )
        SELECT
            ns.nspname,
            t.oid,
            t.typname,
            t.typtype,
            t.typnotnull,
            t.elemtypoid
        FROM typ AS t
        LEFT JOIN elemtyp ON elemtyp.elemtyp_oid = t.elemtypoid
        INNER JOIN pg_namespace AS ns ON ns.oid = t.typnamespace
        WHERE 
            (t.typtype IN ('b', 'r', 'm', 'e', 'd')) 
            OR ((t.typtype = 'c') AND (t.relkind = 'c')) 
            OR ((t.typtype = 'p') AND (t.typname IN ('record', 'void', 'unknown'))) 
            OR (
                (t.typtype = 'a') AND (
                    (elemtyp.elemtyptype IN ('b', 'r', 'm', 'e', 'd')) 
                    OR ((elemtyp.elemtyptype = 'p') AND (elemtyp.elemtypname IN ('record', 'void'))) 
                    OR ((elemtyp.elemtyptype = 'c') AND (elemtyp.elemrelkind = 'c'))
                )
            )
        ORDER BY multiIf(
            t.typtype IN ('b', 'e', 'p'), 0, 
            t.typtype = 'c', 1, 
            t.typtype = 'r', 2, 
            t.typtype = 'm', 3, 
            (t.typtype = 'd') AND (elemtyp.elemtyptype != 'a'), 4, 
            t.typtype = 'a', 5, 
            (t.typtype = 'd') AND (elemtyp.elemtyptype = 'a'), 6, 
            7
        ) ASC;
)";
        
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
    std::unordered_map<String, String> & parameters = start_up_message.parameters;

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
        CurrentThread::QueryScope query_scope{query_context};

        String columns_to_insert;
        if (!copy_query->column_names.empty())
        {
            for (const auto & column_name : copy_query->column_names)
                columns_to_insert += fmt::format("{}, ", column_name);
            columns_to_insert.pop_back();
            columns_to_insert.pop_back();
            columns_to_insert = "(" + columns_to_insert + ")";
        }

        auto [ast, io] = executeQuery(fmt::format("INSERT INTO `{}` {} FROM INFILE 'psql_copy'", copy_query->table_name, columns_to_insert), query_context, {}, QueryProcessingStage::Enum::Complete);
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
        query_context->setCurrentQueryId(fmt::format("postgres:{:d}:{:d}", connection_id, secret_key));

        CurrentThread::QueryScope query_scope{query_context};

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
        std::vector<char> result_buf;
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

            CurrentThread::QueryScope query_scope{query_context};
            auto corrected_sql_query = sql_query;

            if (sql_query == start_dotnet_query)
                corrected_sql_query = translated_start_dotnet_query;

            PostgreSQLProtocol::Messaging::CommandComplete::Command command =
                PostgreSQLProtocol::Messaging::CommandComplete::classifyQuery(sql_query);

            UInt64 affected_rows = executeQueryWithTracking(std::move(corrected_sql_query), query_context, command);

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

    CurrentThread::QueryScope query_scope{query_context};

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

        CurrentThread::QueryScope query_scope{query_context};
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

        prepared_statements_manager.deleteStatement(query->function_name);
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
    String out_str;
    auto out_buffer = WriteBufferFromString(out_str);

    auto execute_query = [&] (const String & query)
    {
        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);
        executeQuery(read_buf, out_buffer, query_context, {});
    };

    execute_query("DROP TABLE IF EXISTS pg_type;");
    execute_query("DROP TABLE IF EXISTS pg_namespace;");
    execute_query("DROP TABLE IF EXISTS pg_class;");
    execute_query("DROP TABLE IF EXISTS pg_proc;");
    execute_query("DROP TABLE IF EXISTS pg_range;");
    execute_query("DROP TABLE IF EXISTS pg_attribute;");
    execute_query("DROP TABLE IF EXISTS pg_enum;");
    execute_query(R"(
CREATE TABLE pg_type
(
    oid UInt32,
    typnamespace UInt32,
    typname String,
    typrelid UInt32,
    typnotnull UInt8,
    typtype String,
    typreceive UInt32,
    typelem UInt32,
    typbasetype UInt32,
    typcategory String
)
ENGINE = MergeTree
ORDER BY oid;)");

    execute_query(R"(
CREATE TABLE pg_proc
(
    oid UInt32,
    proname String
)
ENGINE = MergeTree
ORDER BY oid;
    )");

    execute_query(R"(
CREATE TABLE pg_class
(
    oid UInt32,
    relkind String
)
ENGINE = MergeTree
ORDER BY oid;
    )");

    execute_query(R"(
CREATE TABLE pg_range
(
    rngtypid UInt32,
    rngsubtype UInt32,
    rngmultitypid UInt32
)
ENGINE = MergeTree
ORDER BY rngtypid;
    )");

    execute_query(R"(
CREATE TABLE pg_namespace
(
    oid UInt32,
    nspname String
)
ENGINE = MergeTree
ORDER BY oid;
    )");

    execute_query(R"(
CREATE TABLE pg_attribute
(
    atttypid UInt32,
    attrelid UInt32,
    attname String,
    attnum Int32,
    attisdropped Boolean
)
ENGINE = MergeTree
ORDER BY atttypid;
    )");

    execute_query(R"(
CREATE TABLE pg_enum
(
    oid UInt32,
    enumtypid UInt32,
    enumsortorder Float64,
    enumlabel String
)
ENGINE = MergeTree
ORDER BY oid;
    )");

    execute_query(R"(
INSERT INTO pg_type VALUES
(16,    11,            'bool',      0,         0,           'b',       246,         0,        0,            'B'),
(17,    11,            'bytea',     0,         0,           'b',       248,         0,        0,            'U'),
(18,    11,            'char',      0,         0,           'b',       245,         0,        0,            'S'),
(19,    11,            'name',      0,         0,           'b',       244,         0,        0,            'S'),
(20,    11,            'int8',      0,         0,           'b',       241,         0,        0,            'N'),
(21,    11,            'int2',      0,         0,           'b',       243,         0,        0,            'N'),
(23,    11,            'int4',      0,         0,           'b',       242,         0,        0,            'N'),
(25,    11,            'text',      0,         0,           'b',       247,         0,        0,            'S'),
(1043,  11,            'varchar',   0,         0,           'b',       249,         0,        0,            'S'),
(700,   11,            'float4',    0,         0,           'b',       250,         0,        0,            'N'),
(701,   11,            'float8',    0,         0,           'b',       251,         0,        0,            'N'),
(1082,  11,            'date',      0,         0,           'b',       252,         0,        0,            'D'),
(1114,  11,            'timestamp', 0,         0,           'b',       253,         0,        0,            'D');
    )");


    execute_query(R"(
INSERT INTO pg_proc VALUES
(1247,  'boolin'),
(1248,  'boolout'),
(1249,  'byteain'),
(1250,  'byteaout'),
(1251,  'charin'),
(1252,  'charout'),
(1255,  'namein'),
(1256,  'nameout'),
(1259,  'int2in'),
(1260,  'int2out'),
(1261,  'int4in'),
(1262,  'int4out'),
(1265,  'textin'),
(1266,  'textout'),
(1286,  'float4in'),
(1287,  'float4out'),
(1288,  'float8in'),
(1289,  'float8out'),
(1344,  'date_in'),
(1345,  'date_out'),
(2022,  'varcharin'),
(2023,  'varcharout'),
(1115,  'timestamp_in'),
(1116,  'timestamp_out');
    )");

    execute_query(R"(
INSERT INTO pg_class VALUES
(1259,   'r'),
(2615,   'i'),
(1247,   'r'),
(3079,   'v'),
(1260,   'c'),
(1255,   'f'),
(3476,   'm'),
(3074,   'S');
    )");

    execute_query(R"(
INSERT INTO pg_range VALUES
(3904,       23,          3905),
(3906,       1700,        3907),
(3910,       1114,        3911),
(3912,       1184,        3913),
(3914,       1082,        3915),
(3926,       21,          3927);
)");

    execute_query(R"(
INSERT INTO pg_namespace VALUES
(11,    'pg_catalog'),
(2200,  'public'),
(132,   'information_schema'),
(11519, 'pg_toast'),
(99,    'pg_temp_1'),
(100,   'pg_toast_temp_1');
    )");

    execute_query(R"(
INSERT INTO pg_attribute VALUES
(19,        1247,       'typname',        1,       false),
(26,        1247,       'typnamespace',   2,       false),
(23,        1247,       'typrelid',       3,       false),
(16,        1247,       'typnotnull',     4,       false),
(25,        1247,       'typtype',        5,       false),
(26,        1247,       'typreceive',     6,       false),
(26,        1247,       'typelem',        7,       false),
(26,        1247,       'typbasetype',    8,       false),
(18,        1247,       'typcategory',    9,       false);
    )");

    execute_query(R"(
INSERT INTO pg_enum VALUES
(50000, 40000,        1.0,            'sad'),
(50001, 40000,        2.0,            'ok'),
(50002, 40000,        3.0,            'happy');
    )");
}

}
