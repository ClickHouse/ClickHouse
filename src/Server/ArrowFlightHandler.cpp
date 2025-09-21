#include <Server/ArrowFlightHandler.h>

#if USE_ARROWFLIGHT
#include <memory>
#include <arrow/compute/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/types.h>
#include <arrow/ipc/api.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/macros.h>

#include <fstream>
#include <sstream>
#include <Access/Credentials.h>
#include <Core/Block.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Server/IServer.h>
#include <Storages/IStorage.h>
#include <arrow/flight/server.h>
#include <arrow/flight/server_middleware.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Common/logger_useful.h>

const std::string AUTHORIZATION_HEADER = "authorization";

const std::string AUTHORIZATION_MIDDLEWARE_NAME = "authorization_middleware";
const std::string SESSIONS_MIDDLEWARE_NAME = "sessions_middleware";

class AuthMiddleware : public arrow::flight::ServerMiddleware
{
public:
    explicit AuthMiddleware(const std::string & token, const std::string & username, const std::string & password)
        : token_(token)
        , username_(username)
        , password_(password)
    {
    }

    static AuthMiddleware & get(const arrow::flight::ServerCallContext & context)
    {
        return *static_cast<AuthMiddleware *>(context.GetMiddleware(AUTHORIZATION_MIDDLEWARE_NAME));
    }

    const std::string & username() const { return username_; }
    const std::string & password() const { return password_; }

    void SendingHeaders(arrow::flight::AddCallHeaders * outgoing_headers) override
    {
        outgoing_headers->AddHeader(AUTHORIZATION_HEADER, "Bearer " + token_);
    }

    void CallCompleted(const arrow::Status & /*status*/) override { }

    std::string name() const override { return AUTHORIZATION_MIDDLEWARE_NAME; }

private:
    const std::string token_;
    const std::string username_;
    const std::string password_;
};

class AuthMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory
{
public:
    arrow::Status StartCall(
        const arrow::flight::CallInfo & /*info*/,
        const arrow::flight::ServerCallContext & context,
        std::shared_ptr<arrow::flight::ServerMiddleware> * middleware) override
    {
        const auto & headers = context.incoming_headers();

        auto it = headers.find(AUTHORIZATION_HEADER);
        if (it == headers.end())
        {
            return arrow::Status::IOError("Missing Authorization header");
        }

        auto auth_header = std::string(it->second);

        std::string token;

        const std::string prefix_basic = "Basic ";
        if (auth_header.starts_with(prefix_basic))
        {
            token = auth_header.substr(prefix_basic.size());
        }

        const std::string prefix_bearer = "Bearer ";
        if (auth_header.starts_with(prefix_bearer))
        {
            token = auth_header.substr(prefix_bearer.size());
        }

        if (token.empty())
        {
            return arrow::Status::IOError("Expected Basic auth scheme");
        }

        std::string credentials = DB::base64Decode(token, true);
        auto pos = credentials.find(':');
        if (pos == std::string::npos)
        {
            return arrow::Status::IOError("Malformed credentials");
        }

        auto user = credentials.substr(0, pos);
        auto password = credentials.substr(pos + 1);

        *middleware = std::make_unique<AuthMiddleware>(token, user, password);
        return arrow::Status::OK();
    }
};

String readFile(const String & filepath)
{
    Poco::FileInputStream ifs(filepath);
    String buf;
    Poco::StreamCopier::copyToString(ifs, buf);
    return buf;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_EXCEPTION;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    arrow::flight::Location addressToArrowLocation(const Poco::Net::SocketAddress & address_to_listen, bool use_tls)
    {
        auto ip_to_listen = address_to_listen.host();
        auto port_to_listen = address_to_listen.port();

        /// Function arrow::flight::Location::ForGrpc*() builds an URL based so it requires IPv6 address to be enclosed in brackets
        String host_component = (ip_to_listen.family() == Poco::Net::AddressFamily::IPv6) ? ("[" + ip_to_listen.toString() + "]") : ip_to_listen.toString();

        arrow::Result<arrow::flight::Location> parse_location_status;
        if (use_tls)
            parse_location_status = arrow::flight::Location::ForGrpcTls(host_component, port_to_listen);
        else
            parse_location_status = arrow::flight::Location::ForGrpcTcp(host_component, port_to_listen);

        if (!parse_location_status.ok())
        {
            throw Exception(
                ErrorCodes::UNKNOWN_EXCEPTION,
                "Invalid address {} for Arrow Flight Server: {}",
                address_to_listen.toString(),
                parse_location_status.status().ToString());
        }

        return std::move(parse_location_status).ValueOrDie();
    }

    String convertGetDescriptorToSQL(const arrow::flight::FlightDescriptor & flight_descriptor)
    {
        switch (flight_descriptor.type)
        {
            case arrow::flight::FlightDescriptor::PATH:
            {
                const auto & path = flight_descriptor.path;
                if ((path.size() != 1) || path[0].empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Flight descriptor's path should specify the name of a table");
                const String & table_name = path[0];
                return "SELECT * FROM " + table_name;
            }
            case arrow::flight::FlightDescriptor::CMD:
                return flight_descriptor.cmd;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Flight descriptor has an unknown type: {}", flight_descriptor.type);
        }
    }

    String convertPutDescriptorToSQL(const arrow::flight::FlightDescriptor & flight_descriptor)
    {
        switch (flight_descriptor.type)
        {
            case arrow::flight::FlightDescriptor::PATH:
            {
                const auto & path = flight_descriptor.path;
                if ((path.size() != 1) || path[0].empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Flight descriptor's path should specify the name of a table");
                const String & table_name = path[0];
                return "INSERT INTO " + table_name;
            }
            case arrow::flight::FlightDescriptor::CMD:
                return flight_descriptor.cmd;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Flight descriptor has an unknown type: {}", flight_descriptor.type);
        }
    }


    class SessionsMiddleware : public arrow::flight::ServerMiddleware
    {
    public:
        std::string name() const override { return SESSIONS_MIDDLEWARE_NAME; }
        void SendingHeaders(arrow::flight::AddCallHeaders * /*outgoing_headers*/) override {}
        void CallCompleted(const arrow::Status & /*status*/) override {}

        static SessionsMiddleware & get(const arrow::flight::ServerCallContext & context)
        {
            return *static_cast<SessionsMiddleware *>(context.GetMiddleware(SESSIONS_MIDDLEWARE_NAME));
        }

        struct SessionInfo
        {
            arrow::flight::FlightDescriptor flight_descriptor;
            String sql;
            ASTPtr ast;
            std::unique_ptr<Session> session;
            ContextPtr query_context;
            std::optional<CurrentThread::QueryScope> query_scope;
            BlockIO io;
            std::unique_ptr<PullingPipelineExecutor> pulling_executor;
            std::unique_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
            bool is_pulling = false;
        };

        using SessionPtr = std::shared_ptr<const SessionInfo>;

        struct BlockInfo
        {
            size_t rows = 0;
            size_t bytes = 0;
        };

        SessionPtr addSession(std::unique_ptr<SessionInfo> new_session)
        {
            std::lock_guard lock{mutex};
            auto extended = std::make_shared<ExtendedInfo>(std::move(new_session));
            sessions_by_sql[extended->sql] = extended;
            return extended;
        }

        SessionPtr findSessionBySQL(const String & sql, bool start_pulling = false) const
        {
            std::lock_guard lock{mutex};
            auto it = sessions_by_sql.find(sql);
            if (it == sessions_by_sql.end())
                return nullptr;
            if (start_pulling)
            {
                if (it->second->is_pulling || !it->second->io.pipeline.pulling())
                    return nullptr;
                it->second->is_pulling = true;
            }
            return it->second;
        }

        void addTicket(SessionPtr session, const String & ticket, Block && block)
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            extended->tickets.push_back(ticket);
            BlockInfo block_info{.rows = block.rows(), .bytes = block.bytes()};
            extended->blocks_by_ticket[ticket] = std::move(block);
            extended->block_infos_by_ticket[ticket] = block_info;
            sessions_by_ticket[ticket] = extended;
        }

        SessionPtr findSessionByTicket(const String & ticket) const
        {
            std::lock_guard lock{mutex};
            auto it = sessions_by_ticket.find(ticket);
            if (it == sessions_by_ticket.end())
                return nullptr;
            return it->second;
        }

        size_t getNumTickets(SessionPtr session) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->tickets.size();
        }

        Strings getTickets(SessionPtr session, size_t limit = static_cast<size_t>(-1)) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            Strings tickets(extended->tickets.begin(), extended->tickets.begin() + std::min(extended->tickets.size(), limit));
            return tickets;
        }

        Block getBlockByTicket(SessionPtr session, const String & ticket) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->blocks_by_ticket.at(ticket);
        }

        BlockInfo getBlockInfoByTicket(SessionPtr session, const String & ticket) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->block_infos_by_ticket.at(ticket);
        }

        bool isOpen(SessionPtr session) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->is_open;
        }

        void close(SessionPtr session)
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            extended->is_open = false;
        }

        void addPollDescriptor(SessionPtr session, const String & poll_descriptor, size_t num_tickets)
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            extended->poll_descriptors.resize(std::min(extended->poll_descriptors.size(), num_tickets + 1));
            extended->poll_descriptors[num_tickets] = poll_descriptor;
            extended->num_tickets_by_poll_descriptor[poll_descriptor] = num_tickets;
            sessions_by_poll_descriptor[poll_descriptor] = extended;
        }

        SessionPtr findSessionByPollDescriptor(const String & poll_descriptor) const
        {
            std::lock_guard lock{mutex};
            auto it = sessions_by_poll_descriptor.find(poll_descriptor);
            if (it == sessions_by_poll_descriptor.end())
                return nullptr;
            return it->second;
        }

        size_t getNumTicketsByPollDescriptor(SessionPtr session, const String & poll_descriptor) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->num_tickets_by_poll_descriptor.at(poll_descriptor);
        }

        String getPollDescriptorByNumTickets(SessionPtr session, size_t num_tickets) const
        {
            std::lock_guard lock{mutex};
            auto extended = ExtendedInfo::get(session);
            return extended->poll_descriptors.at(num_tickets);
        }

    private:
        struct ExtendedInfo : public SessionInfo
        {
            Strings tickets; /// Tickets to read pulled blocks.
            bool is_open = true;
            std::unordered_map<String, Block> blocks_by_ticket;
            std::unordered_map<String, BlockInfo> block_infos_by_ticket;
            Strings poll_descriptors;
            std::unordered_map<String, size_t> num_tickets_by_poll_descriptor;

            static std::shared_ptr<ExtendedInfo> get(SessionPtr ptr)
            {
                return typeid_cast<std::shared_ptr<ExtendedInfo>>(std::const_pointer_cast<SessionInfo>(ptr));
            }
        };

        std::mutex mutex;
        std::unordered_map<String, std::shared_ptr<ExtendedInfo>> sessions_by_sql TSA_GUARDED_BY(mutex);
        std::unordered_map<String, std::shared_ptr<ExtendedInfo>> sessions_by_ticket TSA_GUARDED_BY(mutex);
        std::unordered_map<String, std::shared_ptr<ExtendedInfo>> sessions_by_poll_descriptor TSA_GUARDED_BY(mutex);
    };

    class SessionsMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory
    {
    public:
        arrow::Status StartCall(
            const arrow::flight::CallInfo & /*info*/,
            const arrow::flight::ServerCallContext & /*context*/,
            std::shared_ptr<arrow::flight::ServerMiddleware> * middleware) override
        {
            *middleware = std::make_unique<SessionsMiddleware>();
            return arrow::Status::OK();
        }
    };


    SessionsMiddleware::SessionPtr createSession(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & flight_descriptor,
        const ContextPtr & global_context,
        const String & sql,
        bool for_pulling = false,
        bool start_pulling = false)
    {
        auto session_info = std::make_unique<SessionsMiddleware::SessionInfo>();
        session_info->flight_descriptor = flight_descriptor;
        session_info->sql = sql;
        session_info->session = std::make_unique<Session>(global_context, ClientInfo::Interface::ARROW_FLIGHT);

        const auto & auth = AuthMiddleware::get(context);
        session_info->session->authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

        auto query_context = session_info->session->makeQueryContext();
        session_info->query_context = query_context;
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.

        session_info->query_scope.emplace(query_context);

        std::tie(session_info->ast, session_info->io) = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

        if (for_pulling)
        {
            auto & pipeline = session_info->io.pipeline;
            if (!pipeline.pulling())
                throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Query doesn't allow pulling data, use method do_put() with this kind of query");

            session_info->pulling_executor = std::make_unique<PullingPipelineExecutor>(pipeline);
            const Block header = session_info->pulling_executor->getHeader();
            CHColumnToArrowColumn::Settings arrow_settings;
            arrow_settings.output_string_as_string = true;  
            session_info->ch_to_arrow_converter = std::make_unique<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
            session_info->is_pulling = start_pulling;
        }

        auto & middleware = SessionsMiddleware::get(context);
        return middleware.addSession(std::move(session_info));
    }

    SessionsMiddleware::SessionPtr createSessionForPulling(
        const arrow::flight::ServerCallContext & context,
        const arrow::flight::FlightDescriptor & flight_descriptor,
        const ContextPtr & global_context,
        const String & sql,
        bool start_pulling)
    {
        return createSession(context, flight_descriptor, global_context, sql, /* for_pulling = */ true, start_pulling);
    }

    String generateTicket()
    {
        return fmt::format("TICKET-{}", toString(UUIDHelpers::generateV4()));
    }

    String generatePollDescriptor()
    {
        return fmt::format("POLL-{}", toString(UUIDHelpers::generateV4()));
    }
}


ArrowFlightHandler::ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightHandler"))
    , address_to_listen(address_to_listen_)
{
}

void ArrowFlightHandler::start()
{
    chassert(!initialized && !stopped);

    bool use_tls = server.config().getBool("arrowflight.enable_ssl", false);

    auto location = addressToArrowLocation(address_to_listen, use_tls);

    arrow::flight::FlightServerOptions options(location);
    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.emplace_back(AUTHORIZATION_MIDDLEWARE_NAME, std::make_shared<AuthMiddlewareFactory>());
    options.middleware.emplace_back(SESSIONS_MIDDLEWARE_NAME, std::make_shared<SessionsMiddlewareFactory>());

    if (use_tls)
    {
        auto cert_path = server.config().getString("arrowflight.ssl_cert_file");
        auto key_path = server.config().getString("arrowflight.ssl_key_file");

        auto cert = readFile(cert_path);
        auto key = readFile(key_path);

        options.tls_certificates.push_back(arrow::flight::CertKeyPair{cert, key});
    }

    auto init_status = Init(options);
    if (!init_status.ok())
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed init Arrow Flight Server: {}", init_status.ToString());
    }

    initialized = true;

    server_thread.emplace([this]
    {
        try
        {
            setThreadName("ArrowFlightSrv");
            if (stopped)
                return;
            auto serve_status = Serve();
            if (!serve_status.ok())
                LOG_ERROR(log, "Failed to serve Arrow Flight: {}", serve_status.ToString());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to serve Arrow Flight");
        }
    });
}

ArrowFlightHandler::~ArrowFlightHandler() = default;

void ArrowFlightHandler::stop()
{
    if (!initialized)
        return;

    if (!stopped.exchange(true))
    {
        try
        {
            auto status = Shutdown();
            if (!status.ok())
                LOG_ERROR(log, "Failed to shutdown Arrow Flight: {}", status.ToString());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to shutdown Arrow Flight");
        }
        if (server_thread)
        {
            server_thread->join();
            server_thread.reset();
        }
    }
}

UInt16 ArrowFlightHandler::portNumber() const
{
    return address_to_listen.port();
}

arrow::Status ArrowFlightHandler::ListFlights(
    const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *, std::unique_ptr<arrow::flight::FlightListing> * listings)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::ListFlights");
    std::vector<arrow::flight::FlightInfo> flights;
    *listings = std::make_unique<arrow::flight::SimpleFlightListing>(std::move(flights));
    return arrow::Status::OK();
}

arrow::Status ArrowFlightHandler::GetFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & descriptor,
    std::unique_ptr<arrow::flight::FlightInfo> * res)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::GetFlightInfo");
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        String sql = convertGetDescriptorToSQL(descriptor);
        auto & middleware = SessionsMiddleware::get(context);

        auto session = middleware.findSessionBySQL(sql, /* start_pulling = */ true);
        if (!session)
            session = createSessionForPulling(context, descriptor, server.context(), sql, /* start_pulling = */ true);

        auto schema = session->ch_to_arrow_converter->getArrowSchema();
        auto & executor = *session->pulling_executor;

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_records = 0;
        int64_t total_bytes = 0;

        Block block;
        while (executor.pull(block))
        {
            if (!block.empty())
            {
                String ticket = generateTicket();
                total_records += block.rows();
                total_bytes += block.bytes();
                middleware.addTicket(session, ticket, std::move(block));
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket};
                endpoints.emplace_back(endpoint);
            }
        }

        middleware.close(session);

        auto info = arrow::flight::FlightInfo::Make(
            schema,
            session->flight_descriptor,
            endpoints,
            total_records,
            total_bytes,
            /* ordered = */ true);

        *res = std::make_unique<arrow::flight::FlightInfo>(std::move(info).ValueOrDie());
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("GetFlightInfo failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::PollFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::PollInfo> * info)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::PollFlightInfo");

    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        auto & middleware = SessionsMiddleware::get(context);
        SessionsMiddleware::SessionPtr session;
        size_t num_tickets = 0;

        if (request.type == arrow::flight::FlightDescriptor::CMD)
        {
            session = middleware.findSessionByPollDescriptor(request.cmd);
            if (session)
                num_tickets = middleware.getNumTicketsByPollDescriptor(session, request.cmd);
        }

        if (!session)
        {
            String sql = convertGetDescriptorToSQL(request);
            session = middleware.findSessionBySQL(sql, /* start_pulling = */ true);
            if (!session)
                session = createSessionForPulling(context, request, server.context(), sql, /* start_pulling = */ true);
            num_tickets = 1;
        }

        auto & executor = *session->pulling_executor;

        while ((middleware.getNumTickets(session) < num_tickets) && middleware.isOpen(session))
        {
            Block block;
            if (executor.pull(block))
            {
                if (!block.empty())
                {
                    String ticket = generateTicket();
                    middleware.addTicket(session, ticket, std::move(block));
                    String next_descriptor = generatePollDescriptor();
                    middleware.addPollDescriptor(session, next_descriptor, middleware.getNumTickets(session) + 1);
                }
            }
            else
            {
                middleware.close(session);
            }
        }

        std::unique_ptr<arrow::flight::FlightInfo> flight_info_res;

        Strings tickets = middleware.getTickets(session, num_tickets);
        if (!tickets.empty())
        {
            std::vector<arrow::flight::FlightEndpoint> endpoints;
            int64_t total_records = 0;
            int64_t total_bytes = 0;

            for (const auto & ticket : tickets)
            {
                auto block_info = middleware.getBlockInfoByTicket(session, ticket);
                total_records += block_info.rows;
                total_bytes += block_info.bytes;
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket};
                endpoints.emplace_back(endpoint);
            }

            auto schema = session->ch_to_arrow_converter->getArrowSchema();

            auto flight_info = arrow::flight::FlightInfo::Make(schema, request, endpoints, total_records, total_bytes, /* ordered = */ true);
            flight_info_res = std::make_unique<arrow::flight::FlightInfo>(flight_info.ValueOrDie());
        }

        std::optional<arrow::flight::FlightDescriptor> next_descriptor_res;
        if ((num_tickets < middleware.getNumTickets(session)) || middleware.isOpen(session))
            next_descriptor_res = arrow::flight::FlightDescriptor::Command(middleware.getPollDescriptorByNumTickets(session, num_tickets + 1));

        *info = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info_res), std::move(next_descriptor_res), std::nullopt, std::nullopt);
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("PollFlightInfo failed: " + e.displayText());
    }
}


arrow::Status ArrowFlightHandler::GetSchema(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * res)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::GetSchema");
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        String sql = convertGetDescriptorToSQL(request);
        auto & middleware = SessionsMiddleware::get(context);

        auto session = middleware.findSessionBySQL(sql, /* start_pulling = */ false);
        if (!session)
            session = createSessionForPulling(context, request, server.context(), sql, /* start_pulling = */ false);

        auto schema = session->ch_to_arrow_converter->getArrowSchema();
        auto schema_result = arrow::flight::SchemaResult::Make(schema);

        *res = std::make_unique<arrow::flight::SchemaResult>(*std::move(schema_result).ValueOrDie());
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("GetSchema failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoGet(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Ticket & request,
    std::unique_ptr<arrow::flight::FlightDataStream> * stream)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::DoGet");
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        auto & middleware = SessionsMiddleware::get(context);
        std::vector<Chunk> chunks;

        auto session = middleware.findSessionByTicket(request.ticket);
        if (session)
        {
            auto block = middleware.getBlockByTicket(session, request.ticket);
            chunks.emplace_back(Chunk(block.getColumns(), block.rows()));
        }
        else
        {
            const auto & sql = request.ticket;
            session = middleware.findSessionBySQL(sql, /* start_pulling = */ true);
            if (!session)
            {
                arrow::flight::FlightDescriptor descriptor;
                descriptor.type = arrow::flight::FlightDescriptor::CMD;
                descriptor.cmd = sql;
                session = createSessionForPulling(context, descriptor, server.context(), sql, /* start_pulling = */ true);
            }

            auto & executor = *session->pulling_executor;

            Block block;
            while (executor.pull(block))
                chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

            middleware.close(session);
        }

        CHColumnToArrowColumn & converter = *session->ch_to_arrow_converter.get();
        auto header = session->io.pipeline.getHeader();

        std::shared_ptr<arrow::Table> arrow_table;
        converter.chChunkToArrowTable(arrow_table, chunks, header.columns());

        auto maybe_combined = arrow_table->CombineChunks();
        if (!maybe_combined.ok())
        {
            return arrow::Status::IOError("DoGet failed: cannot combine chunks: " + maybe_combined.status().ToString());
        }

        const auto & combined_table = maybe_combined.ValueOrDie();
        arrow::TableBatchReader reader(combined_table);

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::RecordBatch> batch;

        while (true)
        {
            auto st = reader.ReadNext(&batch);
            if (!st.ok())
                return arrow::Status::IOError("DoGet failed while reading batch: " + st.ToString());

            if (!batch)
                break;

            batches.emplace_back(std::move(batch));
        }

        if (batches.empty())
            return arrow::Status::Invalid("DoGet failed: no data produced");

        auto schema = batches.front()->schema();
        auto reader_result = arrow::RecordBatchReader::Make(batches, schema);
        if (!reader_result.ok())
        {
            return arrow::Status::IOError("DoGet failed: " + reader_result.status().ToString());
        }

        *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader_result.ValueOrDie());
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("DoGet failed: " + e.displayText());
    }
}


arrow::Status ArrowFlightHandler::DoPut(
    const arrow::flight::ServerCallContext & context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> /*writer*/)
{
    setThreadName("ArrowFlight");
    DB::ThreadStatus thread_status;

    try
    {
        const auto & descriptor = reader->descriptor();
        String sql = convertPutDescriptorToSQL(descriptor);

        auto session = createSession(context, descriptor, server.context(), sql);

        auto * insert = dynamic_cast<DB::ASTInsertQuery *>(session->ast.get());
        if (!insert)
        {
            return arrow::Status::Invalid("DoPut failed: only INSERT is allowed");
        }

        if (!insert->format.empty() && insert->format != "Arrow")
        {
            return arrow::Status::Invalid("DoPut failed: invalid format value, only 'Arrow' custom format supported");
        }

        auto & pipeline = session->io.pipeline;
        if (pipeline.pushing())
        {
            Block header = pipeline.getHeader();
            auto input = std::make_shared<ArrowFlightSource>(std::move(reader), header);
            pipeline.complete(Pipe(std::move(input)));
        }
        else if (pipeline.pulling())
        {
            Block header = pipeline.getHeader();
            auto output = std::make_shared<NullSink>(header);
            pipeline.complete(Pipe(std::move(output)));
        }

        CompletedPipelineExecutor executor(pipeline);
        executor.execute();

        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::ExecutionError("DoPut failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoExchange(
    const arrow::flight::ServerCallContext & /*context*/,
    std::unique_ptr<arrow::flight::FlightMessageReader> /*reader*/,
    std::unique_ptr<arrow::flight::FlightMessageWriter> /*writer*/)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::DoExchange");
    return arrow::Status::NotImplemented("DoExchange is not implemented");
}

arrow::Status ArrowFlightHandler::DoAction(
    const arrow::flight::ServerCallContext & /*context*/,
    const arrow::flight::Action & /*action*/,
    std::unique_ptr<arrow::flight::ResultStream> * /*result*/)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::DoAction");
    return arrow::Status::OK();
}

arrow::Status
ArrowFlightHandler::ListActions(const arrow::flight::ServerCallContext & /*context*/, std::vector<arrow::flight::ActionType> * /*actions*/)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::ListActions");
    return arrow::Status::OK();
}

}

#endif
