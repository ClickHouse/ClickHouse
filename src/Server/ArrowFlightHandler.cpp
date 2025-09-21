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

#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{
    const std::string AUTHORIZATION_HEADER = "authorization";
    const std::string AUTHORIZATION_MIDDLEWARE_NAME = "authorization_middleware";

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
                return arrow::Status::IOError("Missing Authorization header");

            auto auth_header = std::string(it->second);

            std::string token;

            const std::string prefix_basic = "Basic ";
            if (auth_header.starts_with(prefix_basic))
                token = auth_header.substr(prefix_basic.size());

            const std::string prefix_bearer = "Bearer ";
            if (auth_header.starts_with(prefix_bearer))
                token = auth_header.substr(prefix_bearer.size());

            if (token.empty())
                return arrow::Status::IOError("Expected Basic auth scheme");

            std::string credentials = base64Decode(token, true);
            auto pos = credentials.find(':');
            if (pos == std::string::npos)
                return arrow::Status::IOError("Malformed credentials");

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

    /// Extracts the client's address from the call context.
    Poco::Net::SocketAddress getClientAddress(const arrow::flight::ServerCallContext & context)
    {
        /// Returns a string like ipv4:127.0.0.1:55930 or ipv6:%5B::1%5D:55930
        String uri_encoded_peer = context.peer();

        constexpr const std::string_view ipv4_prefix = "ipv4:";
        constexpr const std::string_view ipv6_prefix = "ipv6:";

        bool ipv4 = uri_encoded_peer.starts_with(ipv4_prefix);
        bool ipv6 = uri_encoded_peer.starts_with(ipv6_prefix);

        if (!ipv4 && !ipv6)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ipv4 or ipv6 protocol in peer address, got {}", uri_encoded_peer);

        auto prefix = ipv4 ? ipv4_prefix : ipv6_prefix;
        auto family = ipv4 ? Poco::Net::AddressFamily::Family::IPv4 : Poco::Net::AddressFamily::Family::IPv6;

        uri_encoded_peer= uri_encoded_peer.substr(prefix.length());

        String peer;
        Poco::URI::decode(uri_encoded_peer, peer);

        return Poco::Net::SocketAddress{family, peer};
    }

    /// Extracts an SQL queryt from a flight descriptor.
    [[nodiscard]] arrow::Result<String> convertDescriptorToSQL(const arrow::flight::FlightDescriptor & descriptor, bool for_put_operation)
    {
        switch (descriptor.type)
        {
            case arrow::flight::FlightDescriptor::PATH:
            {
                const auto & path = descriptor.path;
                if (path.size() != 1)
                    return arrow::Status::Invalid("Flight descriptor's path should be one-component (got ", path.size(), " components)");
                if (path[0].empty())
                    return arrow::Status::Invalid("Flight descriptor's path should specify the name of a table");
                const String & table_name = path[0];
                if (for_put_operation)
                    return "INSERT INTO " + backQuoteIfNeed(table_name) + " FORMAT Arrow";
                else
                    return "SELECT * FROM " + backQuoteIfNeed(table_name);
            }
            case arrow::flight::FlightDescriptor::CMD:
            {
                const auto & cmd = descriptor.cmd;
                if (cmd.empty())
                    return arrow::Status::Invalid("Flight descriptor's command should specify a SQL query");
                return cmd;
            }
            default:
                return arrow::Status::TypeError("Flight descriptor has unknown type ", toString(descriptor.type));
        }
    }

    [[nodiscard]] arrow::Result<String> convertGetDescriptorToSQL(const arrow::flight::FlightDescriptor & descriptor)
    {
        return convertDescriptorToSQL(descriptor, /* for_put_operation = */ false);
    }

    [[nodiscard]] arrow::Result<String> convertPutDescriptorToSQL(const arrow::flight::FlightDescriptor & descriptor)
    {
        return convertDescriptorToSQL(descriptor, /* for_put_operation = */ true);
    }

    /// For method doGet() the pipeline should have an output.
    [[nodiscard]] arrow::Status checkPipelineIsPulling(const QueryPipeline & pipeline)
    {
        if (!pipeline.pulling())
            return arrow::Status::Invalid("Query doesn't allow pulling data, use method doPut() with this kind of query");
        return arrow::Status::OK();
    }

    /// We don't allow custom formats except "Arrow" because they can't work with ArrowFlight.
    [[nodiscard]] arrow::Status checkNoCustomFormat(ASTPtr ast)
    {
        if (const auto * ast_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (ast_with_output->format_ast)
                return arrow::Status::ExecutionError("FORMAT clause not supported by Arrow Flight");
        }
        else if (const auto * insert = dynamic_cast<const ASTInsertQuery *>(ast.get()))
        {
            if (!insert->format.empty() && insert->format != "Arrow")
                return arrow::Status::Invalid("DoPut failed: invalid format value, only 'Arrow' custom format supported");
        }
        return arrow::Status::OK();
    }

    using Timestamp = std::chrono::system_clock::time_point;
    using Duration = std::chrono::system_clock::duration;

    Timestamp now()
    {
        return std::chrono::system_clock::now();
    }

    /// We generate tickets with this prefix.
    /// Method DoGet() accepts a ticket which is either 1) a ticket with this prefix; or 2) a SQL query.
    /// A valid SQL query can't start with this prefix so method DoGet() can distinguish those cases.
    const String TICKET_PREFIX = "--#TICKET-";

    bool hasTicketPrefix(const String & ticket)
    {
        return ticket.starts_with(TICKET_PREFIX);
    }

    /// A ticket name with its expiration time.
    struct TicketWithExpirationTime
    {
        String ticket;
        /// When the ticket expires.
        /// std::nullopt means that the ticket expires after using it in DoGet().
        std::optional<Timestamp> expiration_time;
    };

    /// Keeps a block associated with a ticket.
    struct TicketInfo : public TicketWithExpirationTime
    {
        ConstBlockPtr block;
        std::shared_ptr<const CHColumnToArrowColumn> ch_to_arrow_converter;
    };

    /// Creates a converter from ClickHouse blocks to the Arrow format.
    std::shared_ptr<CHColumnToArrowColumn> createCHToArrowConverter(const Block & header)
    {
        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;
        auto ch_to_arrow_converter = std::make_shared<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
        ch_to_arrow_converter->initializeArrowSchema();
        return ch_to_arrow_converter;
    }
}


/// Keeps information about calls - e.g. blocks extracted from pipelines and tickets.
class ArrowFlightHandler::CallsData
{
public:
    CallsData(std::optional<Duration> tickets_lifetime_)
        : tickets_lifetime(tickets_lifetime_)
    {
    }

    ~CallsData() = default;

    /// Creates a ticket for a specified block.
    std::shared_ptr<const TicketInfo> createTicket(ConstBlockPtr block, std::shared_ptr<const CHColumnToArrowColumn> ch_to_arrow_converter)
    {
        String ticket = generateTicketName();
        Timestamp current_time = now();
        auto expiration_time = calculateTicketExpirationTime(current_time);
        auto info = std::make_shared<TicketInfo>();
        info->ticket = ticket;
        info->expiration_time = expiration_time;
        info->block = block;
        info->ch_to_arrow_converter = ch_to_arrow_converter;
        std::lock_guard lock{mutex};
        tickets[ticket] = info;
        if (expiration_time)
        {
            tickets_by_expiration_time.emplace(*expiration_time, ticket);
            updateNextExpirationTime();
        }
        return info;
    }

    /// Returns the information about a ticket.
    [[nodiscard]] arrow::Result<std::shared_ptr<const TicketInfo>> getTicketInfo(const String & ticket) const
    {
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        return it->second;
    }

    /// Extends the expiration time of a ticket.
    [[nodiscard]] arrow::Status extendTicketExpirationTime(const String & ticket)
    {
        if (!tickets_lifetime)
            return arrow::Status::OK();
        Timestamp current_time = now();
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        auto info = it->second;
        auto old_expiration_time = info->expiration_time;
        auto new_expiration_time = calculateTicketExpirationTime(current_time);
        auto new_info = std::make_shared<TicketInfo>(*info);
        new_info->expiration_time = new_expiration_time;
        it->second = new_info;
        tickets_by_expiration_time.erase(std::make_pair(*old_expiration_time, ticket));
        tickets_by_expiration_time.emplace(*new_expiration_time, ticket);
        updateNextExpirationTime();
        return arrow::Status::OK();
    }

    /// Cancels a ticket to free memory.
    void cancelTicket(const String & ticket)
    {
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it != tickets.end())
        {
            auto info = it->second;
            tickets.erase(it);
            if (info->expiration_time)
            {
                tickets_by_expiration_time.erase(std::make_pair(*info->expiration_time, ticket));
                updateNextExpirationTime();
            }
        }
    }

    /// Cancels tickets if `current_time` is greater than their expiration time.
    void cancelExpired()
    {
        auto current_time = now();
        std::lock_guard lock{mutex};
        while (!tickets_by_expiration_time.empty())
        {
            auto it = tickets_by_expiration_time.begin();
            if (current_time <= it->first)
                break;
            tickets.erase(it->second);
            tickets_by_expiration_time.erase(it);
        }
        updateNextExpirationTime();
    }

    /// Waits until it's time to cancel expired tickets.
    void waitNextExpirationTime() const
    {
        auto current_time = now();
        std::unique_lock lock{mutex};
        auto is_ready = [&]
        {
            if (stop_waiting_next_expiration_time)
                return true;
            current_time = now();
            return next_expiration_time && (current_time > *next_expiration_time);
        };
        if (next_expiration_time)
        {
            if (current_time < *next_expiration_time)
                next_expiration_time_decreased.wait_for(lock, *next_expiration_time - current_time, is_ready);
        }
        else
        {
            next_expiration_time_decreased.wait(lock, is_ready);
        }
    }

    void stopWaitingNextExpirationTime()
    {
        std::lock_guard lock{mutex};
        stop_waiting_next_expiration_time = true;
    }

private:
    static String generateTicketName()
    {
        return TICKET_PREFIX + toString(UUIDHelpers::generateV4());
    }

    std::optional<Timestamp> calculateTicketExpirationTime(Timestamp current_time) const
    {
        if (!tickets_lifetime)
            return std::nullopt;
        return current_time + *tickets_lifetime;
    }

    void updateNextExpirationTime() TSA_REQUIRES(mutex)
    {
        auto old_value = next_expiration_time;
        next_expiration_time.reset();
        if (!tickets_by_expiration_time.empty())
            next_expiration_time = tickets_by_expiration_time.begin()->first;
        if (next_expiration_time && (!old_value || next_expiration_time < *old_value))
            next_expiration_time_decreased.notify_all();
    }

    const std::optional<Duration> tickets_lifetime;
    mutable std::mutex mutex;
    std::unordered_map<String, std::shared_ptr<const TicketInfo>> tickets TSA_GUARDED_BY(mutex);
    std::condition_variable evaluation_ended;
    std::set<std::pair<Timestamp, String>> tickets_by_expiration_time TSA_GUARDED_BY(mutex);
    std::optional<Timestamp> next_expiration_time;
    mutable std::condition_variable next_expiration_time_decreased;
    bool stop_waiting_next_expiration_time = false;
};


ArrowFlightHandler::ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightHandler"))
    , address_to_listen(address_to_listen_)
    , tickets_lifetime_seconds(server.config().getUInt("arrowflight.tickets_lifetime_seconds", 600))
    , cancel_ticket_after_do_get(server.config().getBool("arrowflight.cancel_ticket_after_do_get", false))
    , calls_data(
          std::make_unique<CallsData>(
              tickets_lifetime_seconds ? std::make_optional(std::chrono::seconds{tickets_lifetime_seconds}) : std::optional<Duration>{}))
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

    if (tickets_lifetime_seconds)
    {
        cleanup_thread.emplace([this]
        {
            try
            {
                setThreadName("ArrowFlightExpr");
                while (!stopped)
                {
                    calls_data->waitNextExpirationTime();
                    calls_data->cancelExpired();
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to cleanup");
            }
        });
    }
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

        calls_data->stopWaitingNextExpirationTime();
        if (cleanup_thread)
        {
            cleanup_thread->join();
            cleanup_thread.reset();
        }
    }
}

UInt16 ArrowFlightHandler::portNumber() const
{
    return address_to_listen.port();
}

arrow::Status ArrowFlightHandler::GetFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * info)
{
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;
        std::shared_ptr<const CHColumnToArrowColumn> ch_to_arrow_converter;

        auto sql_res = convertGetDescriptorToSQL(request);
        ARROW_RETURN_NOT_OK(sql_res);
        const String & sql = sql_res.ValueOrDie();

        Session session{server.context(), ClientInfo::Interface::ARROW_FLIGHT};

        const auto & auth = AuthMiddleware::get(context);
        session.authenticate(auth.username(), auth.password(), getClientAddress(context));

        auto query_context = session.makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope{query_context};

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        PullingPipelineExecutor executor{block_io.pipeline};
        ch_to_arrow_converter = createCHToArrowConverter(executor.getHeader());

        Block block;
        while (executor.pull(block))
        {
            if (!block.empty())
            {
                total_rows += block.rows();
                total_bytes += block.bytes();
                auto ticket_info = calls_data->createTicket(std::make_shared<Block>(std::move(block)), ch_to_arrow_converter);
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket_info->ticket};
                endpoint.expiration_time = ticket_info->expiration_time;
                endpoints.emplace_back(endpoint);
            }
        }

        auto flight_info_res = arrow::flight::FlightInfo::Make(
            *ch_to_arrow_converter->getArrowSchema(),
            request,
            endpoints,
            total_rows,
            total_bytes,
            /* ordered = */ true);

        ARROW_RETURN_NOT_OK(flight_info_res);
        *info = std::make_unique<arrow::flight::FlightInfo>(std::move(flight_info_res).ValueOrDie());

        return arrow::Status::OK();
    }
    catch (const Exception & e)
    {
        return arrow::Status::ExecutionError("GetFlightInfo failed: ", e.displayText());
    }
}


arrow::Status ArrowFlightHandler::GetSchema(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * schema)
{
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        std::shared_ptr<const CHColumnToArrowColumn> ch_to_arrow_converter;

        auto sql_res = convertGetDescriptorToSQL(request);
        ARROW_RETURN_NOT_OK(sql_res);
        const String & sql = sql_res.ValueOrDie();

        Session session{server.context(), ClientInfo::Interface::ARROW_FLIGHT};

        const auto & auth = AuthMiddleware::get(context);
        session.authenticate(auth.username(), auth.password(), getClientAddress(context));

        auto query_context = session.makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope{query_context};

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        ch_to_arrow_converter = createCHToArrowConverter(block_io.pipeline.getHeader());

        auto schema_res = arrow::flight::SchemaResult::Make(*ch_to_arrow_converter->getArrowSchema());
        ARROW_RETURN_NOT_OK(schema_res);
        *schema = std::make_unique<arrow::flight::SchemaResult>(*std::move(schema_res).ValueOrDie());

        return arrow::Status::OK();
    }
    catch (const Exception & e)
    {
        return arrow::Status::ExecutionError("GetSchema failed: ", e.displayText());
    }
}


arrow::Status ArrowFlightHandler::PollFlightInfo(
    const arrow::flight::ServerCallContext & /*context*/,
    const arrow::flight::FlightDescriptor & /*request*/,
    std::unique_ptr<arrow::flight::PollInfo> * /*info*/)
{
    return arrow::Status::NotImplemented("NYI");
}


arrow::Status ArrowFlightHandler::DoGet(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Ticket & request,
    std::unique_ptr<arrow::flight::FlightDataStream> * stream)
{
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        Block header;
        std::vector<Chunk> chunks;
        std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
        bool should_cancel_ticket = false;
    
        if (hasTicketPrefix(request.ticket))
        {
            auto ticket_info_res = calls_data->getTicketInfo(request.ticket);
            ARROW_RETURN_NOT_OK(ticket_info_res);
            const auto & ticket_info = ticket_info_res.ValueOrDie();
            chunks.emplace_back(Chunk(ticket_info->block->getColumns(), ticket_info->block->rows()));
            header = ticket_info->block->cloneEmpty();
            ch_to_arrow_converter = ticket_info->ch_to_arrow_converter->clone(/* copy_arrow_schema = */ true);
            should_cancel_ticket = cancel_ticket_after_do_get;
        }
        else
        {
            const String & sql = request.ticket;

            Session session{server.context(), ClientInfo::Interface::ARROW_FLIGHT};

            const auto & auth = AuthMiddleware::get(context);
            session.authenticate(auth.username(), auth.password(), getClientAddress(context));

            auto query_context = session.makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            CurrentThread::QueryScope query_scope{query_context};

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
            ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
            ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

            PullingPipelineExecutor executor{block_io.pipeline};

            Block block;
            while (executor.pull(block))
                chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

            header = executor.getHeader();
            ch_to_arrow_converter = createCHToArrowConverter(header);
        }

        std::shared_ptr<arrow::Table> arrow_table;
        ch_to_arrow_converter->chChunkToArrowTable(arrow_table, chunks, header.columns());

        auto stream_res = arrow::RecordBatchReader::MakeFromIterator(
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>{arrow::TableBatchReader{arrow_table}},
            ch_to_arrow_converter->getArrowSchema());
        ARROW_RETURN_NOT_OK(stream_res);
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(stream_res.ValueOrDie());

        if (should_cancel_ticket)
            calls_data->cancelTicket(request.ticket);

        return arrow::Status::OK();
    }
    catch (const Exception & e)
    {
        return arrow::Status::ExecutionError("DoGet failed: ", e.displayText());
    }
}


arrow::Status ArrowFlightHandler::DoPut(
    const arrow::flight::ServerCallContext & context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> /*writer*/)
{
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        const auto & descriptor = reader->descriptor();
        auto sql_res = convertPutDescriptorToSQL(descriptor);
        ARROW_RETURN_NOT_OK(sql_res);
        const String & sql = sql_res.ValueOrDie();

        Session session{server.context(), ClientInfo::Interface::ARROW_FLIGHT};

        const auto & auth = AuthMiddleware::get(context);
        session.authenticate(auth.username(), auth.password(), getClientAddress(context));

        auto query_context = session.makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope{query_context};

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        auto & pipeline = block_io.pipeline;

        if (pipeline.pushing())
        {
            Block header = pipeline.getHeader();
            auto input = std::make_shared<ArrowFlightSource>(std::move(reader), header);
            pipeline.complete(Pipe(std::move(input)));
        }
        else if (pipeline.pulling())
        {
            Block header = pipeline.getHeader();
            auto output = std::make_shared<NullSink>(std::make_shared<Block>(header));
            pipeline.complete(Pipe(std::move(output)));
        }

        CompletedPipelineExecutor executor(pipeline);
        executor.execute();

        return arrow::Status::OK();
    }
    catch (const Exception & e)
    {
        return arrow::Status::ExecutionError("DoPut failed: ", e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoAction(
    const arrow::flight::ServerCallContext & /*context*/,
    const arrow::flight::Action & /*action*/,
    std::unique_ptr<arrow::flight::ResultStream> * /*result*/)
{
    return arrow::Status::NotImplemented("NYI");
}

}

#endif
