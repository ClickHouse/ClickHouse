#include <Server/ArrowFlightHandler.h>

#if USE_ARROWFLIGHT

#include <functional>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Base64.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Server/IServer.h>
#include <base/EnumReflection.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/ProcessList.h>

#include <Common/config_version.h>
#include <Common/scope_guard_safe.h>
#include <Common/StdHelpers.h>

#include <boost/algorithm/string.hpp>

#include <Server/arrowFlightProto.h>

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_union.h>
#include <arrow/table.h>
#include <arrow/scalar.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/flight/sql/protocol_internal.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INVALID_SESSION_TIMEOUT;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int UNKNOWN_SETTING;
    extern const int SYNTAX_ERROR;
}

namespace
{
    const std::string AUTHORIZATION_HEADER = "authorization";
    const std::string AUTHORIZATION_MIDDLEWARE_NAME = "authorization_middleware";

    class AuthMiddleware : public arrow::flight::ServerMiddleware
    {
    public:
        explicit AuthMiddleware(const arrow::flight::ServerCallContext & context, const std::string & token, const std::string & username, const std::string & password,
                                const std::string & session_id = "", bool session_check = false, unsigned session_timeout = 0, bool session_close = false)
            : context_(context)
            , token_(token)
            , username_(username)
            , password_(password)
            , session_id_(session_id)
            , session_check_(session_check)
            , session_timeout_(session_timeout)
            , session_close_(session_close)
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

        const arrow::flight::ServerCallContext & context() const { return context_; }
        const std::string & sessionId() const { return session_id_; }
        bool sessionCheck() const { return session_check_; }
        unsigned sessionTimeout() const { return session_timeout_; }
        bool sessionClose() const { return session_close_; }

    private:
        const arrow::flight::ServerCallContext & context_;
        const std::string token_;
        const std::string username_;
        const std::string password_;
        const std::string session_id_;
        const bool session_check_;
        const unsigned session_timeout_;
        const bool session_close_;
    };

    std::chrono::steady_clock::duration parseSessionTimeout(
        const Poco::Util::AbstractConfiguration & config,
        unsigned query_session_timeout)
    {
        unsigned session_timeout = config.getInt("default_session_timeout", 60);

        if (query_session_timeout)
        {
            session_timeout = query_session_timeout;
            unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);

            if (session_timeout > max_session_timeout)
                throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Session timeout '{}' is larger than max_session_timeout: {}. "
                    "Maximum session timeout could be modified in configuration file.",
                    session_timeout, max_session_timeout);
        }

        return std::chrono::seconds(session_timeout);
    }

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

            std::string user = "default";
            std::string password;

            std::string credentials = base64Decode(token, true);
            auto pos = credentials.find(':');
            if (pos != std::string::npos)
            {
                user = credentials.substr(0, pos);
                password = credentials.substr(pos + 1);
            }

            std::string session_id;
            auto session_it = headers.find("x-clickhouse-session-id");
            if (session_it != headers.end())
                session_id = std::string(session_it->second);

            std::string session_check;
            session_it = headers.find("x-clickhouse-session-check");
            if (session_it != headers.end())
                session_check = std::string(session_it->second);

            std::string session_timeout_str;
            session_it = headers.find("x-clickhouse-session-timeout");
            if (session_it != headers.end())
                session_timeout_str = std::string(session_it->second);

            unsigned session_timeout = 0;
            if (!session_timeout_str.empty())
            {
                ReadBufferFromString buf(session_timeout_str);
                if (!tryReadIntText(session_timeout, buf) || !buf.eof())
                    return arrow::Status::IOError("Invalid session timeout: " + session_timeout_str);
            }

            std::string session_close;
            session_it = headers.find("x-clickhouse-session-close");
            if (session_it != headers.end())
                session_close = std::string(session_it->second);

            *middleware = std::make_unique<AuthMiddleware>(context, token, user, password, session_id, session_check == "1", session_timeout, session_close == "1");
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

        /// Function arrow::flight::Location::ForGrpc*() builds an URL so it requires IPv6 address to be enclosed in brackets
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

        uri_encoded_peer = uri_encoded_peer.substr(prefix.length());

        String peer;
        Poco::URI::decode(uri_encoded_peer, peer);

        return Poco::Net::SocketAddress{family, peer};
    }

    std::shared_ptr<Session> authenticate(const AuthMiddleware & auth, const ContextPtr & context)
    {
        auto session = std::make_shared<Session>(context, ClientInfo::Interface::ARROW_FLIGHT);
        session->authenticate(auth.username(), auth.password(), getClientAddress(auth.context()));

        /// The user could specify session identifier and session timeout.
        /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
        if (auth.sessionId().empty())
            session->makeSessionContext();
        else
            session->makeSessionContext(auth.sessionId(), parseSessionTimeout(context->getConfigRef(), auth.sessionTimeout()), auth.sessionCheck());

        return session;
    }

    void releaseOrCloseSession(std::shared_ptr<Session> session, const String & session_id, bool close_session)
    {
        if (!session_id.empty())
        {
            if (close_session)
                session->closeSession(session_id);
            else
                session->releaseSessionID();
        }
    }

    /// Extracts an SQL query from a flight descriptor.
    /// It depends on the flight descriptor's type (PATH/CMD) and on the operation's type (DoPut/DoGet).
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
                return arrow::Status::TypeError("Flight descriptor has unknown type ", magic_enum::enum_name(descriptor.type));
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
            if (ast_with_output->format_ast && (getIdentifierName(ast_with_output->format_ast) != "Arrow"))
                return arrow::Status::ExecutionError("Invalid format, only 'Arrow' format is supported");
        }
        else if (const auto * insert = dynamic_cast<const ASTInsertQuery *>(ast.get()))
        {
            if (!insert->format.empty() && insert->format != "Values" && insert->format != "Arrow")
                return arrow::Status::ExecutionError("Invalid format (", insert->format, "), only 'Arrow' format is supported");
        }
        return arrow::Status::OK();
    }

    using Timestamp = std::chrono::system_clock::time_point;
    using Duration = std::chrono::system_clock::duration;

    Timestamp now()
    {
        return std::chrono::system_clock::now();
    }

    /// We use the ALREADY_EXPIRED timestamp (January 1, 1970) as the expiration time of a ticket or a poll descriptor
    /// which is already expired.
    const Timestamp ALREADY_EXPIRED = Timestamp{Duration{0}};

    /// We generate tickets with this prefix.
    /// Method DoGet() accepts a ticket which is either 1) a ticket with this prefix; or 2) a SQL query.
    /// A valid SQL query can't start with this prefix so method DoGet() can distinguish those cases.
    const String TICKET_PREFIX = "~TICKET-";

    bool hasTicketPrefix(const String & ticket)
    {
        return ticket.starts_with(TICKET_PREFIX);
    }

    /// We generate poll descriptors with this prefix.
    /// Methods PollFlightInfo() or GetSchema() accept a flight descriptor which is either
    /// 1) a normal flight descriptor (a table name or a SQL query); or 2) a poll descriptor with this prefix.
    /// A valid SQL query can't start with this prefix so methods PollFlightInfo() and GetSchema() can distinguish those cases.
    const String POLL_DESCRIPTOR_PREFIX = "~POLL-";

    bool hasPollDescriptorPrefix(const String & poll_descriptor)
    {
        return poll_descriptor.starts_with(POLL_DESCRIPTOR_PREFIX);
    }

    /// A ticket name with its expiration time.
    struct TicketWithExpirationTime
    {
        String ticket;
        /// When the ticket expires.
        /// std::nullopt means that the ticket expires after using it in DoGet().
        /// Can be equal to ALREADY_EXPIRED.
        std::optional<Timestamp> expiration_time;
    };

    /// A poll descriptor's name with its expiration time.
    struct PollDescriptorWithExpirationTime
    {
        String poll_descriptor;
        /// When the poll descriptor expires.
        /// std::nullopt means that the poll descriptor expires after using it in PollFlightInfo();
        /// Can be equal to ALREADY_EXPIRED.
        std::optional<Timestamp> expiration_time;
    };

    struct TicketInfo : public TicketWithExpirationTime
    {
        std::shared_ptr<arrow::Table> arrow_table;
    };

    /// Information about a poll descriptor.
    /// Objects of type PollDescriptorInfo are stored as a kind of a doubly linked list,
    /// the previous object is stored as `previous_info`, and the next object is referenced by `next_poll_descriptor`.
    struct PollDescriptorInfo : public PollDescriptorWithExpirationTime
    {
        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<const PollDescriptorInfo> previous_info;
        bool evaluating = false;
        bool evaluated = false;

        /// The following fields can be set only if `evaluated == true`:

        /// A success or error error.
        std::optional<arrow::Status> status;

        /// A new ticket. Along with tickets from previous infos (previous_info, previous_info->previous_info, etc.)
        /// represents all tickets associated with this poll descriptor.
        /// Can be unset if there is no block; or it can specify an already expired ticket.
        std::optional<String> ticket;

        /// Adds rows. Along with added rows from previous infos (previous_info, previous_info->previous_info, etc.)
        /// represents the total number of rows associated with this poll descriptor.
        /// Can be unset if there is no rows added.
        std::optional<size_t> rows;

        /// Adds bytes. Along with added bytes from previous infos (previous_info, previous_info->previous_info, etc.)
        /// represents the total number of bytes associated with this poll descriptor.
        /// Can be unset if there is no bytes added.
        std::optional<size_t> bytes;

        /// Next poll descriptor if any.
        /// Can be unset if there is no next poll descriptor (no more blocks are to pull from the query pipeline).
        std::optional<String> next_poll_descriptor;
    };

    /// Keeps a query context and a pipeline executor for PollFlightInfo.
    class PollSession
    {
    public:
        PollSession(
            ContextPtr query_context_,
            ThreadGroupPtr thread_group_,
            BlockIO && block_io_,
            std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
            std::function<void(Block &)> block_modifier_ = nullptr)
            : query_context(query_context_)
            , thread_group(thread_group_)
            , block_io(std::move(block_io_))
            , executor(block_io.pipeline)
            , schema(
                CHColumnToArrowColumn::calculateArrowSchema(
                    executor.getHeader().getColumnsWithTypeAndName(),
                    "Arrow",
                    nullptr,
                    {.output_string_as_string = true}
                )
            )
            , block_modifier(block_modifier_)
        {
            if (schema_modifier)
            {
                auto result = schema_modifier(schema);
                if (!result.ok())
                    throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to convert Arrow schema {}", schema->ToString());
                schema = result.ValueUnsafe();
            }
        }

        ~PollSession() = default;

        ThreadGroupPtr getThreadGroup() const { return thread_group; }
        std::shared_ptr<arrow::Schema> getSchema() const { return schema; }
        bool getNextBlock(Block & block)
        {
            if (!executor.pull(block))
                return false;
            if (block_modifier)
                block_modifier(block);
            return true;
        }
        void onFinish() { block_io.onFinish(); }
        void onException() { block_io.onException(); }

    private:
        ContextPtr query_context;
        ThreadGroupPtr thread_group;
        BlockIO block_io;
        PullingPipelineExecutor executor;
        std::shared_ptr<arrow::Schema> schema;
        std::function<void(Block &)> block_modifier;
    };

    /// Creates a converter to convert ClickHouse blocks to the Arrow format.
    std::shared_ptr<CHColumnToArrowColumn> createCHToArrowConverter(const Block & header)
    {
        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;
        auto ch_to_arrow_converter = std::make_shared<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
        ch_to_arrow_converter->initializeArrowSchema();
        return ch_to_arrow_converter;
    }
}


/// Keeps information about calls - e.g. blocks extracted from query pipelines, flight tickets, poll descriptors.
class ArrowFlightHandler::CallsData
{
public:
    CallsData(std::optional<Duration> tickets_lifetime_, std::optional<Duration> poll_descriptors_lifetime_, LoggerPtr log_)
        : tickets_lifetime(tickets_lifetime_)
        , poll_descriptors_lifetime(poll_descriptors_lifetime_)
        , log(log_)
    {
    }

    /// Creates a flight ticket which allows to download a specified block.
    std::shared_ptr<const TicketInfo> createTicket(std::shared_ptr<arrow::Table> arrow_table)
    {
        String ticket = generateTicketName();
        LOG_DEBUG(log, "Creating ticket {}", ticket);
        auto expiration_time = calculateTicketExpirationTime(now());
        auto info = std::make_shared<TicketInfo>();
        info->ticket = ticket;
        info->expiration_time = expiration_time;
        info->arrow_table = arrow_table;
        std::lock_guard lock{mutex};
        bool inserted = tickets.try_emplace(ticket, info).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted); /// Flight tickets are unique.
        if (expiration_time)
        {
            inserted = tickets_by_expiration_time.emplace(*expiration_time, ticket).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
            chassert(inserted); /// Flight tickets are unique.
            updateNextExpirationTime();
        }
        return info;
    }

    [[nodiscard]] arrow::Result<std::shared_ptr<const TicketInfo>> getTicketInfo(const String & ticket) const
    {
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        return it->second;
    }

    /// Finds the expiration time for a specified ticket.
    /// If the ticket is not found it means it was expired and removed from the map.
    std::optional<Timestamp> getTicketExpirationTime(const String & ticket) const
    {
        if (!tickets_lifetime)
            return std::nullopt;
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return ALREADY_EXPIRED;
        return it->second->expiration_time;
    }

    /// Extends the expiration time of a ticket.
    /// The function calculates a new expiration time of a ticket based on the current time.
    [[nodiscard]] arrow::Status extendTicketExpirationTime(const String & ticket)
    {
        if (!tickets_lifetime)
            return arrow::Status::OK();
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        auto info = it->second;
        auto old_expiration_time = info->expiration_time;
        auto new_expiration_time = calculateTicketExpirationTime(now());
        auto new_info = std::make_shared<TicketInfo>(*info);
        new_info->expiration_time = new_expiration_time;
        it->second = new_info;
        tickets_by_expiration_time.erase(std::make_pair(*old_expiration_time, ticket));
        tickets_by_expiration_time.emplace(*new_expiration_time, ticket);
        updateNextExpirationTime();
        return arrow::Status::OK();
    }

    /// Cancels a ticket to free memory.
    /// Tickets are cancelled either by timer (if setting "arrowflight.tickets_lifetime_seconds" > 0)
    /// or after they are used by method DoGet (if setting "arrowflight.cancel_flight_descriptor_after_poll_flight_info" is set to true).
    void cancelTicket(const String & ticket)
    {
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return; /// The ticked has been already cancelled.
        LOG_DEBUG(log, "Cancelling ticket {}", ticket);
        auto info = it->second;
        tickets.erase(it);
        if (info->expiration_time)
        {
            tickets_by_expiration_time.erase(std::make_pair(*info->expiration_time, ticket));
            updateNextExpirationTime();
        }
    }

    void setFlightDescriptorMapLocked(const String & flight_descriptor, const String & query_id) TSA_REQUIRES(mutex)
    {
        flight_descriptor_to_query_id[flight_descriptor] = query_id;
        query_id_to_flight_descriptors[query_id].insert(flight_descriptor);
    }

    std::optional<String> getQueryIdFromFlightDescriptor(const String & flight_descriptor)
    {
        std::lock_guard lock{mutex};
        if (flight_descriptor_to_query_id.contains(flight_descriptor))
            return flight_descriptor_to_query_id[flight_descriptor];
        return std::nullopt;
    }

    void eraseFlightDescriptorMapByQueryIdLocked(const String & query_id) TSA_REQUIRES(mutex)
    {
        auto it = query_id_to_flight_descriptors.find(query_id);
        if (it == query_id_to_flight_descriptors.end())
            return;
        for (const auto & flight_descriptor : it->second)
            flight_descriptor_to_query_id.erase(flight_descriptor);
        query_id_to_flight_descriptors.erase(it);
    }

    void eraseFlightDescriptorMapByQueryId(const String & query_id)
    {
        std::lock_guard lock{mutex};
        eraseFlightDescriptorMapByQueryIdLocked(query_id);
    }

    void eraseFlightDescriptorMapByDescriptorLocked(const String & flight_descriptor) TSA_REQUIRES(mutex)
    {
        if (!flight_descriptor_to_query_id.contains(flight_descriptor))
            return;
        eraseFlightDescriptorMapByQueryIdLocked(flight_descriptor_to_query_id[flight_descriptor]);
    }

    void eraseFlightDescriptorMapByDescriptor(const String & flight_descriptor)
    {
        std::lock_guard lock{mutex};
        eraseFlightDescriptorMapByDescriptorLocked(flight_descriptor);
    }

    /// Creates a poll descriptor.
    /// Poll descriptors are returned by method PollFlightInfo to get subsequent results from a long-running query.
    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptor(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info, std::optional<String> query_id = std::nullopt)
    {
        String poll_descriptor;
        if (previous_info)
        {
            if (!previous_info->evaluated)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is not evaluated");
            if (!previous_info->next_poll_descriptor)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is final");
            poll_descriptor = *previous_info->next_poll_descriptor;
            query_id =  getQueryIdFromFlightDescriptor(previous_info->poll_descriptor);
        }
        else
        {
            poll_descriptor = generatePollDescriptorName();
        }
        LOG_DEBUG(log, "Creating poll descriptor {}", poll_descriptor);
        auto current_time = now();
        auto expiration_time = calculatePollDescriptorExpirationTime(current_time);
        auto info = std::make_shared<PollDescriptorInfo>();
        info->poll_descriptor = poll_descriptor;
        info->expiration_time = expiration_time;
        info->schema = poll_session->getSchema();
        info->previous_info = previous_info;
        std::lock_guard lock{mutex};
        bool inserted = poll_descriptors.try_emplace(poll_descriptor, info).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted); /// Poll descriptors are unique.
        inserted = poll_sessions.try_emplace(poll_descriptor, std::move(poll_session)).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted); /// Poll descriptors are unique.
        if (expiration_time)
        {
            inserted = poll_descriptors_by_expiration_time.emplace(*expiration_time, poll_descriptor).second;  /// NOLINT(clang-analyzer-deadcode.DeadStores)
            chassert(inserted); /// Poll descriptors are unique.
            updateNextExpirationTime();
        }
        if (query_id)
            setFlightDescriptorMapLocked(poll_descriptor, *query_id);
        return info;
    }

    [[nodiscard]] arrow::Result<std::shared_ptr<const PollDescriptorInfo>> getPollDescriptorInfo(const String & poll_descriptor) const
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
            return arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
        return it->second;
    }

    /// Finds query id for a specified flight descriptor.
    std::optional<String> getQueryIdByFlightDescriptor(const String & flight_descriptor) const
    {
        std::lock_guard lock{mutex};
        auto it = flight_descriptor_to_query_id.find(flight_descriptor);
        if (it == flight_descriptor_to_query_id.end())
            return std::nullopt;
        return it->second;
    }

    /// Finds the expiration time for a specified poll descriptor.
    /// If the poll descriptor is not found it means it was expired and removed from the map.
    PollDescriptorWithExpirationTime getPollDescriptorWithExpirationTime(const String & poll_descriptor) const
    {
        if (!poll_descriptors_lifetime)
            return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = std::nullopt};
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
            return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = ALREADY_EXPIRED};
        return *it->second;
    }

    /// Extends the expiration time of a poll descriptor.
    /// The function calculates a new expiration time of a ticket based on the current time.
    [[nodiscard]] arrow::Status extendPollDescriptorExpirationTime(const String & poll_descriptor)
    {
        if (!poll_descriptors_lifetime)
            return arrow::Status::OK();
        auto current_time = now();
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
            return arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
        auto info = it->second;
        auto old_expiration_time = info->expiration_time;
        auto new_expiration_time = calculatePollDescriptorExpirationTime(current_time);
        auto new_info = std::make_shared<PollDescriptorInfo>(*info);
        new_info->expiration_time = new_expiration_time;
        it->second = new_info;
        poll_descriptors_by_expiration_time.erase(std::make_pair(*old_expiration_time, poll_descriptor));
        poll_descriptors_by_expiration_time.emplace(*new_expiration_time, poll_descriptor);
        updateNextExpirationTime();
        return arrow::Status::OK();
    }

    /// Starts evaluation (i.e. getting a block of data) for a specified poll descriptor.
    /// The function returns nullptr if it's already evaluated.
    /// If it's being evaluated at the moment in another thread the function waits until it finishes and then returns nullptr.
    [[nodiscard]] arrow::Result<std::unique_ptr<PollSession>> startEvaluation(const String & poll_descriptor)
    {
        arrow::Result<std::unique_ptr<PollSession>> res;
        std::unique_lock lock{mutex};
        evaluation_ended.wait(lock, [&]() TSA_REQUIRES(mutex)
        {
            auto it = poll_descriptors.find(poll_descriptor);
            if (it == poll_descriptors.end())
            {
                res = arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
                return true;
            }
            auto info = it->second;
            if (info->evaluated)
            {
                res = std::unique_ptr<PollSession>{nullptr};
                return true;
            }
            if (!info->evaluating)
            {
                auto it2 = poll_sessions.find(poll_descriptor);
                if (it2 == poll_sessions.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Session is not attached to non-evaluated poll descriptor {}", poll_descriptor);
                res = std::move(it2->second);
                poll_sessions.erase(it2);
                auto new_info = std::make_shared<PollDescriptorInfo>(*info);
                new_info->evaluating = true;
                it->second = new_info;
                return true;
            }
            return false; /// The poll descriptor is being evaluating in another thread, we need to wait.
        });
        return res;
    }

    /// Ends evaluation for a specified poll descriptor.
    void endEvaluation(const String & poll_descriptor, const std::optional<String> & ticket, UInt64 rows, UInt64 bytes, bool last)
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
        {
            /// The poll descriptor expired during the query execution.
            return;
        }

        auto info = it->second;
        if (info->evaluated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Poll descriptor can't be evaluated twice");

        auto new_info = std::make_shared<PollDescriptorInfo>(*info);
        new_info->evaluating = false;
        new_info->evaluated = true;
        new_info->status = arrow::Status::OK();
        new_info->ticket = ticket;
        new_info->rows = rows;
        new_info->bytes = bytes;
        if (!last)
            new_info->next_poll_descriptor = generatePollDescriptorName();
        it->second = new_info;
        info = new_info;
        evaluation_ended.notify_all();
    }

    /// Ends evaluation for a specified poll descriptor with an error.
    void endEvaluationWithError(const String & poll_descriptor, const arrow::Status & error_status)
    {
        chassert(!error_status.ok());
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it != poll_descriptors.end())
        {
            auto info = it->second;
            if (!info->evaluated)
            {
                auto new_info = std::make_shared<PollDescriptorInfo>(*info);
                new_info->evaluating = false;
                new_info->evaluated = true;
                new_info->status = error_status;
                it->second = new_info;
                info = new_info;
                evaluation_ended.notify_all();
            }
        }
    }

    /// Cancels a poll descriptor to free memory.
    /// Poll descriptors are cancelled either by timer (if setting "arrowflight.poll_descriptors_lifetime_seconds" > 0)
    /// or after they are used by method PollFlightInfo (if setting "arrowflight.cancel_ticket_after_do_get" is set to true).
    void cancelPollDescriptor(const String & poll_descriptor)
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it != poll_descriptors.end())
        {
            LOG_DEBUG(log, "Cancelling poll descriptor {}", poll_descriptor);
            auto info = it->second;
            poll_descriptors.erase(it);
            if (info->expiration_time)
            {
                poll_descriptors_by_expiration_time.erase(std::make_pair(*info->expiration_time, poll_descriptor));
                updateNextExpirationTime();
            }
        }
        auto it2 = poll_sessions.find(poll_descriptor);
        if (it2 != poll_sessions.end())
            poll_sessions.erase(it2);
    }

    /// Cancels tickets and poll descriptors if the current time is greater than their expiration time.
    void cancelExpired()
    {
        auto current_time = now();
        std::lock_guard lock{mutex};
        while (!tickets_by_expiration_time.empty())
        {
            auto it = tickets_by_expiration_time.begin();
            if (current_time <= it->first)
                break;
            LOG_DEBUG(log, "Cancelling expired ticket {}", it->second);
            tickets.erase(it->second);
            tickets_by_expiration_time.erase(it);
        }
        while (!poll_descriptors_by_expiration_time.empty())
        {
            auto it = poll_descriptors_by_expiration_time.begin();
            if (current_time <= it->first)
                break;
            LOG_DEBUG(log, "Cancelling expired poll descriptor {}", it->second);
            poll_descriptors.erase(it->second);
            poll_sessions.erase(it->second);
            poll_descriptors_by_expiration_time.erase(it);
        }
        updateNextExpirationTime();
    }

    /// Waits until maybe it's time to cancel expired tickets or poll descriptors.
    void waitNextExpirationTime() const
    {
        auto current_time = now();
        std::unique_lock lock{mutex};
        auto expiration_time = next_expiration_time;
        auto is_ready = [&]
        {
            if (stop_waiting_next_expiration_time)
                return true;
            if (next_expiration_time != expiration_time)
                return true; /// We need to restart waiting if the next expiration time has changed.
            current_time = now();
            return (expiration_time && (current_time > *expiration_time));
        };
        if (expiration_time)
        {
            if (current_time < *expiration_time)
                next_expiration_time_updated.wait_for(lock, *expiration_time - current_time, is_ready);
        }
        else
        {
            next_expiration_time_updated.wait(lock, is_ready);
        }
    }

    void stopWaitingNextExpirationTime()
    {
        std::lock_guard lock{mutex};
        stop_waiting_next_expiration_time = true;
        next_expiration_time_updated.notify_all();
    }

private:
    static String generateTicketName()
    {
        return TICKET_PREFIX + toString(UUIDHelpers::generateV4());
    }

    static String generatePollDescriptorName()
    {
        return POLL_DESCRIPTOR_PREFIX + toString(UUIDHelpers::generateV4());
    }

    std::optional<Timestamp> calculateTicketExpirationTime(Timestamp current_time) const
    {
        if (!tickets_lifetime)
            return std::nullopt;
        return current_time + *tickets_lifetime;
    }

    std::optional<Timestamp> calculatePollDescriptorExpirationTime(Timestamp current_time) const
    {
        if (!poll_descriptors_lifetime)
            return std::nullopt;
        return current_time + *poll_descriptors_lifetime;
    }

    void updateNextExpirationTime() TSA_REQUIRES(mutex)
    {
        auto expiration_time = next_expiration_time;
        next_expiration_time.reset();
        if (!tickets_by_expiration_time.empty())
            next_expiration_time = tickets_by_expiration_time.begin()->first;
        if (!poll_descriptors_by_expiration_time.empty())
        {
            auto other_expiration_time = poll_descriptors_by_expiration_time.begin()->first;
            next_expiration_time = next_expiration_time ? std::min(*next_expiration_time, other_expiration_time) : other_expiration_time;
        }
        if (next_expiration_time != expiration_time)
            next_expiration_time_updated.notify_all();
    }

    const std::optional<Duration> tickets_lifetime;
    const std::optional<Duration> poll_descriptors_lifetime;
    const LoggerPtr log;
    mutable std::mutex mutex;
    std::unordered_map<String, std::shared_ptr<const TicketInfo>> tickets TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::shared_ptr<const PollDescriptorInfo>> poll_descriptors TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::unique_ptr<PollSession>> poll_sessions TSA_GUARDED_BY(mutex);
    std::condition_variable evaluation_ended;
    /// associates flight descriptors with query id
    std::unordered_map<String, String> flight_descriptor_to_query_id TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::unordered_set<String>> query_id_to_flight_descriptors TSA_GUARDED_BY(mutex);
    /// `tickets_by_expiration_time` and `poll_descriptors_by_expiration_time` are sorted by `expiration_time` so `std::set` is used.
    std::set<std::pair<Timestamp, String>> tickets_by_expiration_time TSA_GUARDED_BY(mutex);
    std::set<std::pair<Timestamp, String>> poll_descriptors_by_expiration_time TSA_GUARDED_BY(mutex);
    std::optional<Timestamp> next_expiration_time;
    mutable std::condition_variable next_expiration_time_updated;
    bool stop_waiting_next_expiration_time = false;
};


ArrowFlightHandler::ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightHandler"))
    , address_to_listen(address_to_listen_)
    , tickets_lifetime_seconds(server.config().getUInt("arrowflight.tickets_lifetime_seconds", 600))
    , cancel_ticket_after_do_get(server.config().getBool("arrowflight.cancel_ticket_after_do_get", false))
    , poll_descriptors_lifetime_seconds(server.config().getUInt("arrowflight.poll_descriptors_lifetime_seconds", 600))
    , cancel_poll_descriptor_after_poll_flight_info(server.config().getBool("arrowflight.cancel_flight_descriptor_after_poll_flight_info", false))
    , calls_data(
          std::make_unique<CallsData>(
              tickets_lifetime_seconds ? std::make_optional(std::chrono::seconds{tickets_lifetime_seconds}) : std::optional<Duration>{},
              poll_descriptors_lifetime_seconds ? std::make_optional(std::chrono::seconds{poll_descriptors_lifetime_seconds})
                                                : std::optional<Duration>{},
              log))
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
            DB::setThreadName(ThreadName::ARROW_FLIGHT_SERVER);
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

    if (tickets_lifetime_seconds || poll_descriptors_lifetime_seconds)
    {
        cleanup_thread.emplace([this]
        {
            try
            {
                DB::setThreadName(ThreadName::ARROW_FLIGHT_EXPR);
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
            status = Wait();
            if (!status.ok())
                LOG_ERROR(log, "Failed to wait for shutdown Arrow Flight: {}", status.ToString());
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
        calls_data.reset();
    }
}

UInt16 ArrowFlightHandler::portNumber() const
{
    return address_to_listen.port();
}

static size_t calculateTableBytes(const std::shared_ptr<arrow::Table>& table)
{
    int64_t total_bytes = 0;
    for (const auto & chunked_array : table->columns())
        for (const auto & array : chunked_array->chunks())
            for (const auto& buffer : array->data()->buffers)
                if (buffer)
                    total_bytes += buffer->size();
    return total_bytes;
}

static ColumnsWithTypeAndName getHeader(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName res;
    for (const auto & column : columns)
        res.emplace_back(column.cloneEmpty());
    return res;
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables_impl(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    bool single_table,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(Block &)> block_modifier = nullptr
)
{
    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
    CurrentThread::QueryScope query_scope = CurrentThread::QueryScope::create(query_context);

    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Table>> tables;

    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
    try
    {
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        PullingPipelineExecutor executor{block_io.pipeline};
        schema = CHColumnToArrowColumn::calculateArrowSchema(executor.getHeader().getColumnsWithTypeAndName(), "Arrow", nullptr, {.output_string_as_string = true});
        if (schema_modifier)
        {
            auto status = schema_modifier(schema);
            ARROW_RETURN_NOT_OK(status);
            schema = status.ValueUnsafe();
        }

        std::optional<ColumnsWithTypeAndName> header;
        std::vector<Chunk> chunks;
        Block block;
        while (executor.pull(block))
        {
            if (!block.empty())
            {
                if (block_modifier)
                    block_modifier(block);
                if (!header)
                    header = getHeader(block.getColumnsWithTypeAndName());
                chunks.emplace_back(Chunk{block.getColumns(), block.rows()});
                if (!single_table)
                {
                    tables.emplace_back(CHColumnToArrowColumn::chunkToArrowTable(*header, "Arrow", chunks, {.output_string_as_string = true}, header->size(), schema));
                    chunks.clear();
                }
            }
        }
        if (single_table)
            tables.emplace_back(CHColumnToArrowColumn::chunkToArrowTable(*header, "Arrow", chunks, {.output_string_as_string = true}, header->size(), schema));

        block_io.onFinish();
    }
    catch (...)
    {
        block_io.onException();
        throw;
    }

    return std::tuple{schema, tables};
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(Block &)> block_modifier = nullptr
)
{
    return executeSQLtoTables_impl(session, sql, false, schema_modifier, block_modifier);
}

[[maybe_unused]] static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::Table>>> executeSQLtoTable(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(Block &)> block_modifier = nullptr
)
{
    auto res = executeSQLtoTables_impl(session, sql, true, schema_modifier, block_modifier);
    ARROW_RETURN_NOT_OK(res);
    return std::tuple{std::get<0>(res.ValueUnsafe()), std::get<1>(res.ValueUnsafe()).front()};
}

using CommandSelectorTuple = std::tuple<std::string, std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)>, std::function<void(Block &)>>;

/// commandSelector accepts arrow flight sql command in protobuf any-message and produces either resulting arrow::Table
/// (and if schema_only == true then table can be empty - only schema is requested) or set of sql query - which will be executed,
/// and, if resulting table requires modification, possible schema_modifier and block_modifier - they should consistently
/// manipulate schema and blocks to produce compatible results.
std::variant<
    CommandSelectorTuple,
    arrow::Result<std::shared_ptr<arrow::Table>>
>
commandSelector(const google::protobuf::Any & any_msg, bool schema_only = false)
{
    std::string sql;
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
    std::function<void(Block &)> block_modifier;

    if (any_msg.Is<arrow::flight::protocol::sql::CommandGetSqlInfo>())
    {
        arrow::flight::protocol::sql::CommandGetSqlInfo command;
        if (any_msg.UnpackTo(&command))
        {
            arrow::MemoryPool* pool = arrow::default_memory_pool();

            auto string_builder = std::make_shared<arrow::StringBuilder>();
            auto boolean_builder = std::make_shared<arrow::BooleanBuilder>();
            auto int64_builder = std::make_shared<arrow::Int64Builder>();
            auto int32_builder = std::make_shared<arrow::Int32Builder>();

            // string_list: list<item: string> not null
            auto string_list_type = arrow::list(arrow::utf8());
            auto string_list_builder = std::make_shared<arrow::ListBuilder>(pool, std::make_shared<arrow::StringBuilder>(), string_list_type);

            // int32_to_int32_list_map: map<int32, list<item: int32>> not null
            auto value_type = arrow::list(arrow::int32());
            auto value_builder = std::make_shared<arrow::ListBuilder>(pool, std::make_shared<arrow::Int32Builder>(), value_type);
            auto int32_to_int32_list_map_type = arrow::map(arrow::int32(), value_type);
            auto int32_to_int32_list_map_builder = std::make_shared<arrow::MapBuilder>(pool, std::make_shared<arrow::Int32Builder>(), value_builder, int32_to_int32_list_map_type);

            // dense_union
            auto dense_union_type = arrow::dense_union(
                {
                    std::make_shared<arrow::Field>("string_value", arrow::utf8(), false),
                    std::make_shared<arrow::Field>("bool_value", arrow::boolean(), false),
                    std::make_shared<arrow::Field>("bigint_value", arrow::int64(), false),
                    std::make_shared<arrow::Field>("int32_bitmask", arrow::int32(), false),
                    std::make_shared<arrow::Field>("string_list", string_list_type, false),
                    std::make_shared<arrow::Field>("int32_to_int32_list_map", int32_to_int32_list_map_type, false)
                });

            auto dense_union_builder = std::make_shared<arrow::DenseUnionBuilder>(
                pool,
                std::vector<std::shared_ptr<arrow::ArrayBuilder>>{
                    string_builder,
                    boolean_builder,
                    int64_builder,
                    int32_builder,
                    string_list_builder,
                    int32_to_int32_list_map_builder
                },
                dense_union_type);

            using SqlInfo = arrow::flight::protocol::sql::SqlInfo;

            auto info_name_builder = std::make_shared<arrow::UInt32Builder>();

            [[maybe_unused]] static const size_t SQL_INFO_STRING = 0;
            [[maybe_unused]] static const size_t SQL_INFO_BOOLEAN = 1;
            [[maybe_unused]] static const size_t SQL_INFO_INT64 = 2;
            [[maybe_unused]] static const size_t SQL_INFO_INT32 = 3;

            [[maybe_unused]] auto builder_string_append = [&](auto i, const std::string & v)
            {
                ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
                ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_STRING));
                return string_builder->Append(v);
            };

            [[maybe_unused]] auto builder_boolean_append = [&](auto i, bool v)
            {
                ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
                ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_BOOLEAN));
                return boolean_builder->Append(v);
            };

            [[maybe_unused]] auto builder_int64_append = [&](auto i, int64_t v)
            {
                ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
                ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_INT64));
                return int64_builder->Append(v);
            };

            [[maybe_unused]] auto builder_int32_append = [&](auto i, int32_t v)
            {
                ARROW_RETURN_NOT_OK(info_name_builder->Append(i));
                ARROW_RETURN_NOT_OK(dense_union_builder->Append(SQL_INFO_INT32));
                return int32_builder->Append(v);
            };

            if (!schema_only)
            {
                for (const auto & info_name : command.info())
                {
                    switch (info_name)
                    {
                        case SqlInfo::FLIGHT_SQL_SERVER_NAME:
                            ARROW_RETURN_NOT_OK(builder_string_append(info_name, "ClickHouse"));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_VERSION:
                            ARROW_RETURN_NOT_OK(builder_string_append(info_name, VERSION_STRING));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION:
                            ARROW_RETURN_NOT_OK(builder_string_append(info_name, ARROW_VERSION_STRING));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY:
                            ARROW_RETURN_NOT_OK(builder_boolean_append(info_name, false));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_SQL:
                            ARROW_RETURN_NOT_OK(builder_boolean_append(info_name, true));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT:
                            ARROW_RETURN_NOT_OK(builder_boolean_append(info_name, false));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION:
                            ARROW_RETURN_NOT_OK(builder_string_append(info_name, ""));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION:
                            ARROW_RETURN_NOT_OK(builder_string_append(info_name, ""));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION:
                            ARROW_RETURN_NOT_OK(builder_int32_append(info_name, arrow::flight::protocol::sql::SQL_SUPPORTED_TRANSACTION_NONE));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_CANCEL:
                            ARROW_RETURN_NOT_OK(builder_boolean_append(info_name, true));
                            break;
                        case SqlInfo::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT:
                        case SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT:
                            ARROW_RETURN_NOT_OK(builder_int32_append(info_name, 0));
                            break;
                        default:
                            return arrow::Status::Invalid("CommandGetSqlInfo for info_name " + std::to_string(info_name) + " is not implemented.");
                    }
                }
            }

            // Schema for table
            std::shared_ptr<arrow::Schema> table_schema = arrow::schema({
                arrow::field("info_name", arrow::uint32()),
                arrow::field("value", dense_union_type)
            });

            auto info_name = info_name_builder->Finish();
            ARROW_RETURN_NOT_OK(info_name);

            auto value = dense_union_builder->Finish();
            ARROW_RETURN_NOT_OK(value);

            return arrow::Table::Make(table_schema, {info_name.ValueUnsafe(), value.ValueUnsafe()});
        }
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCrossReference>())
    {
        return arrow::Status::NotImplemented("CommandGetCrossReference is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetCatalogs>())
    {
        sql = "SELECT '' AS catalog_name FROM numbers(0)";
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetDbSchemas>())
    {
        arrow::flight::protocol::sql::CommandGetDbSchemas command;
        any_msg.UnpackTo(&command);

        std::vector<std::string> where;
        if (command.has_db_schema_filter_pattern())
            where.push_back("database LIKE '" + command.db_schema_filter_pattern() + "'");

        auto where_expression = where.empty() ? "" : " WHERE " + boost::algorithm::join(where, " AND ");

        sql = "SELECT NULL::Nullable(String) AS catalog_name, name AS db_schema_name FROM system.databases" + where_expression;
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetExportedKeys>())
    {
        return arrow::Status::NotImplemented("CommandGetExportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetImportedKeys>())
    {
        return arrow::Status::NotImplemented("CommandGetImportedKeys is not supported");
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetPrimaryKeys>())
    {
        arrow::flight::protocol::sql::CommandGetPrimaryKeys command;
        any_msg.UnpackTo(&command);

        std::vector<std::string> where;
        where.push_back("database = '" + (command.has_db_schema() ? command.db_schema() : "default") + "'");
        where.push_back("name = '" + command.table() + "'");
        auto where_expression = where.empty() ? "" : " WHERE " + boost::algorithm::join(where, " AND ");

        sql =
            "SELECT "
                "NULL::Nullable(String) AS catalog_name, "
                "database AS schema_name, "
                "name AS table_name, "
                "(arrayJoin(arrayMap((x, y) -> (trimBoth(x), y), splitByChar(',', primary_key) AS p_keys, arrayEnumerate(p_keys))) AS p_key).1 AS column_name, "
                "p_key.2 AS key_seq, "
                "NULL::Nullable(String) AS pk_name "
            "FROM system.tables "
            + where_expression;
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetTables>())
    {
        arrow::flight::protocol::sql::CommandGetTables command;
        any_msg.UnpackTo(&command);

        std::vector<std::string> where;
        if (command.has_db_schema_filter_pattern())
            where.push_back("database LIKE '" + command.db_schema_filter_pattern() + "'");
        if (command.has_table_name_filter_pattern())
            where.push_back("table LIKE '" + command.table_name_filter_pattern() + "'");
        auto where_expression = where.empty() ? "" : " WHERE " + boost::algorithm::join(where, " AND ");

        if (command.include_schema())
        {
            sql =
                "SELECT "
                    "NULL::Nullable(String) AS catalog_name, "
                    "database::Nullable(String) AS db_schema_name, "
                    "table AS table_name, "
                    "engine AS table_type, "
                    "table_schema "
                "FROM system.tables AS left "
                "LEFT JOIN "
                "("
                    "SELECT "
                        "database, "
                        "table, "
                        "arraySort((x, y) -> y, groupArray((name, type)), groupArray(position)) AS table_schema "
                    "FROM system.columns "
                    "GROUP BY "
                        "database, "
                        "table"
                ") AS right ON left.database = right.database AND left.table = right.table";

            schema_modifier = [](std::shared_ptr<arrow::Schema> table_schema)
            {
                const auto & table_schema_field = table_schema->field(4);
                return table_schema->SetField(4, std::make_shared<arrow::Field>(table_schema_field->name(), arrow::binary(), table_schema_field->nullable()));
            };

            block_modifier = [](Block & block)
            {
                const auto & table_scheme_column = block.getByPosition(4);
                auto new_column = ColumnString::create();
                const auto & arr = typeid_cast<const ColumnArray &>(*table_scheme_column.column);
                const auto & tuple_col = typeid_cast<const ColumnTuple &>(arr.getData());
                const auto & name_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(0));
                const auto & type_col = typeid_cast<const ColumnString &>(tuple_col.getColumn(1));
                for (size_t i = 0; i < table_scheme_column.column->size(); ++i)
                {
                    ColumnsWithTypeAndName table_columns;
                    auto start = i ? arr.getOffsets()[i - 1] : 0;
                    auto end = arr.getOffsets()[i];
                    for (size_t j = 0; j < end - start; ++j)
                    {
                        const auto name = name_col.getDataAt(start + j);
                        const auto type = type_col.getDataAt(start + j);

                        auto data_type = DataTypeFactory::instance().get(String(type));
                        table_columns.emplace_back(nullptr, data_type, String(name));
                    }
                    auto table_schema = CHColumnToArrowColumn::calculateArrowSchema(table_columns, "Arrow", nullptr, {.output_string_as_string = true});
                    auto serialized_buffer = arrow::ipc::SerializeSchema(*table_schema, arrow::default_memory_pool()).ValueOrDie();
                    new_column->insertData(reinterpret_cast<const char *>(serialized_buffer->data()), serialized_buffer->size());
                }

                block.erase(4);
                block.insert(ColumnWithTypeAndName(std::move(new_column), std::make_shared<DataTypeString>(), table_scheme_column.name));
            };
        }
        else
            sql = "SELECT NULL::Nullable(String) AS catalog_name, database::Nullable(String) AS db_schema_name, table AS table_name, engine AS table_type FROM system.tables" + where_expression;
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandGetTableTypes>())
    {
        sql = "SELECT name AS table_type FROM system.table_engines";
    }
    else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementQuery>())
    {
        arrow::flight::protocol::sql::CommandStatementQuery command;
        any_msg.UnpackTo(&command);
        sql = command.query();
    }

    return CommandSelectorTuple{sql, schema_modifier, block_modifier};
}

arrow::Status ArrowFlightHandler::GetFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        std::string sql;
        std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
        std::function<void(Block &)> block_modifier;
        std::shared_ptr<arrow::Table> table;
        std::shared_ptr<arrow::Schema> schema;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            return arrow::Status::Invalid("Method GetFlightInfo cannot be called with a flight descriptor returned by method PollFlightInfo");
        }
        else
        {
            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = commandSelector(any_msg);
                if (const auto * command_selector_tuple = std::get_if<0>(&res))
                    std::tie(sql, schema_modifier, block_modifier) = *command_selector_tuple;
                else if (const auto * result_table = std::get_if<1>(&res))
                {
                    ARROW_RETURN_NOT_OK(*result_table);
                    table = result_table->ValueUnsafe();
                }
            }


            if (!table && sql.empty())
            {
                auto sql_res = convertGetDescriptorToSQL(request);
                ARROW_RETURN_NOT_OK(sql_res);
                sql = sql_res.ValueUnsafe();
            }
        }

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        if (table)
        {
            schema = table->schema();
            total_rows = table->num_rows();
            total_bytes = calculateTableBytes(table);
            auto ticket_info = calls_data->createTicket(table);
            arrow::flight::FlightEndpoint endpoint;
            endpoint.ticket = arrow::flight::Ticket{.ticket = ticket_info->ticket};
            endpoint.expiration_time = ticket_info->expiration_time;
            endpoints.emplace_back(endpoint);
        }
        else
        {
            // We generate a table for every chunk of data, which then produces ticket for every table
            // so clients can parallelize data retrieval.
            // However, it's unclear if this is necessary since we later indicate that data is ordered
            // and all endpoints are local. This forces clients to request data through the same connection,
            // and even with gRPC, clients are forced to prioritize the order.
            // TODO: Consider single ticket optimization for ordered local data to reduce overhead (executeSQLtoTable)
            auto execute_res = executeSQLtoTables(session, sql, schema_modifier, block_modifier);
            ARROW_RETURN_NOT_OK(execute_res);
            std::vector<std::shared_ptr<arrow::Table>> tables;
            std::tie(schema, tables) = execute_res.ValueUnsafe();

            for (auto & t : tables)
            {
                total_rows += t->num_rows();
                total_bytes += calculateTableBytes(t);
                auto ticket_info = calls_data->createTicket(t);
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket_info->ticket};
                endpoint.expiration_time = ticket_info->expiration_time;
                endpoints.emplace_back(endpoint);
            }
        }

        auto flight_info_res = arrow::flight::FlightInfo::Make(
            *schema,
            request,
            endpoints,
            total_rows,
            total_bytes,
            /* ordered = */ true);

        ARROW_RETURN_NOT_OK(flight_info_res);
        *info = std::make_unique<arrow::flight::FlightInfo>(std::move(flight_info_res).ValueOrDie());

        LOG_INFO(log, "GetFlightInfo returns flight info {}", (*info)->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("GetFlightInfo", impl);
}


arrow::Status ArrowFlightHandler::GetSchema(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * schema_result)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetSchema is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close http session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        std::shared_ptr<arrow::Schema> schema;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
            auto poll_info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            const auto & poll_info = poll_info_res.ValueOrDie();
            schema = poll_info->schema;
        }
        else
        {
            std::string sql;
            std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
            std::function<void(Block &)> block_modifier;
            std::shared_ptr<arrow::Table> table;

            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = commandSelector(any_msg, true);
                if (const auto * command_selector_tuple = std::get_if<0>(&res))
                    std::tie(sql, schema_modifier, block_modifier) = *command_selector_tuple;
                else if (const auto * result_table = std::get_if<1>(&res))
                {
                    ARROW_RETURN_NOT_OK(*result_table);
                    schema = result_table->ValueUnsafe()->schema();
                }
            }

            if (!schema)
            {
                if (sql.empty())
                {
                    auto sql_res = convertGetDescriptorToSQL(request);
                    ARROW_RETURN_NOT_OK(sql_res);
                    sql = sql_res.ValueUnsafe();
                }

                auto query_context = session->makeQueryContext();
                query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
                CurrentThread::QueryScope query_scope = CurrentThread::QueryScope::create(query_context);

                auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
                ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                PullingPipelineExecutor executor{block_io.pipeline};

                schema = CHColumnToArrowColumn::calculateArrowSchema(executor.getHeader().getColumnsWithTypeAndName(), "Arrow", nullptr, {.output_string_as_string = true});
                if (schema_modifier)
                {
                    auto status = schema_modifier(schema);
                    ARROW_RETURN_NOT_OK(status);
                    schema = status.ValueUnsafe();
                }
            }
        }

        auto schema_res = arrow::flight::SchemaResult::Make(*schema);
        ARROW_RETURN_NOT_OK(schema_res);
        *schema_result = std::make_unique<arrow::flight::SchemaResult>(*std::move(schema_res).ValueUnsafe());

        LOG_INFO(log, "GetSchema returns schema {}", schema->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("GetSchema", impl);
}


arrow::Status ArrowFlightHandler::PollFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::PollInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "PollFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        std::string sql;
        std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
        std::function<void(Block &)> block_modifier;
        std::shared_ptr<arrow::Table> table;

        std::shared_ptr<const PollDescriptorInfo> poll_info;
        std::shared_ptr<arrow::Schema> schema;
        std::optional<PollDescriptorWithExpirationTime> next_poll_descriptor;
        bool should_cancel_poll_descriptor = false;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
            ARROW_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
            auto poll_info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            poll_info = poll_info_res.ValueOrDie();
            schema = poll_info->schema;
            if (poll_info->next_poll_descriptor)
                next_poll_descriptor = calls_data->getPollDescriptorWithExpirationTime(*poll_info->next_poll_descriptor);
            should_cancel_poll_descriptor = cancel_poll_descriptor_after_poll_flight_info;
        }
        else
        {
            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = commandSelector(any_msg);
                if (const auto * command_selector_tuple = std::get_if<0>(&res))
                    std::tie(sql, schema_modifier, block_modifier) = *command_selector_tuple;
                else if (const auto * result_table = std::get_if<1>(&res))
                {
                    ARROW_RETURN_NOT_OK(*result_table);
                    table = result_table->ValueUnsafe();
                }
            }

            if (table)
            {
                auto ticket_info = calls_data->createTicket(table);
                std::vector<arrow::flight::FlightEndpoint> endpoints;
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket_info->ticket};
                endpoint.expiration_time = ticket_info->expiration_time;
                endpoints.emplace_back(endpoint);

                auto flight_info_res = arrow::flight::FlightInfo::Make(*table->schema(), request, endpoints, table->num_rows(), calculateTableBytes(table), /* ordered = */ true);
                ARROW_RETURN_NOT_OK(flight_info_res);
                auto flight_info = std::make_unique<arrow::flight::FlightInfo>(flight_info_res.ValueOrDie());
                *info = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info), std::nullopt, std::nullopt, std::nullopt);

                LOG_INFO(log, "PollFlightInfo returns {}", (*info)->ToString());
                return arrow::Status::OK();
            }

            if (sql.empty())
            {
                auto sql_res = convertGetDescriptorToSQL(request);
                ARROW_RETURN_NOT_OK(sql_res);
                sql = sql_res.ValueUnsafe();
            }

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.

            auto thread_group = ThreadGroup::createForQuery(query_context);
            CurrentThread::attachToGroup(thread_group);

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
            try
            {
                ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                auto poll_session = std::make_unique<PollSession>(query_context, thread_group, std::move(block_io), schema_modifier, block_modifier);
                schema = poll_session->getSchema();

                auto next_info = calls_data->createPollDescriptor(std::move(poll_session), /* previous_info = */ nullptr, query_context->getCurrentQueryId());
                next_poll_descriptor = *next_info;
            }
            catch (...)
            {
                block_io.onException();
                throw;
            }
        }

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        while (poll_info)
        {
            if (poll_info->ticket)
            {
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = *poll_info->ticket};
                endpoint.expiration_time = calls_data->getTicketExpirationTime(*poll_info->ticket);
                endpoints.emplace_back(endpoint);
            }
            if (poll_info->rows)
                total_rows += *poll_info->rows;
            if (poll_info->bytes)
                total_bytes += *poll_info->bytes;
            poll_info = poll_info->previous_info;
        }
        std::reverse(endpoints.begin(), endpoints.end());

        auto flight_info_res = arrow::flight::FlightInfo::Make(*schema, request, endpoints, total_rows, total_bytes, /* ordered = */ true);
        ARROW_RETURN_NOT_OK(flight_info_res);
        std::unique_ptr<arrow::flight::FlightInfo> flight_info = std::make_unique<arrow::flight::FlightInfo>(flight_info_res.ValueOrDie());

        std::optional<arrow::flight::FlightDescriptor> next;
        std::optional<Timestamp> expiration_time;
        if (next_poll_descriptor)
        {
            next = arrow::flight::FlightDescriptor::Command(next_poll_descriptor->poll_descriptor);
            expiration_time = next_poll_descriptor->expiration_time;
        }

        *info = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info), std::move(next), std::nullopt, expiration_time);

        if (should_cancel_poll_descriptor)
            calls_data->cancelPollDescriptor(request.cmd);

        LOG_INFO(log, "PollFlightInfo returns {}", (*info)->ToString());
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("PollFlightInfo", impl);
}


/// evaluatePollDescriptors() pulls a block from the query pipeline.
/// This function blocks until it either gets a nonempty block from the query pipeline or finds out that there will be no blocks anymore.
///
/// NOTE: The current implementation doesn't allow to set a timeout to avoid blocking calls as it's suggested in the documentation
/// for PollFlightInfo (see https://arrow.apache.org/docs/format/Flight.html#downloading-data-by-running-a-heavy-query).
arrow::Status ArrowFlightHandler::evaluatePollDescriptor(const String & poll_descriptor)
{
    auto poll_session_res = calls_data->startEvaluation(poll_descriptor);
    ARROW_RETURN_NOT_OK(poll_session_res);
    auto poll_session = std::move(poll_session_res).ValueOrDie();

    if (!poll_session)
    {
        /// Already evaluated.
        auto info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
        ARROW_RETURN_NOT_OK(info_res);
        const auto & info = info_res.ValueOrDie();
        if (!info->evaluated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Session is not attached to non-evaluated poll descriptor {}", poll_descriptor);
        return *info->status;
    }

    ThreadGroupSwitcher thread_group_switcher{poll_session->getThreadGroup(), ThreadName::ARROW_FLIGHT};
    bool last = false;

    try
    {
        std::optional<String> ticket;
        UInt64 rows = 0;
        UInt64 bytes = 0;
        Block block;
        if (poll_session->getNextBlock(block))
        {
            if (!block.empty())
            {
                auto header = getHeader(block.getColumnsWithTypeAndName());
                rows = block.rows();
                bytes = block.bytes();
                std::vector<Chunk> chunks;
                chunks.emplace_back(Chunk{std::move(block).getColumns(), rows});
                std::shared_ptr<arrow::Table> table = CHColumnToArrowColumn::chunkToArrowTable(header, "Arrow", chunks, {.output_string_as_string = true}, header.size(), poll_session->getSchema());
                auto ticket_info = calls_data->createTicket(table);
                ticket = ticket_info->ticket;
            }
        }
        else
        {
            last = true;
        }

        calls_data->endEvaluation(poll_descriptor, ticket, rows, bytes, last);
        poll_session->onFinish();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Poll: Failed to get next block");
        auto error_status = arrow::Status::ExecutionError("Poll: Failed to get next block: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
        calls_data->endEvaluationWithError(poll_descriptor, error_status);
        poll_session->onException();
        return error_status;
    }

    auto info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
    ARROW_RETURN_NOT_OK(info_res);
    const auto & info = info_res.ValueOrDie();
    if (last)
        calls_data->eraseFlightDescriptorMapByDescriptor(poll_descriptor);
    else
        calls_data->createPollDescriptor(std::move(poll_session), info);

    return arrow::Status::OK();
}


arrow::Status ArrowFlightHandler::DoGet(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Ticket & request,
    std::unique_ptr<arrow::flight::FlightDataStream> * stream)
{
    auto impl = [&]
    {
        LOG_INFO(log, "DoGet is called for ticket {}", request.ticket);
        std::vector<Chunk> chunks;
        std::shared_ptr<arrow::Table> table;
        bool should_cancel_ticket = false;

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        if (hasTicketPrefix(request.ticket))
        {
            auto ticket_info_res = calls_data->getTicketInfo(request.ticket);
            ARROW_RETURN_NOT_OK(ticket_info_res);
            const auto & ticket_info = ticket_info_res.ValueOrDie();
            table = ticket_info->arrow_table;
            should_cancel_ticket = cancel_ticket_after_do_get;
        }
        else
        {
            const String & sql = request.ticket;

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            CurrentThread::QueryScope query_scope = CurrentThread::QueryScope::create(query_context);

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
            try
            {
                ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                PullingPipelineExecutor executor{block_io.pipeline};

                Block block;
                while (executor.pull(block))
                    chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

                auto header = executor.getHeader();
                auto ch_to_arrow_converter = createCHToArrowConverter(header);
                ch_to_arrow_converter->chChunkToArrowTable(table, chunks, header.columns());
                block_io.onFinish();
            }
            catch (...)
            {
                block_io.onException();
                throw;
            }
        }

        auto stream_res = arrow::RecordBatchReader::MakeFromIterator(
            arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>{arrow::TableBatchReader{table}}, table->schema());
        ARROW_RETURN_NOT_OK(stream_res);
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(stream_res.ValueOrDie());

        if (should_cancel_ticket)
            calls_data->cancelTicket(request.ticket);

        LOG_INFO(log, "DoGet succeeded");
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoGet", impl);
}


arrow::Status ArrowFlightHandler::DoPut(
    const arrow::flight::ServerCallContext & context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
{
    auto impl = [&]
    {
        const auto & request = reader->descriptor();
        LOG_INFO(log, "DoPut is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        std::string sql;

        if (
            google::protobuf::Any any_msg;
                request.type == arrow::flight::FlightDescriptor::CMD
                && !request.cmd.empty()
                && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
        )
        {
            if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementUpdate>())
            {
                arrow::flight::protocol::sql::CommandStatementUpdate command;
                any_msg.UnpackTo(&command);
                sql = command.query();
            }
            else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementIngest>())
            {
                using CommandStatementIngest = arrow::flight::protocol::sql::CommandStatementIngest;
                CommandStatementIngest command;
                any_msg.UnpackTo(&command);
                if (command.has_table_definition_options())
                {
                    const auto & options = command.table_definition_options();
                    if (options.if_not_exist() != CommandStatementIngest::TableDefinitionOptions::TABLE_NOT_EXIST_OPTION_FAIL ||
                        options.if_exists() != CommandStatementIngest::TableDefinitionOptions::TABLE_EXISTS_OPTION_APPEND)
                    {
                        return arrow::Status::NotImplemented("Only appending to existing tables is supported (TABLE_NOT_EXIST_OPTION_FAIL + TABLE_EXISTS_OPTION_APPEND)");
                    }
                }

                if (command.has_catalog())
                    return arrow::Status::NotImplemented("Catalogs are not supported.");

                if (command.temporary())
                    return arrow::Status::NotImplemented("Implicit temporary tables are not supported.");

                sql = "INSERT INTO " + (command.has_schema() ? command.schema() + "." : "") + command.table() + " FORMAT Arrow";
            }
        }

        if (sql.empty())
        {
            auto sql_res = convertPutDescriptorToSQL(request);
            ARROW_RETURN_NOT_OK(sql_res);
            sql = sql_res.ValueOrDie();
        }

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope = CurrentThread::QueryScope::create(query_context);

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);
        try
        {
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
                pipeline.complete(std::move(output));
            }

            if (pipeline.completed())
            {
                CompletedPipelineExecutor executor(pipeline);
                executor.execute();
            }

            arrow::flight::protocol::sql::DoPutUpdateResult update_result;
            update_result.set_record_count(query_context->getProcessListElement()->getInfo().written_rows);
            ARROW_RETURN_NOT_OK(writer->WriteMetadata(*arrow::Buffer::FromString(update_result.SerializeAsString())));

            block_io.onFinish();
            LOG_INFO(log, "DoPut succeeded");
        }
        catch (...)
        {
            block_io.onException();
            throw;
        }

        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoPut", impl);
}


arrow::Status ArrowFlightHandler::tryRunAndLogIfError(std::string_view method_name, std::function<arrow::Status()> && func) const
{
    DB::setThreadName(ThreadName::ARROW_FLIGHT);
    ThreadStatus thread_status;
    try
    {
        auto status = std::move(func)();
        if (!status.ok())
            LOG_ERROR(log, "{} failed: {}", method_name, status.ToString());
        return status;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("{} failed", method_name));
        return arrow::Status::ExecutionError(method_name, " failed: ", getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }
}


arrow::Status ArrowFlightHandler::DoAction(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Action & action,
    std::unique_ptr<arrow::flight::ResultStream> * result_stream)
{
    auto impl = [&]
    {
        LOG_INFO(log, "DoAction is called for action {} {}", action.type, action.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = authenticate(auth, server.context());
        /// Close session (if any) after processing the request
        bool close_session = auth.sessionClose() && server.config().getBool("enable_arrow_close_session", true);
        SCOPE_EXIT_SAFE({ releaseOrCloseSession(session, auth.sessionId(), close_session); });

        std::vector<arrow::flight::Result> results;

        if (action.type == arrow::flight::ActionType::kCancelFlightInfo.type)
        {
            auto request = arrow::flight::CancelFlightInfoRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}).ValueOrDie();
            auto query_id = calls_data->getQueryIdFromFlightDescriptor(request.info->descriptor().cmd);
            auto result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kNotCancellable};
            if (query_id)
            {
                calls_data->eraseFlightDescriptorMapByQueryId(*query_id);
                auto& process_list = server.context()->getProcessList();
                auto cancel_result = process_list.sendCancelToQuery(*query_id, auth.username());
                if (cancel_result == CancellationCode::CancelSent)
                    result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kCancelled};
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})

            results.push_back(std::move(packed_result));
        }
        else if (action.type == arrow::flight::ActionType::kSetSessionOptions.type)
        {
            auto request = arrow::flight::SetSessionOptionsRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}).ValueOrDie();
            arrow::flight::SetSessionOptionsResult result;

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            CurrentThread::QueryScope query_scope = CurrentThread::QueryScope::create(query_context);

            auto visitor = overloaded {
                [](const std::monostate &) { return std::string(); },
                [](const std::string & str) { return fmt::format("='{}'", str); },
                [](bool b) { return fmt::format("={}", b ? "true" : "false"); },
                [](const std::vector<std::string> & strings) { return strings.empty() ? "=[]" : "=['" + boost::join(strings, "','")  + "']"; },
                [](const auto & v) { return fmt::format("={}", v); }
            };

            for (const auto & [setting, value] : request.session_options)
            {
                auto set_query = "SET " + setting + std::visit(visitor, value);
                std::optional<BlockIO> block_io;
                try
                {
                    auto [ast, bio] = executeQuery(set_query, query_context, QueryFlags{}, QueryProcessingStage::Complete);
                    block_io = std::move(bio);
                }
                catch (DB::Exception & e)
                {
                    auto error_value = [&]()
                    {
                        if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || e.code() == ErrorCodes::SYNTAX_ERROR)
                            return arrow::flight::SetSessionOptionErrorValue::kInvalidValue;
                        else if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                            return arrow::flight::SetSessionOptionErrorValue::kInvalidName;
                        else
                            return arrow::flight::SetSessionOptionErrorValue::kUnspecified;
                    }();

                    result.errors[setting] = arrow::flight::SetSessionOptionsResult::Error{error_value};
                }
                catch (...)
                {
                    if (block_io)
                        block_io->onException();
                    throw;
                }

                if (block_io)
                    block_io->onFinish();
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})

            results.push_back(std::move(packed_result));
        }
        else if (action.type == arrow::flight::ActionType::kGetSessionOptions.type)
        {
            arrow::flight::GetSessionOptionsRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}).ValueOrDie();
            arrow::flight::GetSessionOptionsResult result;

            auto execute_res = executeSQLtoTable(session, "SHOW SETTINGS LIKE '%'");
            ARROW_RETURN_NOT_OK(execute_res);
            auto [_, table] = execute_res.ValueUnsafe();
            for (int64_t i = 0; i < table->num_rows(); ++i)
            {
                auto name = static_cast<arrow::StringScalar&>(*table->column(0)->GetScalar(i).ValueOrDie()).ToString();
                auto value = static_cast<arrow::StringScalar&>(*table->column(2)->GetScalar(i).ValueOrDie()).ToString();
                result.session_options[name] = value;
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})

            results.push_back(std::move(packed_result));
        }
        else
            return arrow::Status::NotImplemented(action.type, " is not supported");

        *result_stream = std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));
        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoAction", impl);
}

}

#endif
