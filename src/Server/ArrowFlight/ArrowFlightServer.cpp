#include <Server/ArrowFlight/ArrowFlightServer.h>

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/AuthMiddleware.h>
#include <Server/ArrowFlight/commandSelector.h>

#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/quoteString.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/ArrowFlightSource.h>
#include <QueryPipeline/Pipe.h>
#include <Poco/FileStream.h>
#include <Poco/StreamCopier.h>
#include <Interpreters/ProcessList.h>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/protocol_internal.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int UNKNOWN_SETTING;
    extern const int SYNTAX_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace Setting
{
    extern const SettingsBool output_format_arrow_unsupported_types_as_binary;
}

namespace
{
    /// Helper for std::visit with multiple lambda overloads
    /// Usage:
    ///   std::variant<int, std::string> v = 42;
    ///   auto result = std::visit(overloaded {
    ///       [](int i) { return std::to_string(i); },
    ///       [](const std::string& s) { return s; },
    ///       [](const auto& other) { return "unknown"; }
    ///   }, v);
    template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; }; // NOLINT
    template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

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

        arrow::flight::FlightDescriptor original_flight_descriptor;
        std::string query_id;

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
            std::function<void(ContextPtr, Block &)> block_modifier_ = nullptr)
            : query_context(query_context_)
            , thread_group(thread_group_)
            , block_io(std::move(block_io_))
            , block_modifier(block_modifier_)
        {
            try
            {
                executor.emplace(block_io.pipeline);
                schema = CHColumnToArrowColumn::calculateArrowSchema(
                    executor->getHeader().getColumnsWithTypeAndName(),
                    "Arrow",
                    nullptr,
                    {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});

                if (schema_modifier)
                {
                    auto result = schema_modifier(schema);
                    if (!result.ok())
                        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to convert Arrow schema: {} (schema: {})", result.status().ToString(), schema->ToString());
                    schema = result.ValueUnsafe();
                }
            }
            catch (...)
            {
                try { block_io.onException(); }
                catch (...) { tryLogCurrentException("PollSession: block_io.onException() failed during constructor rollback"); }
                throw;
            }
        }

        ~PollSession() = default;

        ContextPtr queryContext() { return query_context; }

        ThreadGroupPtr getThreadGroup() const { return thread_group; }
        std::shared_ptr<arrow::Schema> getSchema() const { return schema; }
        bool getNextBlock(Block & block)
        {
            if (!executor->pull(block))
                return false;
            if (block_modifier)
                block_modifier(query_context, block);
            return true;
        }
        void onFinish() { block_io.onFinish(); }
        void onException() { block_io.onException(); }
        void onCancelOrConnectionLoss() { block_io.onCancelOrConnectionLoss(); }

    private:
        ContextPtr query_context;
        ThreadGroupPtr thread_group;
        BlockIO block_io;
        std::optional<PullingPipelineExecutor> executor;
        std::shared_ptr<arrow::Schema> schema;
        std::function<void(ContextPtr, Block &)> block_modifier;
    };

    /// Creates a converter to convert ClickHouse blocks to the Arrow format.
    std::shared_ptr<CHColumnToArrowColumn> createCHToArrowConverter(const Block & header, ContextPtr query_context)
    {
        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;
        arrow_settings.output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary];
        auto ch_to_arrow_converter = std::make_shared<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
        ch_to_arrow_converter->initializeArrowSchema();
        return ch_to_arrow_converter;
    }
}


/// Keeps information about calls - e.g. blocks extracted from query pipelines, flight tickets, poll descriptors.
class ArrowFlightServer::CallsData
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

    void eraseFlightDescriptorMapEntryLocked(const String & flight_descriptor) TSA_REQUIRES(mutex)
    {
        auto it_fd = flight_descriptor_to_query_id.find(flight_descriptor);
        if (it_fd == flight_descriptor_to_query_id.end())
            return;

        String query_id = it_fd->second;
        flight_descriptor_to_query_id.erase(it_fd);

        auto it_q = query_id_to_flight_descriptors.find(query_id);
        if (it_q == query_id_to_flight_descriptors.end())
            return;

        it_q->second.erase(flight_descriptor);
        if (it_q->second.empty())
            query_id_to_flight_descriptors.erase(it_q);
    }

    void eraseFlightDescriptorMapEntry(const String & flight_descriptor)
    {
        std::lock_guard lock{mutex};
        eraseFlightDescriptorMapEntryLocked(flight_descriptor);
    }

    /// Creates a poll descriptor.
    /// Poll descriptors are returned by method PollFlightInfo to get subsequent results from a long-running query.
    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptorImpl(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info, std::optional<arrow::flight::FlightDescriptor> flight_descriptor = std::nullopt, std::optional<String> query_id = std::nullopt)
    {
        String poll_descriptor;
        if (previous_info)
        {
            if (!previous_info->evaluated)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is not evaluated");
            if (!previous_info->next_poll_descriptor)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is final");
            poll_descriptor = *previous_info->next_poll_descriptor;
            query_id = getQueryIdByFlightDescriptor(previous_info->poll_descriptor);
            if (!query_id)
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                    "Cannot create continuation poll descriptor: previous poll descriptor {} was expired or cancelled",
                    previous_info->poll_descriptor);
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
        info->query_id = *query_id;
        if (previous_info)
            info->original_flight_descriptor = previous_info->original_flight_descriptor;
        else
            info->original_flight_descriptor = *flight_descriptor;
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

    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptor(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info)
    {
        return createPollDescriptorImpl(std::move(poll_session), previous_info);
    }

    std::shared_ptr<const PollDescriptorInfo>
    createPollDescriptor(std::unique_ptr<PollSession> poll_session, const arrow::flight::FlightDescriptor & flight_descriptor, const String & query_id)
    {
        return createPollDescriptorImpl(std::move(poll_session), nullptr, flight_descriptor, query_id);
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
            evaluation_ended.notify_all();
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
        else
        {
            evaluation_ended.notify_all();
        }
    }

    /// Cancels a poll descriptor to free memory.
    /// Poll descriptors are cancelled either by timer (if setting "arrowflight.poll_descriptors_lifetime_seconds" > 0)
    /// or after they are used by method PollFlightInfo (if setting "arrowflight.cancel_ticket_after_do_get" is set to true).
    void cancelPollDescriptor(const String & poll_descriptor)
    {
        std::unique_ptr<PollSession> poll_session_to_cancel;
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
            {
                poll_session_to_cancel = std::move(it2->second);
                poll_sessions.erase(it2);
            }
            eraseFlightDescriptorMapEntryLocked(poll_descriptor);
            evaluation_ended.notify_all();
        }

        if (poll_session_to_cancel)
        {
            try
            {
                poll_session_to_cancel->onCancelOrConnectionLoss();
            }
            catch (...)
            {
                tryLogCurrentException(log, "cancelPollDescriptor: block_io.onCancelOrConnectionLoss failed");
            }
        }
    }

    /// Cancels tickets and poll descriptors if the current time is greater than their expiration time.
    void cancelExpired()
    {
        std::vector<std::unique_ptr<PollSession>> poll_sessions_to_cancel;
        auto current_time = now();
        {
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

            for (auto it = poll_descriptors_by_expiration_time.begin(); it != poll_descriptors_by_expiration_time.end();)
            {
                if (current_time <= it->first)
                    break;

                auto pd_it = poll_descriptors.find(it->second);
                if (pd_it->second->evaluating)
                {
                    ++it;
                    continue;
                }

                LOG_DEBUG(log, "Cancelling expired poll descriptor {}", it->second);
                poll_descriptors.erase(pd_it);
                auto it2 = poll_sessions.find(it->second);
                if (it2 != poll_sessions.end())
                {
                    poll_sessions_to_cancel.emplace_back(std::move(it2->second));
                    poll_sessions.erase(it2);
                }
                eraseFlightDescriptorMapEntryLocked(it->second);
                it = poll_descriptors_by_expiration_time.erase(it);
            }
            updateNextExpirationTime();
        }

        for (auto & session : poll_sessions_to_cancel)
        {
            if (!session)
                continue;

            try
            {
                session->onCancelOrConnectionLoss();
            }
            catch (...)
            {
                tryLogCurrentException(log, "cancelExpired: block_io.onCancelOrConnectionLoss failed");
            }
        }
    }

    std::vector<String> collectPollDescriptorsForQueryId(const String & query_id) const
    {
        std::lock_guard lock{mutex};
        auto it = query_id_to_flight_descriptors.find(query_id);
        if (it == query_id_to_flight_descriptors.end())
            return {};
        return {it->second.begin(), it->second.end()};
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


ArrowFlightServer::ArrowFlightServer(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightServer"))
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

void ArrowFlightServer::start()
{
    chassert(!initialized && !stopped);

    bool use_tls = server.config().getBool("arrowflight.enable_ssl", false);

    auto location = addressToArrowLocation(address_to_listen, use_tls);

    arrow::flight::FlightServerOptions options(location);
    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.emplace_back(AUTHORIZATION_MIDDLEWARE_NAME, std::make_shared<AuthMiddlewareFactory>(server));

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

ArrowFlightServer::~ArrowFlightServer() = default;

void ArrowFlightServer::stop()
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

UInt16 ArrowFlightServer::portNumber() const
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

static std::shared_ptr<arrow::Table> getEmptyArrowTable(std::shared_ptr<arrow::Schema> schema)
{
    size_t columns_num = schema->num_fields();
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(columns_num);

    for (size_t i = 0; i < columns_num; ++i)
        empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{}, schema->field(static_cast<int>(i))->type()));

    return arrow::Table::Make(schema, empty_columns);
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables_impl(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    bool single_table,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(ContextPtr, Block &)> block_modifier = nullptr
)
{
    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
    QueryScope query_scope = QueryScope::create(query_context);

    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Table>> tables;

    auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

    bool query_finished = false;
    bool handling_exception = false;
    SCOPE_EXIT({
        if (query_finished)
            block_io.onFinish();
        else if (!handling_exception)
            block_io.onCancelOrConnectionLoss();
    });

    try
    {
        ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
        ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

        PullingPipelineExecutor executor{block_io.pipeline};
        schema = CHColumnToArrowColumn::calculateArrowSchema(
            executor.getHeader().getColumnsWithTypeAndName(),
            "Arrow",
            nullptr,
            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});

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
                    block_modifier(query_context, block);
                if (!header)
                    header = getHeader(block.getColumnsWithTypeAndName());
                chunks.emplace_back(Chunk{block.getColumns(), block.rows()});
                if (!single_table)
                {
                    tables.emplace_back(
                        CHColumnToArrowColumn::calculateArrowTable(
                            *header, "Arrow", chunks,
                            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
                            header->size(), schema));
                    chunks.clear();
                }
            }
        }

        if (!header)
            tables.emplace_back(getEmptyArrowTable(schema));
        else if (single_table)
            tables.emplace_back(
        CHColumnToArrowColumn::calculateArrowTable(
            *header, "Arrow", chunks,
            {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
            header->size(), schema));

        query_finished = true;
    }
    catch (...)
    {
        handling_exception = true;
        block_io.onException();
        throw;
    }

    return std::tuple{schema, tables};
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::vector<std::shared_ptr<arrow::Table>>>> executeSQLtoTables(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(ContextPtr, Block &)> block_modifier = nullptr
)
{
    return executeSQLtoTables_impl(session, sql, false, schema_modifier, block_modifier);
}

static arrow::Result<std::tuple<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::Table>>> executeSQLtoTable(
    const std::shared_ptr<Session> & session,
    const std::string & sql,
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier = nullptr,
    std::function<void(ContextPtr, Block &)> block_modifier = nullptr
)
{
    auto res = executeSQLtoTables_impl(session, sql, true, schema_modifier, block_modifier);
    ARROW_RETURN_NOT_OK(res);
    return std::tuple{std::get<0>(res.ValueUnsafe()), std::get<1>(res.ValueUnsafe()).front()};
}

arrow::Status ArrowFlightServer::GetFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::string sql;
        std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
        std::function<void(ContextPtr, Block &)> block_modifier;
        std::shared_ptr<arrow::Table> table;
        std::shared_ptr<arrow::Schema> schema;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            return arrow::Status::Invalid("Method GetFlightInfo cannot be called with a flight descriptor returned by method PollFlightInfo");
        }
        else
        {
            if (request.cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
                return arrow::Status::Invalid("Command payload is too large");
            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = ArrowFlight::commandSelector(any_msg);
                if (const auto * sql_set = res.getSQLSet())
                {
                    sql = sql_set->sql;
                    schema_modifier = sql_set->schema_modifier;
                    block_modifier = sql_set->block_modifier;
                }
                else if (const auto * result_table = res.getTable())
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
            endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
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
                endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
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


arrow::Status ArrowFlightServer::GetSchema(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::SchemaResult> * schema_result)
{
    auto impl = [&]
    {
        LOG_INFO(log, "GetSchema is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

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
            std::function<void(ContextPtr, Block &)> block_modifier;
            std::shared_ptr<arrow::Table> table;

            if (request.cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
                return arrow::Status::Invalid("Command payload is too large");
            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = ArrowFlight::commandSelector(any_msg, true);
                if (const auto * sql_set = res.getSQLSet())
                {
                    sql = sql_set->sql;
                    schema_modifier = sql_set->schema_modifier;
                    block_modifier = sql_set->block_modifier;
                }
                else if (const auto * result_table = res.getTable())
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
                QueryScope query_scope = QueryScope::create(query_context);

                auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

                bool query_finished = false;
                bool handling_exception = false;
                SCOPE_EXIT({
                    if (query_finished)
                        block_io.onFinish();
                    else if (!handling_exception)
                        block_io.onCancelOrConnectionLoss();
                });

                try
                {
                    ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                    ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                    PullingPipelineExecutor executor{block_io.pipeline};

                    schema = CHColumnToArrowColumn::calculateArrowSchema(
                        executor.getHeader().getColumnsWithTypeAndName(), "Arrow", nullptr,
                        {.output_string_as_string = true, .output_unsupported_types_as_binary = query_context->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]});
                    if (schema_modifier)
                    {
                        auto status = schema_modifier(schema);
                        ARROW_RETURN_NOT_OK(status);
                        schema = status.ValueUnsafe();
                    }

                    query_finished = true;
                }
                catch (...)
                {
                    handling_exception = true;
                    block_io.onException();
                    throw;
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


arrow::Status ArrowFlightServer::PollFlightInfo(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::PollInfo> * info)
{
    auto impl = [&]
    {
        LOG_INFO(log, "PollFlightInfo is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::string sql;
        std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
        std::function<void(ContextPtr, Block &)> block_modifier;
        std::shared_ptr<arrow::Table> table;

        std::shared_ptr<const PollDescriptorInfo> poll_info;
        std::shared_ptr<arrow::Schema> schema;
        std::optional<PollDescriptorWithExpirationTime> next_poll_descriptor;
        bool should_cancel_poll_descriptor = false;

        arrow::flight::FlightDescriptor original_flight_descriptor;
        std::string query_id;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && hasPollDescriptorPrefix(request.cmd))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
            ARROW_RETURN_NOT_OK(calls_data->extendPollDescriptorExpirationTime(poll_descriptor));
            auto poll_info_res = calls_data->getPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            poll_info = poll_info_res.ValueOrDie();
            original_flight_descriptor = poll_info->original_flight_descriptor;
            query_id = poll_info->query_id;
            schema = poll_info->schema;
            if (poll_info->next_poll_descriptor)
                next_poll_descriptor = calls_data->getPollDescriptorWithExpirationTime(*poll_info->next_poll_descriptor);
            should_cancel_poll_descriptor = cancel_poll_descriptor_after_poll_flight_info;
        }
        else
        {
            if (request.cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
                return arrow::Status::Invalid("Command payload is too large");
            if (
                google::protobuf::Any any_msg;
                    request.type == arrow::flight::FlightDescriptor::CMD
                    && !request.cmd.empty()
                    && any_msg.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))
            )
            {
                auto res = ArrowFlight::commandSelector(any_msg);
                if (const auto * sql_set = res.getSQLSet())
                {
                    sql = sql_set->sql;
                    schema_modifier = sql_set->schema_modifier;
                    block_modifier = sql_set->block_modifier;
                }
                else if (const auto * result_table = res.getTable())
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
                endpoint.ticket = arrow::flight::Ticket(ticket_info->ticket);
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
            SCOPE_EXIT({ CurrentThread::detachFromGroupIfNotDetached(); });

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            bool block_io_owned_here = true;
            SCOPE_EXIT({
                if (block_io_owned_here)
                    block_io.onCancelOrConnectionLoss();
            });

            ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
            ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

            block_io_owned_here = false;
            auto poll_session = std::make_unique<PollSession>(query_context, thread_group, std::move(block_io), schema_modifier, block_modifier);

            schema = poll_session->getSchema();

            original_flight_descriptor = request;
            query_id = query_context->getCurrentQueryId();
            auto next_info = calls_data->createPollDescriptor(std::move(poll_session), original_flight_descriptor, query_id);
            next_poll_descriptor = *next_info;
        }

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        while (poll_info)
        {
            if (poll_info->ticket)
            {
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{*poll_info->ticket};
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

        auto flight_info_res = arrow::flight::FlightInfo::Make(*schema, original_flight_descriptor, endpoints, total_rows, total_bytes, /* ordered = */ true, query_id);
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
arrow::Status ArrowFlightServer::evaluatePollDescriptor(const String & poll_descriptor)
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

    std::optional<String> ticket;
    try
    {
        UInt64 rows = 0;
        UInt64 bytes = 0;
        Block block;
        while (poll_session->getNextBlock(block))
        {
            if (block.empty())
                continue;

            auto header = getHeader(block.getColumnsWithTypeAndName());
            rows = block.rows();
            bytes = block.bytes();
            std::vector<Chunk> chunks;
            chunks.emplace_back(Chunk{std::move(block).getColumns(), rows});
            std::shared_ptr<arrow::Table> table = CHColumnToArrowColumn::calculateArrowTable(
                header, "Arrow", chunks,
                {.output_string_as_string = true, .output_unsupported_types_as_binary = poll_session->queryContext()->getSettingsRef()[Setting::output_format_arrow_unsupported_types_as_binary]},
                header.size(), poll_session->getSchema());
            auto ticket_info = calls_data->createTicket(table);
            ticket = ticket_info->ticket;
            break;
        }

        if (!ticket)
            poll_session->onFinish();
        calls_data->endEvaluation(poll_descriptor, ticket, rows, bytes, !ticket);
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
    if (!info_res.ok())
    {
        if (ticket)
            poll_session->onCancelOrConnectionLoss();
        return info_res.status();
    }
    const auto & info = info_res.ValueOrDie();
    if (!ticket)
        calls_data->eraseFlightDescriptorMapByDescriptor(poll_descriptor);
    else
        calls_data->createPollDescriptor(std::move(poll_session), info);

    return arrow::Status::OK();
}


arrow::Status ArrowFlightServer::DoGet(
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
        auto session = auth.getSession();

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
            QueryScope query_scope = QueryScope::create(query_context);

            auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

            bool query_finished = false;
            bool handling_exception = false;
            SCOPE_EXIT({
                if (query_finished)
                    block_io.onFinish();
                else if (!handling_exception)
                    block_io.onCancelOrConnectionLoss();
            });

            try
            {
                ARROW_RETURN_NOT_OK(checkNoCustomFormat(ast));
                ARROW_RETURN_NOT_OK(checkPipelineIsPulling(block_io.pipeline));

                PullingPipelineExecutor executor{block_io.pipeline};

                Block block;
                while (executor.pull(block))
                    chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

                auto header = executor.getHeader();
                auto ch_to_arrow_converter = createCHToArrowConverter(header, query_context);
                ch_to_arrow_converter->chChunkToArrowTable(table, chunks, header.columns());

                query_finished = true;
            }
            catch (...)
            {
                handling_exception = true;
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


arrow::Status ArrowFlightServer::DoPut(
    const arrow::flight::ServerCallContext & context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
{
    auto impl = [&]
    {
        const auto & request = reader->descriptor();
        LOG_INFO(log, "DoPut is called for descriptor {}", request.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::string sql;
        if (request.cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
            return arrow::Status::Invalid("Command payload is too large");
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
                if (!any_msg.UnpackTo(&command))
                    return arrow::Status::SerializationError("Deserialization of sql::CommandStatementUpdate failed.");
                if (command.query().empty())
                    return arrow::Status::Invalid("CommandStatementUpdate has an empty query");
                sql = command.query();
            }
            else if (any_msg.Is<arrow::flight::protocol::sql::CommandStatementIngest>())
            {
                using CommandStatementIngest = arrow::flight::protocol::sql::CommandStatementIngest;
                CommandStatementIngest command;
                if (!any_msg.UnpackTo(&command))
                    return arrow::Status::SerializationError("Deserialization of sql::CommandStatementIngest failed.");
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

                std::string schema_string;
                if (command.has_schema())
                {
                    if (!isValidIdentifier(command.schema()))
                        return arrow::Status::Invalid("Invalid schema name: ", command.schema());
                    schema_string = backQuoteIfNeed(command.schema()) + ".";
                }

                if (!isValidIdentifier(command.table()))
                    return arrow::Status::Invalid("Invalid table name: ", command.table());

                sql = "INSERT INTO " + schema_string + backQuoteIfNeed(command.table()) + " FORMAT Arrow";
            }
        }

        bool dont_write_flight_sql_metadata = sql.empty();
        if (sql.empty())
        {
            auto sql_res = convertPutDescriptorToSQL(request);
            ARROW_RETURN_NOT_OK(sql_res);
            sql = sql_res.ValueOrDie();
        }

        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        QueryScope query_scope = QueryScope::create(query_context);

        auto [ast, block_io] = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete);

        bool query_finished = false;
        bool handling_exception = false;
        SCOPE_EXIT({
            if (query_finished)
                block_io.onFinish();
            else if (!handling_exception)
                block_io.onCancelOrConnectionLoss();
        });

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

            query_finished = true;
        }
        catch (...)
        {
            handling_exception = true;
            block_io.onException();
            throw;
        }

        if (!dont_write_flight_sql_metadata)
        {
            arrow::flight::protocol::sql::DoPutUpdateResult update_result;
            if (auto element = query_context->getProcessListElement())
                update_result.set_record_count(element->getInfo().written_rows);
            else
                update_result.set_record_count(0);

            ARROW_RETURN_NOT_OK(writer->WriteMetadata(*arrow::Buffer::FromString(update_result.SerializeAsString())));
        }

        LOG_INFO(log, "DoPut succeeded");

        return arrow::Status::OK();
    };
    return tryRunAndLogIfError("DoPut", impl);
}


arrow::Status ArrowFlightServer::tryRunAndLogIfError(std::string_view method_name, std::function<arrow::Status()> && func) const
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


arrow::Status ArrowFlightServer::DoAction(
    const arrow::flight::ServerCallContext & context,
    const arrow::flight::Action & action,
    std::unique_ptr<arrow::flight::ResultStream> * result_stream)
{
    auto impl = [&]
    {
        LOG_INFO(log, "DoAction is called for action {} {}", action.type, action.ToString());

        const auto & auth = AuthMiddleware::get(context);
        auto session = auth.getSession();

        std::vector<arrow::flight::Result> results;

        if (action.type == arrow::flight::ActionType::kCancelFlightInfo.type)
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty CancelFlightInfo action.");
            ARROW_ASSIGN_OR_RAISE(auto request, arrow::flight::CancelFlightInfoRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}))
            LOG_DEBUG(log, "CancelFlightInfo request {}", request.ToString());
            auto query_id = request.info->app_metadata();
            auto result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kNotCancellable};

            if (!query_id.empty())
            {
                auto poll_descriptors = calls_data->collectPollDescriptorsForQueryId(query_id);

                auto & process_list = server.context()->getProcessList();
                auto cancel_result = process_list.sendCancelToQuery(query_id, auth.getUsername());
                if (cancel_result == CancellationCode::CancelSent)
                    result = arrow::flight::CancelFlightInfoResult{arrow::flight::CancelStatus::kCancelled};

                for (const auto & pd : poll_descriptors)
                    calls_data->cancelPollDescriptor(pd);
            }

            ARROW_ASSIGN_OR_RAISE(auto serialized, result.SerializeToString())
            ARROW_ASSIGN_OR_RAISE(auto packed_result, arrow::Result<arrow::flight::Result>{arrow::flight::Result{arrow::Buffer::FromString(std::move(serialized))}})
            results.push_back(std::move(packed_result));
        }
        else if (action.type == arrow::flight::ActionType::kSetSessionOptions.type)
        {
            if (!action.body)
                return arrow::Status::Invalid("Invalid empty SetSessionOptions action.");
            ARROW_ASSIGN_OR_RAISE(auto request, arrow::flight::SetSessionOptionsRequest::Deserialize({action.body->data_as<char>(), static_cast<size_t>(action.body->size())}))
            arrow::flight::SetSessionOptionsResult result;

            auto query_context = session->makeQueryContext();
            query_context->setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            QueryScope query_scope = QueryScope::create(query_context);

            auto visitor = overloaded {
                [](const std::monostate &) { return std::string("=DEFAULT"); },
                [](const std::string & str) { return fmt::format("={}", quoteString(str)); },
                [](bool b) { return fmt::format("={}", b ? "true" : "false"); },
                [](const std::vector<std::string> & strings)
                {
                    std::string res = "=[";
                    for (size_t i = 0; i < strings.size(); ++i)
                    {
                        if (i > 0) res += ",";
                        res += quoteString(strings[i]);
                    }
                    res += "]";
                    return res;
                },
                [](const auto & v) { return fmt::format("={}", v); }
            };

            for (const auto & [setting, value] : request.session_options)
            {
                if (!isValidIdentifier(setting))
                {
                    result.errors[setting] = arrow::flight::SetSessionOptionsResult::Error{
                        arrow::flight::SetSessionOptionErrorValue::kInvalidName
                    };
                    continue;
                }

                auto set_query = "SET " + backQuoteIfNeed(setting) + std::visit(visitor, value);
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
            std::string_view body_view = action.body
                ? std::string_view{action.body->data_as<char>(), static_cast<size_t>(action.body->size())}
                : std::string_view{};
            ARROW_RETURN_NOT_OK(arrow::flight::GetSessionOptionsRequest::Deserialize(body_view));
            arrow::flight::GetSessionOptionsResult result;

            auto execute_res = executeSQLtoTable(session, "SELECT name, value FROM system.settings");
            ARROW_RETURN_NOT_OK(execute_res);
            auto [_, table] = execute_res.ValueUnsafe();
            const auto & names = table->column(0);
            const auto & values = table->column(1);

            if (names->num_chunks() != values->num_chunks())
                return arrow::Status::Invalid("Unexpected chunk layout mismatch for settings columns");

            for (int chunk_idx = 0; chunk_idx < names->num_chunks(); ++chunk_idx)
            {
                const auto & name_chunk_any = names->chunk(chunk_idx);
                const auto & value_chunk_any = values->chunk(chunk_idx);

                if (name_chunk_any->type_id() != arrow::Type::STRING || value_chunk_any->type_id() != arrow::Type::STRING)
                    return arrow::Status::TypeError("Expected STRING chunks in settings result");

                if (name_chunk_any->length() != value_chunk_any->length())
                    return arrow::Status::Invalid("Mismatched chunk lengths for settings columns");

                const auto & name_chunk = static_cast<const arrow::StringArray &>(*name_chunk_any);
                const auto & value_chunk = static_cast<const arrow::StringArray &>(*value_chunk_any);

                for (int64_t i = 0; i < name_chunk.length(); ++i)
                {
                    if (name_chunk.IsNull(i) || value_chunk.IsNull(i))
                        continue;
                    result.session_options[name_chunk.GetString(i)] = value_chunk.GetString(i);
                }
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
