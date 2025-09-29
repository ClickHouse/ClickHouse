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
    const String TICKET_PREFIX = "--TICKET-";
    const String POLL_DESCRIPTOR_PREFIX = "--POLL-";

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

            std::string credentials = DB::base64Decode(token, true);
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

    [[nodiscard]] arrow::flight::Result<String> convertDescriptorToSQL(const arrow::flight::FlightDescriptor & descriptor, bool for_put_operation)
    {
        switch (flight_descriptor.type)
        {
            case arrow::flight::FlightDescriptor::PATH:
            {
                const auto & path = flight_descriptor.path;
                if (path.size() != 1)
                    return arrow::Status::Invalid("Flight descriptor's path should be one-component (got ", path.size(), " components)");
                if (path[0].empty())
                    return arrow::Status::Invalid("Flight descriptor's path should specify the name of a table");
                const String & table_name = path[0];
                if (for_put_operation)
                    return "INSERT INTO " + backQuoteIfNeed(table_name);
                else
                    return "SELECT * FROM " + backQuoteIfNeed(table_name);
            }
            case arrow::flight::FlightDescriptor::CMD:
            {
                const auto & cmd = flight_descriptor.cmd;
                if (cmd.empty())
                    return arrow::Status::Invalid("Flight descriptor's command should specify a ClickHouse query");
                return cmd;
            }
            default:
                return arrow::Status::TypeError("Flight descriptor has unknown type ", toString(flight_descriptor.type));
        }
    }

    [[nodiscard]] arrow::flight::Result<String> convertGetDescriptorToSQL(const arrow::flight::FlightDescriptor & descriptor)
    {
        return convertDescriptorToSQL(descriptor, /* for_put_operation = */ false);
    }

    [[nodiscard]] arrow::flight::Result<String> convertPutDescriptorToSQL(const arrow::flight::FlightDescriptor & flight_descriptor)
    {
        return convertDescriptorToSQL(descriptor, /* for_put_operation = */ true);
    }

    std::shared_ptr<CHColumnToArrowColumn> createCHToArrowConverter(const Block & header)
    {
        CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;  
        return std::make_shared<CHColumnToArrowColumn>(header, "Arrow", arrow_settings);
    }

    using Timestamp = std::chrono::system_clock::time_point;
    using Duration = std::chrono::system_clock::duration;

    Timestamp now()
    {
        return std::chrono::system_clock::now();
    }

    static const Timestamp ALREADY_EXPIRED = {0};

    struct TicketWithExpirationTime
    {
        String ticket;
        std::optional<Timestamp> expiration_time;
    };

    bool hasTicketPrefix(const String & ticket)
    {
        return ticket.starts_with(TICKET_PREFIX);
    }

    bool hasPollPrefix(const String & ticket)
    {
        return ticket.starts_with(POLL_DESCRIPTOR_PREFIX);
    }

    struct TicketInfo : public TicketWithExpirationTime
    {
        ConstBlockPtr block;
        std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
    };

    struct PollDescriptorWithExpirationTime
    {
        String poll_descriptor;
        std::optional<Timestamp> expiration_time;
    };

    struct PollDescriptorInfo : public PollDescriptorWithExpirationTime
    {
        std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
        std::shared_ptr<PollDescriptorInfo> previous_info;
        bool evaluating = false;
        bool evaluated = false;
        /// The following fields can be set only if `evaluated == true`.
        String ticket;
        size_t rows = 0;
        size_t bytes = 0;
        String next_descriptor;
        std::exception_ptr error;
    };

    /// Keeps a query context and a pipeline executor for PollFlightInfo.
    class PollSession
    {
    public:
        PollSession(std::unique_ptr<Session> session_, ContextPtr query_context_, ThreadGroupPtr thread_group_, BlockIO && block_io,
                    std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter_)
            : session(std::move(session_))
            , query_context(query_context_)
            , thread_group(thread_group_)
            , block_io(std::move(block_io))
            , executor(block_io.pipeline)
            , ch_to_arrow_converter(ch_to_arrow_converter_)
        {
        }

        ~PollSession() = default;

        ThreadGroupPtr getThreadGroup() const { return thread_group; }
        std::shared_ptr<CHColumnToArrowColumn> getCHToArrowConverter() const { return ch_to_arrow_converter; }

        bool getNextBlock(Block & block) { return executor->pull(block); }

    private:
        std::unique_ptr<Session> session;
        ContextPtr query_context;
        ThreadGroupPtr thread_group;
        BlockIO io;
        PullingPipelineExecutor executor;
        std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
    };
}


/// Keeps information about calls - e.g. blocks extracted from pipelines, tickets, poll descriptors.
class ArrowFlightHandler::CallsData
{
public:
    CallsData(std::optional<Duration> tickets_lifetime_, std::optional<Duration> poll_descriptors_lifetime_)
        : tickets_lifetime(tickets_lifetime_)
        , poll_descriptors_lifetime(poll_descriptors_lifetime_)
    {
    }

    ~CallsData() = default;

    /// Adds a ticket for a specified block.
    std::shared_ptr<const TicketInfo>
    addTicket(ConstBlockPtr block, std::shared_ptr<CHColumnToArrowColumn> ch_to_arrow_converter)
    {
        String ticket = generateTicket();
        Timestamp current_time = now();
        auto expiration_time = calculateTicketExpirationTime(current_time);
        auto new_info = std::make_shared<TicketInfo>(TicketInfo{
            .ticket = ticket, .expiration_time = expiration_time, .block = block, .ch_to_arrow_converter = ch_to_arrow_converter});
        std::lock_guard lock{mutex};
        tickets[ticket] = new_info;
        if (expiration_time)
        {
            tickets_by_expiration_time.emplace(expiration_time, ticket);
            updateNextExpirationTime();
        }
        return new_info;
    }

    TicketWithExpirationTime getTicketWithExpirationTime(const String & ticket) const
    {
        if (!tickets_lifetime)
            return TicketWithExpirationTime{.ticket = ticket, .expiration_time = std::nullopt};
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return TicketWithExpirationTime{.ticket = ticket, .expiration_time = ALREADY_EXPIRED};
        return *it->second;
    }

    /// Returns the information about a ticket.
    [[nodiscard]] arrow::Result<std::shared_ptr<const TicketInfo>> tryGetTicketInfo(const String & ticket) const
    {
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        return it->second;
    }

    /// Extends the expiration time of a ticket.
    [[nodiscard]] arrow::Status tryExtendTicketExpirationTime(const String & ticket) const
    {
        if (!tickets_lifetime)
            return arrow::Status::OK();
        Timestamp current_time = now();
        std::lock_guard lock{mutex};
        auto it = tickets.find(ticket);
        if (it == tickets.end())
            return arrow::Status::KeyError("Ticket ", quoteString(ticket), " not found");
        auto info = it->second;
        auto old_expiration_time = *info->expiration_time;
        auto new_expiration_time = calculateTicketExpirationTime(current_time);
        auto new_info = std::make_shared<TicketInfo>(*info);
        new_info->expiration_time = new_expiration_time;
        it->second = new_info;
        tickets_by_expiration_time.erase(std::make_pair(*old_expiration_time, ticket));
        tickets_by_expiration_time.emplace(*new_expiration_time, ticket);
        updateNextExpirationTime();
        return arrow::Status::OK();
    }

    /// Forgets a ticket to free memory.
    void forgetTicket(const String & ticket)
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

    /// Adds a poll descriptor.
    std::shared_ptr<const PollDescriptorInfo>
    addPollDescriptor(std::unique_ptr<PollSession> poll_session, std::shared_ptr<const PollDescriptorInfo> previous_info)
    {
        String poll_descriptor;
        if (previous_info)
        {
            if (!previous_info->evaluated)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is not evaluated");
            if (previous_info->next_poll_descriptor.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Adding a poll descriptor while the previous poll descriptor is marked as final");
            poll_descriptor = previous_info->next_poll_descriptor;
        }
        else
        {
            poll_descriptor = generatePollDescriptor();
        }
        auto current_time = now();
        auto expiration_time = calculatePollDescriptorExpirationTime(current_time);
        auto info = std::make_shared<PollDescriptorInfo>(PollDescriptorInfo{
            .poll_descriptor = poll_descriptor,
            .expiration_time = expiration_time,
            .ch_to_arrow_converter = poll_session->getCHToArrowConverter()});
        std::lock_guard lock{mutex};
        poll_descriptors[poll_descriptor] = info;
        poll_sessions[poll_descriptor] = std::move(poll_session);
        if (expiration_time)
        {
            poll_descriptors_by_expiration_time.emplace(expiration_time, poll_descriptor);
            updateNextExpirationTime();
        }
        return info;
    }

    /// Returns the information about a poll descriptor.
    [[nodiscard]] arrow::Result<std::shared_ptr<const PollDescriptorInfo>> tryGetPollDescriptorInfo(const String & poll_descriptor) const
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
            return arrow::Status::KeyError("Poll descriptor ", quoteString(poll_descriptor), " not found");
        return it->second;
    }

    PollDescriptorWithExpirationTime getPollDescriptorWithExpirationTime(const String & poll_descriptor) const
    {
        if (!poll_lifetime)
            return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = std::nullopt};
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it == poll_descriptors.end())
            return PollDescriptorWithExpirationTime{.poll_descriptor = poll_descriptor, .expiration_time = ALREADY_EXPIRED};
        return *it->second;
    }

    /// Extends the expiration time of a poll descriptor.
    [[nodiscard]] arrow::Status tryExtendPollDescriptorExpirationTime(const String & poll_descriptor) const
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
        poll_descriptor_by_expiration_time.erase(std::make_pair(*old_expiration_time, poll_descriptor));
        poll_descriptor_by_expiration_time.emplace(*new_expiration_time, poll_descriptor);
        updateNextExpirationTime();
        return arrow::Status::OK();
    }

    /// Starts evaluation (i.e. getting a block of data) for a specified poll descriptor.
    /// The function returns nullptr if it's already evaluated.
    /// If it's being evaluated at the moment in another thread the function waits until it finishes and then returns nullptr.
    [[nodiscard]] arrow::Result<std::unique_ptr<PollSession>> tryStartEvaluation(const String & poll_descriptor)
    {
        arrow::Result<std::unique_ptr<PollSession>> res;
        std::unique_lock lock{mutex};
        evaluation_ended.wait(lock, [&]
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
    void endEvaluation(const String & poll_descriptor, const String & ticket, UInt64 rows, UInt64 bytes, bool last)
    {
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
                new_info->ticket = ticket;
                new_info->rows = rows;
                new_info->bytes = bytes;
                if (!last)
                    new_info->next_poll_descriptor = generatePollDescriptor();
                it->second = new_info;
                info = new_info;
                evaluation_ended.notify_all();
            }
        }
    }

    /// Ends evaluation for a specified poll descriptor with an error.
    void endEvaluationWithError(const String & poll_descriptor, std::exception_ptr error)
    {
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
                new_info->error = std::move(error);
                it->second = new_info;
                info = new_info;
                evaluation_ended.notify_all();
            }
        }
    }

    /// Forgets a poll descriptor to free memory.
    void forgetPollDescriptor(const String & poll_descriptor)
    {
        std::lock_guard lock{mutex};
        auto it = poll_descriptors.find(poll_descriptor);
        if (it != poll_descriptors.end())
        {
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

    /// Forgets tickets and poll descriptors if `current_time` is greater than their expiration time.
    void forgetExpired()
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
        while (!poll_descriptors_by_expiration_time.empty())
        {
            auto it = poll_descriptors_by_expiration_time.begin();
            if (current_time <= it->first)
                break;
            poll_descriptors.erase(it->second);
            poll_sessions.erase(it->second);
            poll_descriptors_by_expiration_time.erase(it);
        }
        updateNextExpirationTime();
    }

    /// Waits until it's time to forget expired tickets or poll descriptors.
    void waitUntilExpirationTime() const
    {
        auto current_time = now();
        std::unique_lock lock{mutex};
        auto is_ready = [&]
        {
            if (stop_waiting_expiration_time)
                return true;
            current_time = now();
            return next_expiration_time && (current_time > *next_expiration_time);
        };
        if (next_expiration_time)
        {
            if (current_time < *next_expiration_time)
                next_expiration_time_decreased.wait_for(*next_expiration_time - current_time, is_ready);
        }
        else
        {
            next_expiration_time_decreased.wait(is_ready);
        }
    }

    void stopWaitingExpirationTime()
    {
        std::lock_guard lock{mutex};
        stop_waiting_expiration_time = true;
    }

private:
    static String generateTicket()
    {
        return fmt::format("TICKET-{}", toString(UUIDHelpers::generateV4()));
    }

    static String generatePollDescriptor()
    {
        return fmt::format("POLL-{}", toString(UUIDHelpers::generateV4()));
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
        auto old_next_expiration_time = expiration_time;
        next_expiration_time.reset();
        if (!tickets_by_expiration_time.empty())
            next_expiration_time = tickets_by_expiration_time.begin()->first;
        if (!poll_descriptors_by_expiration_time.empty())
        {
            auto other_time = poll_descriptors_by_expiration_time.begin()->first;
            next_expiration_time = next_expiration_time ? std::min(*next_expiration_time, other_time) : other_time;
        }
        if (next_expiration_time && (!old_next_expiration_time || next_expiration_time < *old_next_expiration_time))
            next_expiration_time_decreased.notify_all();
    }

    const std::optional<Duration> tickets_lifetime;
    const std::optional<Duration> poll_descriptors_lifetime;
    std::mutex mutex;
    std::unordered_map<String, std::shared_ptr<const TicketInfo>> tickets TSA_GUARDED_BY(mutex);
    std::unordered_map<String, std::shared_ptr<const PollDescriptorInfo>> poll_descriptors TSA_GUARDED_BY(mutex);
    std::unordeded_map<String, std::unique_ptr<PollSession>> poll_sessions TSA_GUARDED_BY(mutex);
    std::condition_variable evaluation_ended;
    std::set<std::pair<Timestamp, String>> tickets_by_expiration_time TSA_GUARDED_BY(mutex);
    std::set<std::pair<Timestamp, Strings>> poll_descriptors_by_expiration_time TSA_GUARDED_BY(mutex);
    std::optional<Timestamp> next_expiration_time;
    std::condition_variable next_expiration_time_decreased;
    bool stop_waiting_expiration_time = false;
};













/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
    class PullingSession
    {
    public:
        PullingSession()

    private:
        arrow::flight::FlightDescriptor flight_descriptor;
        String sql;
        ASTPtr ast;
        std::unique_ptr<Session> session;
        ContextPtr query_context;
        std::optional<CurrentThread::QueryScope> query_scope;
        BlockIO io;
        std::unique_ptr<PullingPipelineExecutor> pulling_executor;
        std::unique_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
        Strings tickets;
        Strings poll_descriptors;
        std::unordered_map<String, size_t> num_tickets_by_poll
    };


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
#endif
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////





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

    expiration_thread.emplace([this]
    {
        try
        {
            setThreadName("ArrowFlightExpr");
            while (!stopped)
            {
                calls_data->waitExpirationTime();
                calls_data->forgetExpired();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to forget expired");
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

        calls_data->stopWaitingExpirationTime();
        if (expiration_thread)
        {
            expiration_thread->join();
            expiration_thread.reset();
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
    const arrow::flight::FlightDescriptor & request,
    std::unique_ptr<arrow::flight::FlightInfo> * res)
{
    LOG_INFO(getLogger("!!!"), "ArrowFlightHandler::GetFlightInfo");
    setThreadName("ArrowFlight");
    ThreadStatus thread_status;

    try
    {
        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;
        std::shared_ptr<CHColumnToArrowConverter> ch_to_arrow_converter;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && (hasPollDescriptorPrefix(request.cmd)))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
            auto poll_info_res = calls_data->tryGetPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_info_res);
            auto poll_info = poll_info_res.ValueOrDie();
            ch_to_arrow_converter = poll_info->ch_to_arrow_converter;
            while (poll_info)
            {
                total_rows += poll_info->rows;
                total_bytes += poll_info->bytes;
                if (!poll_info->ticket.empty())
                {
                    auto ticket = calls_data->getTicketWithExpirationTime(poll_info->ticket);
                    arrow::flight::FlightEndpoint endpoint;
                    endpoint.ticket = arrow::flight::Ticket{.ticket = ticket.ticket, .expiration_time = ticket.expiration_time};
                    endpoints.emplace_back(endpoint);
                }
            }
            std::reverse(endpoints.begin(), endpoints.end());
        }
        else
        {
            auto sql_res = convertGetDescriptorToSQL(descriptor);
            ARROW_RETURN_NOT_OK(sql_res);
            auto sql = sql_res.ValueOrDie();

            Session session{global_context, ClientInfo::Interface::ARROW_FLIGHT};

            const auto & auth = AuthMiddleware::get(context);
            session.authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

            auto query_context = session.makeQueryContext();
            query_context.setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            CurrentThread::QueryScope query_scope{query_context};

            auto block_io = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete).second;
            PullingPipelineExecutor executor{block_io.pipeline};

            ch_to_arrow_converter = createCHToArrowConverter(executor.getHeader());

            Block block;
            while (executor.pull(block))
            {
                if (!block.empty())
                {
                    total_rows += block->rows();
                    total_bytes += block->bytes();
                    auto info = calls_data->addTicket(std::make_shared<Block>(std::move(block)), ch_to_arrow_converter);
                    arrow::flight::FlightEndpoint endpoint;
                    endpoint.ticket = arrow::flight::Ticket{.ticket = info->ticket, .expiration_time = info->expiration_time};
                    endpoints.emplace_back(endpoint);
                }
            }
        }

        auto flight_info_res = arrow::flight::FlightInfo::Make(
            ch_to_arrow_converter->getArrowSchema(),
            request,
            endpoints,
            total_rows,
            total_bytes,
            /* ordered = */ true);

        ARROW_RETURN_NOT_OK(flight_info_res);
        *res = std::make_unique<arrow::flight::FlightInfo>(std::move(flight_info_res).ValueOrDie());
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::ExecutionError("GetFlightInfo failed: ", e.displayText());
    }
}

arrow::Status ArrowHander::evaluatePollDescriptor(const String & poll_descriptor)
{
    auto poll_session_res = calls_data.tryStartEvaluation(poll_descriptor);
    ARROW_RETURN_NOT_OK(poll_session_res);
    auto poll_session = poll_session_res.ValueOrDie();

    if (!poll_session)
    {
        /// Already evaluated.
        auto info_res = calls_data.tryGetPollDescriptorInfo(poll_descriptor);
        ARROW_RETURN_NOT_OK(info_res);
        auto info = info_res.ValueOrDie();
        if (!info->evaluated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Session is not attached to non-evaluated poll descriptor {}", poll_descriptor);
        if (info->error)
            return arrow::Status::ExecutionError("PollFlightInfo failed: ", getExceptionMessage(info->error, /* with_stacktrace = */ true));
        return arrow::Status::OK();
    }

    ThreadGroupSwitcher thread_group_switcher{poll_session->getThreadGroup(), "ArrowFlight"};
    std::shared_ptr<CHColumnToArrowConverter> converter = poll_session->getCHToArrowConverter();

    try
    {
        String ticket;
        UInt64 rows = 0;
        UInt64 bytes = 0;
        bool last = false;
        Block block;
        if (poll_session->getNextBlock(block))
        {
            if (!block.empty())
            {
                rows = block->rows();
                bytes = block->bytes();
                ticket = calls_data->addTicket(std::make_shared<Block>(std::move(block)), converter);
            }
        }
        else
        {
            last = true;
        }

        calls_data.endEvaluation(poll_descriptor, ticket, rows, bytes, last);
    }
    catch (...)
    {
        calls_data.endEvaluationWithError(poll_descriptor, current_exception());
        return arrow::Status::ExecutionError("PollFlightInfo failed: ", getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }

    auto info_res = calls_data.tryGetPollDescriptorInfo(poll_descriptor);
    ARROW_RETURN_NOT_OK(info_res);
    auto info = info_res.ValueOrDie();

    if (!last)
        calls_data.addPollDescriptor(converter->getSchema(), info, std::move(poll_session));

    return arrow::Status::OK();
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
        std::shared_ptr<const PollDescriptorInfo> poll_info;
        PollDescriptorWithExpirationTime next_descriptor;

        if ((request.type == arrow::flight::FlightDescriptor::CMD) && (hasPollDescriptorPrefix(request.cmd)))
        {
            const String & poll_descriptor = request.cmd;
            ARROW_RETURN_NOT_OK(evaluatePollDescriptor(poll_descriptor));
            auto poll_descriptor_info_res = calls_data->tryGetPollDescriptorInfo(poll_descriptor);
            ARROW_RETURN_NOT_OK(poll_descriptor_info_res);
            poll_descriptor_info = poll_descriptor_info_res.ValueOrDie();
            if (!poll_descriptor_info->next_descriptor.empty())
                next_descriptor = calls_data->getPollDescriptorWithExpirationTime(poll_descriptor_info->next_descriptor);
        }
        else
        {
            auto sql_res = convertGetDescriptorToSQL(descriptor);
            ARROW_RETURN_NOT_OK(sql_res);
            auto sql = sql_res.ValueOrDie();

            auto session = std::make_unique<Session>(global_context, ClientInfo::Interface::ARROW_FLIGHT);

            const auto & auth = AuthMiddleware::get(context);
            session->authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

            auto query_context = session->makeQueryContext();
            query_context.setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.

            auto thread_group = ThreadGroup::createForQuery(query_context);
            CurrentThread::attachToGroup(thread_group);

            auto block_io = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete).second;
            auto ch_to_arrow_converter = createCHToArrowConverter(block_io.pipeline.getHeader());

            auto poll_session = std::make_unique<PollSession>(std::move(session), query_context, thread_groip, std::move(block_io),
                                                              ch_to_arrow_converter);

            next_descriptor = *calls_data->addPollDescriptor(ch_to_arrow_converter->getSchema(), std::move(poll_session), /* previous_info = */ nullptr);
        }

        std::vector<arrow::flight::FlightEndpoint> endpoints;
        int64_t total_rows = 0;
        int64_t total_bytes = 0;

        while (poll_info)
        {
            total_rows += poll_info->rows;
            total_bytes += poll_info->bytes;
            if (!poll_info->ticket.empty())
            {
                auto ticket = calls_data->getTicketWithExpirationTime(poll_info->ticket);
                arrow::flight::FlightEndpoint endpoint;
                endpoint.ticket = arrow::flight::Ticket{.ticket = ticket.ticket, .expiration_time = ticket.expiration_time};
                endpoints.emplace_back(endpoint);
            }
        }
        std::reverse(endpoints.begin(), endpoints.end());

        std::unique_ptr<arrow::flight::FlightInfo> flight_info;
        if (!endpoints.empty())
        {
            auto flight_info_res = arrow::flight::FlightInfo::Make(ch_to_arrow_converter->getArrowSchema(), request, endpoints, total_rows, total_bytes, /* ordered = */ true);
            ARROW_RETURN_NOT_OK(flight_info_res);
            flight_info = std::make_unique<arrow::flight::FlightInfo>(flight_info_res.ValueOrDie());
        }

        std::optional<arrow::flight::FlightDescriptor> next;
        std::optional<Timestamp> expiration_time;
        if (!next_descriptor.poll_descriptor.empty())
        {
            next = arrow::flight::FlightDescriptor::Command(next_descriptor.poll_descriptor);
            expiration_time = next_descriptor.expiration_time;
        }

        *res = std::make_unique<arrow::flight::PollInfo>(std::move(flight_info), std::move(next), std::nullopt, expiration_time);
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
        auto sql_res = convertGetDescriptorToSQL(request);
        ARROW_RETURN_NOT_OK(sql_res);
        auto sql = sql_res.ValueOrDie();

        Session session{global_context, ClientInfo::Interface::ARROW_FLIGHT};

        const auto & auth = AuthMiddleware::get(context);
        session.authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

        auto query_context = session.makeQueryContext();
        query_context.setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope{query_context};

        auto block_io = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete).second;
        ARROW_RETURN_IF(!block_io.pipeline.pulling(), 
                        arrow::Status::IOError("GetSchema failed: Query doesn't allow pulling data, use method do_put() with this kind of query"));

        auto schema = createCHToArrowConverter(block_io.pipeline.getHeader())->getArrowSchema();
        auto schema_res = arrow::flight::SchemaResult::Make(schema);
        ARROW_RETURN_NOT_OK(schema_res);

        *res = std::make_unique<arrow::flight::SchemaResult>(*std::move(schema_res).ValueOrDie());
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        ARROW_RETURN_NOT_OK(arrow::Status::IOError("GetSchema failed: " + e.displayText()));
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
        Block header;
        std::vector<Chunk> chunks;
        std::shared_ptr<CHColumnToArrowColumn> converter;
    
        if (hasTicketPrefix(request.ticket))
        {
            auto info_res = calls_data.tryGetTicketInfo(request.ticket);
            ARROW_RETURN_NOT_OK(info_res);
            auto info = info_res.ValueOrDie();
            chunks.emplace_back(Chunk(info->block->getColumns(), info->block->rows()));
            header = info->block->cloneEmpty();
            converter = info->ch_to_arrow_converter;
        }
        else
        {
            auto sql_res = convertGetDescriptorToSQL(request.ticket);
            ARROW_RETURN_NOT_OK(sql_res);
            auto sql = sql_res.ValueOrDie();

            Session session{global_context, ClientInfo::Interface::ARROW_FLIGHT};

            const auto & auth = AuthMiddleware::get(context);
            session.authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

            auto query_context = session.makeQueryContext();
            query_context.setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
            CurrentThread::QueryScope query_scope{query_context};

            auto block_io = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete).second;
            PullingPipelineExecutor executor{block_io.pipeline};

            Block block;
            while (executor.pull(block))
                chunks.emplace_back(Chunk(block.getColumns(), block.rows()));

            header = executor.getHeader();
            converter = createCHToArrowConverter(header);
        }

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
        auto sql_res = convertGetDescriptorToSQL(descriptor);
        ARROW_RETURN_NOT_OK(sql_res);
        auto sql = sql_res.ValueOrDie();

        Session session{global_context, ClientInfo::Interface::ARROW_FLIGHT};

        const auto & auth = AuthMiddleware::get(context);
        session.authenticate(auth.username(), auth.password(), Poco::Net::SocketAddress{context.peer()});

        auto query_context = session.makeQueryContext();
        query_context.setCurrentQueryId(""); /// Empty string means the query id will be autogenerated.
        CurrentThread::QueryScope query_scope{query_context};

        auto block_io = executeQuery(sql, query_context, QueryFlags{}, QueryProcessingStage::Complete).second;
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


String ArrowFlightHandler::addBlock(BlockInfo && block_info)
{
    String ticket = generateTicket();
    std::lock_guard lock{mutex};
    blocks[ticket] = std::move(block_info);
    return ticket;
}

std::optional<BlockInfo> ArrowFlightHandler::findBlockInfo(const String & ticket) const
{
    std::lock_guard lock{mutex};
    auto it = blocks.find(ticket);
    if (it == blocks.end())
        return std::nullopt;
    return it->second;
}

void ArrowFlightHandler::forgetBlock(const String & ticket) const
{
    std::lock_guard lock{mutex};
    auto it = blocks.find(ticket);
    if (it != blocks.end())
        blocks.erase(it);
}

class ArrowFlightHandler::PollSession
{
public:
    PollSession(QueryExecution && query_execution)
    {

    }

    ~PollSession() = default;

    void addTicket(const String & ticket)
    {
        std::lock_guard lock{poll_session_mutex};
        tickets.emplace_back(ticket);
    }

    Strings getTickets() const
    {
        std::lock_guard lock{poll_session_mutex};
        return tickets;
    }

private:
    std::unique_ptr<Session> session;
    ContextPtr query_context;
    std::optional<CurrentThread::QueryScope> query_scope;
    BlockIO io;
    std::unique_ptr<PullingPipelineExecutor> pulling_executor;
    std::unique_ptr<CHColumnToArrowColumn> ch_to_arrow_converter;
    std::mutex poll_session_mutex;
    Strings tickets TSA_GUARDED_BY(poll_session_mutex);
};

String ArrowFlightHandler::addPollDescriptor(std::shared_ptr<PollSession> poll_session, const String & old_poll_descriptor)
{
    String poll_descriptor = generatePollDescriptor();
    std::mutex lock{mutex};
    if (!old_poll_descriptor.empty())
        poll_sessions.erase(old_poll_descriptor);
    poll_sessions[poll_descriptor] = poll_session;
    return poll_descriptor;
}

std::shared_ptr<PollSession> ArrowFlightHandler::findPollSession(const String & poll_descriptor) const
{
    std::mutex lock{mutex};
    auto it = poll_sessions.find(poll_descriptor);
    if (it == poll_sessions.end())
        return nullptr;
    return it->second;
}

void ArrowFlightHandler::setPollDescriptorInfo(const String & poll_descriptor, PollDescriptorInfo && info)
{
    std::mutex lock{mutex};
    if (poll_sessions.contains(poll_descriptor))
        poll_descriptor_infos[poll_descriptor] = std::move(info);
}

std::optional<PollDescriptorInfo> ArrowFlightHandler::findPollDescriptorInfo(const String & poll_descriptor) const
{
    std::mutex lock{mutex};
    auto it = poll_descriptor_infos.find(poll_descriptor);
    if (it == poll_descriptor_infos.end())
        return std::nullopt;
    return it->second;
}

void ArrowFlightHandler::forgetPollDescriptor(const String & poll_descriptor)
{
    std::lock_guard lock{mutex};
    auto it = poll_sessions.find(poll_descriptor);
    if (it != poll_sessions.end())
        poll_sessions.erase(it);
    auto it2 = poll_descriptor_infos.find(poll_descriptor);
    if (it2 != poll_descriptor_infos.end())
        poll_descriptor_infos.erase(it2);
}

}

#endif
