#include <Server/StatelessWorker/StatelessWorkerControlServer.h>
#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Core/Defines.h>
#include <Core/ProtocolDefines.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/PipeFDs.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <fmt/ranges.h>

#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <optional>

namespace CurrentMetrics
{
    extern const Metric StatelessWorkerControlSessions;
    extern const Metric StatelessWorkerControlSessionsActive;
    extern const Metric StatelessWorkerControlSessionsScheduled;
}

namespace ProfileEvents
{
    extern const Event StatelessWorkerControlReclaimedTasks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int AUTHENTICATION_FAILED;
    extern const int CANNOT_POLL;
    extern const int INCORRECT_DATA;
    extern const int SOCKET_TIMEOUT;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

namespace FailPoints
{
    extern const char stateless_worker_pause_before_start_task[];
}

using namespace StatelessWorkerControlProtocol;

namespace
{

using Clock = std::chrono::steady_clock;

constexpr size_t SESSION_OUTPUT_BUFFER_BYTES = 64 * 1024;

/// Returns the message of the in-flight exception without a stack trace, as a failed task reports it.
String currentErrorMessage()
{
    return getCurrentExceptionMessage(/*with_stacktrace=*/ false, /*check_embedded_stacktrace=*/ false, /*with_extra_info=*/ false, /*with_version=*/ false);
}

ControlHelloReply makeHelloReply(Int32 code, String message)
{
    return ControlHelloReply{
        .version = PROTOCOL_VERSION,
        .code = code,
        .message = std::move(message),
        .task_serialization_version = DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION,
        .plan_serialization_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION,
    };
}

/// Waits until one of `fds` is readable; false at `deadline`.
bool pollUntil(pollfd * fds, nfds_t count, Clock::time_point deadline)
{
    while (true)
    {
        const auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
        const int timeout = static_cast<int>(std::clamp<Int64>(left, 0, std::numeric_limits<int>::max()));
        const int ready = ::poll(fds, count, timeout);
        if (ready >= 0)
            return ready > 0;
        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot poll a stateless worker control connection");
    }
}

/// Wakes a session's `poll`. Shared with the session's tasks, which can end after the session.
class SessionWakeup final : public StatelessTaskExecutor::StatusChangeNotifier
{
public:
    SessionWakeup() { pipe.setNonBlockingReadWrite(); }

    void notify() override
    {
        const char byte = 0;
        /// A full pipe holds a wakeup already.
        ssize_t written = 0;
        do
        {
            written = ::write(pipe.fds_rw[1], &byte, 1);
        } while (written < 0 && errno == EINTR);
    }

    [[nodiscard]] int fd() const { return pipe.fds_rw[0]; }

    void drain() const
    {
        char bytes[256];
        ssize_t received = 0;
        do
        {
            received = ::read(pipe.fds_rw[0], bytes, sizeof(bytes));
        } while (received > 0 || (received < 0 && errno == EINTR));
    }

private:
    PipeFDs pipe;
};

}

class StatelessWorkerControlServer::Session
{
public:
    Session(StatelessWorkerControlServer & server_, const Poco::Net::StreamSocket & socket_, const ControlHello & hello);
    ~Session();

    /// Serves the connection until it closes, then reclaims the session's tasks and ends its admission.
    void run() noexcept;
    void wake() const { wakeup->notify(); }

private:
    void serve();
    /// Waits for input or a wakeup; false when the socket has nothing to read.
    bool waitForInput(Clock::time_point deadline);
    void handleFrame(const PacketHeader & header);
    void startTasks(const PacketHeader & header);
    void getStatuses(const PacketHeader & header);
    void answerParkedStatuses();
    void checkTaskIds(const Strings & task_ids) const;
    /// Returns true when the error was sent.
    bool trySendFatalError(int code, const String & message) noexcept;
    void reclaimTasks();

    template <typename Message>
    void send(const Message & message)
    {
        can_send = false;
        writeFrame(message, out);
        out.next();
        can_send = true;
    }

    StatelessWorkerControlServer & server;
    StatelessTaskExecutor & executor;
    Poco::Net::StreamSocket socket;
    const String peer;
    const String unique_query_id;
    const Purpose purpose;
    const String initiator;
    const String task_id_prefix;
    const std::shared_ptr<SessionWakeup> wakeup = std::make_shared<SessionWakeup>();
    ReadBufferFromPocoSocket in;
    WriteBufferFromPocoSocket out;
    /// False once a send failed, as the connection may then end inside a frame.
    bool can_send = true;
    /// The tasks this session started and the initiator has not forgotten.
    std::unordered_set<String> started_tasks;

    struct ParkedStatuses
    {
        UInt64 request_id = 0;
        Strings task_ids;
        StatelessTaskExecutor::StatusWait wait;
        Clock::time_point deadline;
    };
    /// A `GetStatuses` held until one of its tasks changes or its wait ends.
    std::optional<ParkedStatuses> parked;
    const LoggerPtr log;
};

StatelessWorkerControlServer::Session::Session(StatelessWorkerControlServer & server_, const Poco::Net::StreamSocket & socket_, const ControlHello & hello)
    : server(server_)
    , executor(*server_.executor)
    , socket(socket_)
    , peer(StreamingExchangeProtocol::describePeer(socket))
    , unique_query_id(hello.unique_query_id)
    , purpose(hello.purpose)
    , initiator(hello.initiator)
    , task_id_prefix(makeUniqueTaskId(unique_query_id, ""))
    /// `StartTasks` bodies carry plans of tens of MiB, which a 64 KiB buffer reads 1.7 times slower.
    , in(socket, DBMS_DEFAULT_BUFFER_SIZE)
    , out(socket, SESSION_OUTPUT_BUFFER_BYTES)
    , log(server_.log)
{
}

StatelessWorkerControlServer::Session::~Session()
{
    out.cancel();
}

void StatelessWorkerControlServer::Session::run() noexcept
{
    setThreadName(ThreadName::STATELESS_WORKER_CONTROL_SESSION);
    bool linger = false;
    try
    {
        serve();
    }
    catch (...)
    {
        const String message = currentErrorMessage();
        LOG_INFO(log, "Closing the control connection of {} from {} ({}): {}", unique_query_id, initiator, peer, message);
        linger = trySendFatalError(getCurrentExceptionCode(), message);
    }

    try
    {
        if (linger)
            lingeringClose(socket);
        else
            socket.close();
        reclaimTasks();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to reclaim the tasks of {} after its control connection closed", unique_query_id));
    }

    if (purpose == Purpose::Query)
    {
        try
        {
            executor.releaseQuery(unique_query_id);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to release the query {}", unique_query_id));
        }
    }
}

void StatelessWorkerControlServer::Session::serve()
{
    const Poco::Timespan io_timeout(std::chrono::duration_cast<std::chrono::microseconds>(server.limits.silence_timeout).count());
    socket.setReceiveTimeout(io_timeout);
    socket.setSendTimeout(io_timeout);
    /// Replies are small frames, often two back to back: with Nagle the second waits for the initiator's delayed ACK (40 ms).
    socket.setNoDelay(true);

    send(makeHelloReply(0, ""));
    LOG_DEBUG(log, "Opened the control connection of {} from {} ({})", unique_query_id, initiator, peer);

    auto silence_deadline = Clock::now() + server.limits.silence_timeout;
    while (true)
    {
        if (server.shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "The stateless worker is shutting down");

        bool readable = in.hasPendingData();
        if (!readable)
            readable = waitForInput(parked ? std::min(parked->deadline, silence_deadline) : silence_deadline);

        if (parked && (Clock::now() >= parked->deadline || parked->wait.changed()))
            answerParkedStatuses();

        if (!readable)
        {
            if (Clock::now() >= silence_deadline)
                throw Exception(ErrorCodes::SOCKET_TIMEOUT, "No frame from the initiator for {} ms", server.limits.silence_timeout.count());
            continue;
        }

        if (in.eof())
        {
            LOG_DEBUG(log, "The initiator {} ({}) closed the control connection of {}", initiator, peer, unique_query_id);
            return;
        }
        const PacketHeader header = readHeader(in, Sender::Initiator);
        silence_deadline = Clock::now() + server.limits.silence_timeout;
        handleFrame(header);
    }
}

bool StatelessWorkerControlServer::Session::waitForInput(Clock::time_point deadline)
{
    pollfd fds[2] = {
        {.fd = socket.sockfd(), .events = POLLIN, .revents = 0},
        {.fd = wakeup->fd(), .events = POLLIN, .revents = 0},
    };
    pollUntil(fds, 2, deadline);
    if (fds[1].revents)
        wakeup->drain();
    return fds[0].revents != 0;
}

void StatelessWorkerControlServer::Session::handleFrame(const PacketHeader & header)
{
    const auto type = static_cast<PacketType>(header.packet_type);
    if (purpose == Purpose::Probe && type != PacketType::Ping)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet {} on a probe control connection", packetTypeName(header.packet_type));

    switch (type)
    {
        case PacketType::StartTasks:
            startTasks(header);
            return;
        case PacketType::GetStatuses:
            getStatuses(header);
            return;
        case PacketType::CancelTasks:
        {
            CancelTasks request;
            readBody(header, in, request);
            checkTaskIds(request.task_ids);
            executor.cancelTasks(request.task_ids);
            send(CancelTasksReply{.request_id = request.request_id});
            return;
        }
        case PacketType::ForgetTasks:
        {
            ForgetTasks request;
            readBody(header, in, request);
            checkTaskIds(request.task_ids);
            executor.forgetTasks(request.task_ids);
            for (const auto & task_id : request.task_ids)
                started_tasks.erase(task_id);
            send(ForgetTasksReply{.request_id = request.request_id});
            return;
        }
        case PacketType::Ping:
        {
            Ping request;
            readBody(header, in, request);
            send(Pong{.request_id = request.request_id});
            return;
        }
        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet {} on the control connection of {}",
                packetTypeName(header.packet_type), unique_query_id);
    }
}

void StatelessWorkerControlServer::Session::startTasks(const PacketHeader & header)
{
    StartTasks request;
    readBody(header, in, request);

    Strings unique_task_ids;
    for (const auto & stage : request.batch.stages)
        for (const auto & batch_task : stage.tasks)
            unique_task_ids.push_back(makeUniqueTaskId(unique_query_id, batch_task.task.task_id));

    FailPointInjection::pauseFailPoint(FailPoints::stateless_worker_pause_before_start_task);

    std::optional<OpenTelemetry::TracingContextHolder> span;
    if (request.trace_context.isTraceEnabled())
    {
        span.emplace("StatelessWorkerControlServer::startTasks", request.trace_context, Context::getGlobalContextInstance()->getOpenTelemetrySpanLog());
        span->root_span.kind = OpenTelemetry::SpanKind::SERVER;
    }

    /// The tasks registered before a failure keep running, so they are the session's either way.
    started_tasks.insert(unique_task_ids.begin(), unique_task_ids.end());
    bool started = false;
    try
    {
        started = executor.startTasks(unique_query_id, std::move(request.batch), request.temp_path, request.collectors, wakeup);
    }
    catch (...)
    {
        const String message = currentErrorMessage();
        LOG_WARNING(log, "Failed to start the tasks of {}: {}", unique_query_id, message);
        if (span)
            span->root_span.addAttribute(std::current_exception());
        send(Error{.request_id = request.request_id, .code = getCurrentExceptionCode(), .message = message});
        return;
    }

    if (started)
        send(StartTasksReply{.request_id = request.request_id, .code = 0, .message = ""});
    else
        send(StartTasksReply{
            .request_id = request.request_id,
            .code = ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
            .message = "The stateless worker runs `stateless_worker_server.max_queries` queries already"});
}

void StatelessWorkerControlServer::Session::getStatuses(const PacketHeader & header)
{
    GetStatuses request;
    readBody(header, in, request);
    checkTaskIds(request.task_ids);

    if (parked)
        answerParkedStatuses();

    StatelessTaskExecutor::StatusWait wait = executor.beginStatusWait(request.task_ids);
    if (request.wait_milliseconds == 0 || wait.changed())
    {
        send(GetStatusesReply{.request_id = request.request_id, .entries = makeTaskStatusEntries(request.task_ids, wait.collect())});
        return;
    }

    /// Bounded by the silence timeout, so a huge wait cannot overflow the deadline.
    const std::chrono::milliseconds wait_time(std::min<UInt64>(request.wait_milliseconds, server.limits.silence_timeout.count()));
    parked = ParkedStatuses{
        .request_id = request.request_id,
        .task_ids = std::move(request.task_ids),
        .wait = std::move(wait),
        .deadline = Clock::now() + wait_time,
    };
}

void StatelessWorkerControlServer::Session::answerParkedStatuses()
{
    const ParkedStatuses request = std::move(*parked);
    parked.reset();
    send(GetStatusesReply{.request_id = request.request_id, .entries = makeTaskStatusEntries(request.task_ids, request.wait.collect())});
}

void StatelessWorkerControlServer::Session::checkTaskIds(const Strings & task_ids) const
{
    for (const auto & task_id : task_ids)
        if (!task_id.starts_with(task_id_prefix))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Task {} is not a task of the query {} of the control connection", task_id, unique_query_id);
}

bool StatelessWorkerControlServer::Session::trySendFatalError(int code, const String & message) noexcept
{
    if (!can_send)
        return false;
    try
    {
        send(Error{.request_id = 0, .code = code, .message = message});
        return true;
    }
    catch (...)
    {
        LOG_DEBUG(log, "Cannot send the error to {}: {}", peer, getCurrentExceptionMessage(false));
        return false;
    }
}

void StatelessWorkerControlServer::Session::reclaimTasks()
{
    if (started_tasks.empty())
        return;

    Strings running;
    {
        const Strings task_ids(started_tasks.begin(), started_tasks.end());
        const auto statuses = executor.beginStatusWait(task_ids).collect();
        for (size_t i = 0; i < task_ids.size(); ++i)
            if (statuses[i].result != StatelessTaskExecutor::UnknownTaskId)
                running.push_back(task_ids[i]);
    }
    started_tasks.clear();
    if (running.empty())
        return;

    ProfileEvents::increment(ProfileEvents::StatelessWorkerControlReclaimedTasks, running.size());
    LOG_INFO(log, "Cancelling {} tasks of {} that the initiator {} did not forget before its control connection closed",
        running.size(), unique_query_id, initiator);
    executor.cancelTasks(running);

    const auto deadline = Clock::now() + server.limits.reclaim_timeout;
    Strings ended;
    while (true)
    {
        const auto statuses = executor.beginStatusWait(running).collect();
        Strings still_running;
        for (size_t i = 0; i < running.size(); ++i)
        {
            const auto result = statuses[i].result;
            if (result == StatelessTaskExecutor::TaskRunnig || result == StatelessTaskExecutor::TaskFailing)
                still_running.push_back(std::move(running[i]));
            else
                ended.push_back(std::move(running[i]));
        }
        running = std::move(still_running);
        if (running.empty() || Clock::now() >= deadline)
            break;

        pollfd wakeup_fd{.fd = wakeup->fd(), .events = POLLIN, .revents = 0};
        if (pollUntil(&wakeup_fd, 1, deadline))
            wakeup->drain();
    }

    executor.forgetTasks(ended);
    if (running.empty())
        return;
    executor.forgetTasksWhenEnded(running);
    LOG_WARNING(log, "{} tasks of {} still run {} ms after they were cancelled for their closed control connection, so they keep the "
        "query's slot until they end: {}", running.size(), unique_query_id, server.limits.reclaim_timeout.count(), fmt::join(running, ", "));
}

StatelessWorkerControlServer::StatelessWorkerControlServer(Limits limits_, CredentialsGetter get_credentials_)
    : limits(limits_)
    , get_credentials(std::move(get_credentials_))
    , executor(std::make_unique<StatelessTaskExecutor>(limits.max_queries))
    , log(getLogger("StatelessWorkerControlServer"))
    /// Unlimited queue: `max_sessions` bounds the sessions, and a new one may wait for the thread of one that is ending.
    , session_pool(
        CurrentMetrics::StatelessWorkerControlSessions,
        CurrentMetrics::StatelessWorkerControlSessionsActive,
        CurrentMetrics::StatelessWorkerControlSessionsScheduled,
        limits.max_sessions, /*max_free_threads=*/ 0, /*queue_size=*/ 0, /*shutdown_on_exception=*/ false)
{
}

StatelessWorkerControlServer::~StatelessWorkerControlServer()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void StatelessWorkerControlServer::handleControlConnection(
    Poco::Net::StreamSocket socket, const StreamingExchangeProtocol::PacketHeader & header, const ExchangeConnectionAuthenticator & authenticate_token)
{
    const String peer = StreamingExchangeProtocol::describePeer(socket);
    String unique_query_id;
    try
    {
        const ControlHello hello = receiveHello(socket, header);
        unique_query_id = hello.unique_query_id;
        authenticate(hello, authenticate_token);
        startSession(socket, hello);
        return;
    }
    catch (...)
    {
        const String message = currentErrorMessage();
        LOG_INFO(log, "Refused the control connection of {} from {}: {}", unique_query_id, peer, message);
        try
        {
            WriteBufferFromPocoSocket reply(socket);
            writeFrame(makeHelloReply(getCurrentExceptionCode(), message), reply);
            reply.finalize();
        }
        catch (...)
        {
            LOG_DEBUG(log, "Cannot send the refusal to {}: {}", peer, getCurrentExceptionMessage(false));
        }
    }
    lingeringClose(socket);
}

ControlHello StatelessWorkerControlServer::receiveHello(Poco::Net::StreamSocket & socket, const StreamingExchangeProtocol::PacketHeader & header) const
{
    checkHeader(header, Sender::Initiator);
    if (header.packet_type != static_cast<UInt64>(PacketType::ControlHello))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "A control connection must open with `ControlHello`, not {}",
            packetTypeName(header.packet_type));

    String body(header.bytes_size, '\0');
    if (!body.empty())
    {
        /// Sized to the body, so it reads nothing that follows the hello.
        ReadBufferFromPocoSocket hello_in(socket, body.size());
        hello_in.setHandshakeTimeout(StreamingExchangeProtocol::HELLO_TIMEOUT_SECONDS * 1000);
        hello_in.readStrict(body.data(), body.size());
        hello_in.clearHandshakeTimeout();
    }
    return parseBody<ControlHello>(header, body);
}

void StatelessWorkerControlServer::authenticate(const ControlHello & hello, const ExchangeConnectionAuthenticator & authenticate_token) const
{
    if (authenticate_token)
        authenticate_token(hello.auth_token);

    /// As `InterserverIOHTTPHandler::checkAuthentication`, an empty user standing for no HTTP credentials.
    const InterserverCredentialsPtr credentials = get_credentials();
    if (credentials)
    {
        const auto [message, valid] = hello.user.empty() ? credentials->isValidUser("", "") : credentials->isValidUser(hello.user, hello.password);
        if (!valid)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "The control connection of {} failed to authenticate: {}", hello.unique_query_id, message);
    }
    else if (!hello.user.empty())
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "The control connection of {} presents interserver credentials, but this server has none", hello.unique_query_id);
    }
}

void StatelessWorkerControlServer::startSession(const Poco::Net::StreamSocket & socket, const ControlHello & hello)
{
    const bool is_query = hello.purpose == Purpose::Query;
    if (is_query)
    {
        if (hello.unique_query_id.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "The control connection names no query");
        if (!executor->admitQuery(hello.unique_query_id))
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                "The stateless worker runs `stateless_worker_server.max_queries` = {} queries already", limits.max_queries);
    }

    try
    {
        auto session = std::make_shared<Session>(*this, socket, hello);
        {
            std::lock_guard lock(sessions_mutex);
            if (shutdown_called)
                throw Exception(ErrorCodes::ABORTED, "The stateless worker is shutting down");
            if (sessions.size() >= limits.max_sessions)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                    "The stateless worker serves `stateless_worker_server.max_control_sessions` = {} control connections already", limits.max_sessions);
            sessions.insert(session);
        }
        if (!session_pool.trySchedule([this, session] { session->run(); removeSession(session); }))
        {
            removeSession(session);
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Cannot start a thread for the control connection of {}", hello.unique_query_id);
        }
    }
    catch (...)
    {
        if (is_query)
            executor->releaseQuery(hello.unique_query_id);
        throw;
    }
}

void StatelessWorkerControlServer::removeSession(const SessionPtr & session)
{
    {
        std::lock_guard lock(sessions_mutex);
        sessions.erase(session);
    }
    sessions_changed.notify_all();
}

void StatelessWorkerControlServer::shutdown()
{
    std::vector<SessionPtr> running;
    {
        std::lock_guard lock(sessions_mutex);
        shutdown_called = true;
        running.assign(sessions.begin(), sessions.end());
    }
    LOG_DEBUG(log, "Shutting down: closing {} control connections", running.size());
    for (const auto & session : running)
        session->wake();
    running.clear();

    executor->shutdown();

    {
        std::unique_lock lock(sessions_mutex);
        sessions_changed.wait(lock, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS { return sessions.empty(); });
    }
    session_pool.wait();
}

bool StatelessWorkerControlServer::waitForSessions(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock lock(sessions_mutex);
    return sessions_changed.wait_until(lock, deadline, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS { return sessions.empty(); });
}

}
