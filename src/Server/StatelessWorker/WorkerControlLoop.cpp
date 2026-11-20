#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/StatelessWorker/WorkerControlLoop.h>
#include <IO/WriteBufferFromString.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Poco/Net/StreamSocket.h>

#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <map>
#include <type_traits>
#include <utility>

namespace ProfileEvents
{
    extern const Event DistributedPlanControlConnects;
    extern const Event DistributedPlanControlPings;
    extern const Event DistributedPlanControlSendBytes;
    extern const Event DistributedPlanControlReceiveBytes;
}

namespace CurrentMetrics
{
    extern const Metric DistributedPlanControlConnections;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace FailPoints
{
    extern const char worker_control_loop_break_connection_fails[];
    extern const char worker_control_loop_open_fails_after_post[];
    extern const char worker_control_loop_break_connections[];
}

using namespace StatelessWorkerControlProtocol;

namespace
{

using Clock = std::chrono::steady_clock;

constexpr auto TICK = std::chrono::milliseconds(100);
/// Per connection, direction and turn, so that a large frame does not hold up the other connections.
constexpr size_t MAX_BYTES_PER_TURN = 1024 * 1024;
constexpr int MAX_EVENTS = 64;

#if defined(MSG_NOSIGNAL)
constexpr int SEND_FLAGS = MSG_NOSIGNAL;
#else
/// Poco sets `SO_NOSIGPIPE` on the socket instead.
constexpr int SEND_FLAGS = 0;
#endif

int socketErrorCode(int socket_errno)
{
    return socket_errno == ETIMEDOUT ? ErrorCodes::SOCKET_TIMEOUT : ErrorCodes::NETWORK_ERROR;
}

/// Returns `exception` as the loss of the control connection to `description`.
std::exception_ptr makeLostConnectionException(const String & description, std::exception_ptr exception)
{
    int code = 0;
    String message;
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const Exception & e)
    {
        code = e.code();
        message = e.message();
    }
    catch (const Poco::Exception & e)
    {
        code = ErrorCodes::NETWORK_ERROR;
        message = e.displayText();
    }
    catch (...)
    {
        code = getCurrentExceptionCode();
        message = getCurrentExceptionMessage(/*with_stacktrace=*/ false);
    }
    return std::make_exception_ptr(Exception(code, "Lost the control connection to stateless worker {}: {}", description, message));
}

}

struct WorkerControlLoop::OutgoingFrame
{
    PacketType type = PacketType::Ping;
    UInt64 request_id = 0;
    /// How long the worker may hold a `GetStatuses`.
    std::chrono::milliseconds wait{0};
    /// The header, then the body without the stage plans.
    String bytes;
    /// Each plan goes out whole at its offset in `bytes`.
    std::vector<std::pair<size_t, std::shared_ptr<const String>>> plans;

    [[nodiscard]] size_t segmentCount() const { return 2 * plans.size() + 1; }

    [[nodiscard]] std::string_view segment(size_t index) const
    {
        if (index % 2 == 1)
            return *plans[index / 2].second;
        const size_t begin = index == 0 ? 0 : plans[index / 2 - 1].first;
        const size_t end = index / 2 < plans.size() ? plans[index / 2].first : bytes.size();
        return std::string_view(bytes).substr(begin, end - begin);
    }
};

struct WorkerControlLoop::Command
{
    enum class Kind : uint8_t
    {
        Open,
        Send,
        Close,
        Abort,
    };

    ConnectionPtr connection;
    Kind kind = Kind::Send;
    OutgoingFrame frame;
};

class WorkerControlLoop::Connection
{
public:
    enum class State : uint8_t
    {
        Connecting,
        HelloSent,
        Ready,
        /// `close` was called: waits for the replies.
        Closing,
        Closed,
        Broken,
    };

    struct PendingRequest
    {
        PacketType type;
        Clock::time_point deadline;
        std::chrono::milliseconds timeout;
    };

    Connection(WorkerControlConnectionParams params_, Poco::Net::SocketAddress address_, IWorkerControlHandler & handler_)
        : params(std::move(params_))
        , address(std::move(address_))
        , description(fmt::format("{}:{}", params.host, params.port))
        , handler(&handler_)
    {
    }

    /// Calls `callback` with the handler unless it is detached.
    template <typename Callback>
    void notify(Callback && callback)
    {
        std::lock_guard lock(handler_mutex);
        if (handler)
            callback(*handler);
    }

    void detach()
    {
        std::lock_guard lock(handler_mutex);
        handler = nullptr;
    }

    bool isOpen() const { return state != State::Closed && state != State::Broken; }
    bool isAccepted() const { return state == State::Ready || state == State::Closing; }

    const WorkerControlConnectionParams params;
    const Poco::Net::SocketAddress address;
    const String description;
    std::atomic<UInt64> next_request_id{1};
    /// Set by `close` and `abort`, after which no request may come.
    std::atomic<bool> closing{false};

    /// The rest belongs to the loop thread.
    State state = State::Connecting;
    Poco::Net::StreamSocket socket;
    int fd = -1;
    uint32_t registered_events = 0;
    std::optional<CurrentMetrics::Increment> open_metric;
    Clock::time_point hello_deadline;
    Clock::time_point last_send;
    bool close_requested = false;
    std::map<UInt64, PendingRequest> requests;
    /// The requests that came before the hello was accepted.
    std::deque<OutgoingFrame> held;
    std::deque<OutgoingFrame> write_queue;
    /// The position in the front frame of `write_queue`.
    size_t write_segment = 0;
    size_t write_offset = 0;
    PacketHeader read_header{};
    size_t header_bytes_read = 0;
    bool reading_body = false;
    String read_body;
    size_t body_bytes_read = 0;
    UInt64 budget_turn = 0;
    size_t read_budget = 0;
    size_t write_budget = 0;

private:
    /// Recursive, so that a callback can detach its own handler.
    std::recursive_mutex handler_mutex;
    IWorkerControlHandler * handler;
};

namespace
{

using Connection = WorkerControlLoop::Connection;
using State = Connection::State;

void refreshBudgets(Connection & connection, UInt64 turn)
{
    if (connection.budget_turn == turn)
        return;
    connection.budget_turn = turn;
    connection.read_budget = MAX_BYTES_PER_TURN;
    connection.write_budget = MAX_BYTES_PER_TURN;
}

/// Drops the written segments and frames at the front of the write queue.
void skipWritten(Connection & connection)
{
    while (!connection.write_queue.empty())
    {
        const auto & frame = connection.write_queue.front();
        if (connection.write_segment < frame.segmentCount())
        {
            if (connection.write_offset < frame.segment(connection.write_segment).size())
                return;
            ++connection.write_segment;
            connection.write_offset = 0;
            continue;
        }
        connection.write_queue.pop_front();
        connection.write_segment = 0;
    }
}

/// Takes the request a reply of `type` answers; `expected` is `std::nullopt` for an `Error`, which answers any.
Connection::PendingRequest takeRequest(Connection & connection, UInt64 request_id, PacketType type, std::optional<PacketType> expected)
{
    auto it = connection.requests.find(request_id);
    if (it == connection.requests.end() || (expected && it->second.type != *expected))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "the worker sent {} for request {}, which {}",
            packetTypeName(static_cast<UInt64>(type)), request_id,
            it == connection.requests.end() ? "is not waiting for a reply" : "is a " + packetTypeName(static_cast<UInt64>(it->second.type)));
    auto request = it->second;
    connection.requests.erase(it);
    return request;
}

}

template <typename Message>
WorkerControlLoop::OutgoingFrame WorkerControlLoop::makeFrame(const Message & message)
{
    OutgoingFrame frame;
    frame.type = Message::type;
    if constexpr (requires { message.request_id; })
        frame.request_id = message.request_id;
    if constexpr (std::is_same_v<Message, GetStatuses>)
        frame.wait = std::chrono::milliseconds(message.wait_milliseconds);

    size_t plan_bytes = 0;
    WriteBufferFromString out(frame.bytes);
    const PacketHeader placeholder{};
    out.write(reinterpret_cast<const char *>(&placeholder), sizeof(placeholder));
    if constexpr (std::is_same_v<Message, StartTasks>)
    {
        message.write(out, [&](const std::shared_ptr<const String> & plan)
        {
            frame.plans.emplace_back(out.count(), plan);
            plan_bytes += plan->size();
        });
    }
    else
    {
        message.write(out);
    }
    out.finalize();

    const PacketHeader header = makeHeader(Message::type, frame.bytes.size() - sizeof(PacketHeader) + plan_bytes);
    memcpy(frame.bytes.data(), &header, sizeof(header));
    return frame;
}

WorkerControlLoop & WorkerControlLoop::instance()
{
    static WorkerControlLoop loop;
    return loop;
}

WorkerControlLoop::WorkerControlLoop()
    : WorkerControlLoop(Timeouts{})
{
}

WorkerControlLoop::WorkerControlLoop(Timeouts timeouts_)
    : timeouts(timeouts_)
    , log(getLogger("WorkerControlLoop"))
{
    epoll.add(wakeup.fd);
}

WorkerControlLoop::~WorkerControlLoop()
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

WorkerControlConnectionPtr WorkerControlLoop::open(WorkerControlConnectionParams params, IWorkerControlHandler & handler)
{
    const Poco::Net::SocketAddress address = DNSResolver::instance().resolveAddress(params.host, params.port);
    const ControlHello hello{
        .version = PROTOCOL_VERSION,
        .purpose = Purpose::Query,
        .unique_query_id = params.unique_query_id,
        .auth_token = params.auth_token,
        .user = params.user,
        .password = params.password,
        .initiator = params.initiator,
    };
    auto connection = std::make_shared<Connection>(std::move(params), address, handler);
    Command command{.connection = connection, .kind = Command::Kind::Open, .frame = makeFrame(hello)};
    /// Before the post: once posted, the connection is detached and ended only by its handle, also when this throws.
    auto handle = std::make_unique<WorkerControlConnection>(*this, connection);

    bool wake = false;
    {
        std::lock_guard lock(mutex);
        if (shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "The stateless worker control loop is shut down");
        if (!thread)
            thread.emplace([this] { run(); });
        wake = commands.empty();
        commands.push_back(std::move(command));
    }
    if (wake)
        wakeup.write();
    fiu_do_on(FailPoints::worker_control_loop_open_fails_after_post,
        { throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after posting a control connection"); });

    ProfileEvents::increment(ProfileEvents::DistributedPlanControlConnects);
    return handle;
}

void WorkerControlLoop::shutdown()
{
    std::optional<ThreadFromGlobalPoolNoTracingContextPropagation> stopped;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        stopped.swap(thread);
    }
    if (!stopped)
        return;
    wakeup.write();
    stopped->join();
}

void WorkerControlLoop::post(Command command)
{
    bool wake = false;
    {
        std::lock_guard lock(mutex);
        if (shutdown_called)
            return;
        wake = commands.empty();
        commands.push_back(std::move(command));
    }
    if (wake)
        wakeup.write();
}

void WorkerControlLoop::run()
{
    setThreadName(ThreadName::WORKER_CONTROL_LOOP);
    std::array<epoll_event, MAX_EVENTS> events{};
    auto next_tick = Clock::now() + TICK;
    while (true)
    {
        /// An error ends only the turn: the connections keep their state, and the next turn serves them again.
        try
        {
            const auto left = std::chrono::ceil<std::chrono::milliseconds>(next_tick - Clock::now()).count();
            const size_t ready = epoll.getManyReady(MAX_EVENTS, events.data(), static_cast<int>(std::clamp<Int64>(left, 0, TICK.count())));
            ++turn;

            for (size_t i = 0; i < ready; ++i)
            {
                if (events[i].data.fd == wakeup.fd)
                {
                    wakeup.read();
                    continue;
                }
                /// A copy: the connection can end and leave the map while it is handled.
                auto it = connections.find(events[i].data.fd);
                if (it == connections.end())
                    continue;
                const ConnectionPtr connection = it->second;
                handleEvents(connection, events[i].events);
            }

            if (!processCommands())
                break;

            if (Clock::now() >= next_tick)
            {
                next_tick = Clock::now() + TICK;
                checkDeadlines();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "A turn of the stateless worker control loop failed");
        }
    }
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    breakAll(std::make_exception_ptr(Exception(ErrorCodes::ABORTED, "the server is shutting down")));
}

bool WorkerControlLoop::processCommands()
{
    std::vector<Command> taken;
    {
        std::lock_guard lock(mutex);
        if (shutdown_called)
            return false;
        taken.swap(commands);
    }

    for (auto & command : taken)
    {
        Connection & connection = *command.connection;
        ThreadGroupSwitcher switcher(connection.params.thread_group, ThreadName::WORKER_CONTROL_LOOP);
        try
        {
            switch (command.kind)
            {
                case Command::Kind::Open:
                    startConnecting(command.connection, std::move(command.frame));
                    break;
                case Command::Kind::Send:
                    enqueue(connection, std::move(command.frame));
                    break;
                case Command::Kind::Close:
                    requestClose(connection);
                    break;
                case Command::Kind::Abort:
                    if (connection.isOpen())
                    {
                        LOG_TRACE(log, "Aborted the control connection of {} to {}", connection.params.unique_query_id, connection.description);
                        endConnection(connection, /*broken=*/ false);
                    }
                    break;
            }
            if (connection.isOpen())
                flush(connection);
        }
        catch (...)
        {
            breakConnection(connection, std::current_exception());
        }
    }
    return true;
}

void WorkerControlLoop::handleEvents(const ConnectionPtr & connection, uint32_t events)
{
    ThreadGroupSwitcher switcher(connection->params.thread_group, ThreadName::WORKER_CONTROL_LOOP);
    try
    {
        /// `Epoll` registers `EPOLLPRI`, which stays raised until the urgent byte is read; a worker sends none.
        if (events & EPOLLPRI)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "the worker sent TCP urgent data");
        refreshBudgets(*connection, turn);
        if (connection->state == State::Connecting)
            completeConnect(*connection, events);
        if (connection->state != State::Connecting && (events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
            receive(*connection);
        if (connection->isOpen())
            flush(*connection);
    }
    catch (...)
    {
        breakConnection(*connection, std::current_exception());
    }
}

void WorkerControlLoop::startConnecting(const ConnectionPtr & connection, OutgoingFrame hello)
{
    connection->socket.connectNB(connection->address);
    connection->fd = connection->socket.impl()->sockfd();
    connection->open_metric.emplace(CurrentMetrics::DistributedPlanControlConnections);
    connection->socket.setNoDelay(true);
    connection->hello_deadline = Clock::now() + timeouts.hello;
    connection->write_queue.push_back(std::move(hello));
    epoll.add(connection->fd, EPOLLIN | EPOLLOUT | EPOLLERR);
    connection->registered_events = EPOLLIN | EPOLLOUT | EPOLLERR;
    connections.emplace(connection->fd, connection);
}

void WorkerControlLoop::completeConnect(Connection & connection, uint32_t events)
{
    if (const int error = connection.socket.impl()->socketError())
        ErrnoException::throwWithErrno(socketErrorCode(error), error, "cannot connect to {}", connection.address.toString());
    if (!(events & EPOLLOUT))
        return;
    connection.state = State::HelloSent;
    LOG_TRACE(log, "Connected the control connection of {} to {}", connection.params.unique_query_id, connection.description);
}

std::chrono::milliseconds WorkerControlLoop::timeoutOf(const OutgoingFrame & frame) const
{
    switch (frame.type)
    {
        case PacketType::StartTasks:
            return timeouts.start_tasks;
        case PacketType::GetStatuses:
            return frame.wait + timeouts.get_statuses_grace;
        case PacketType::CancelTasks:
            return timeouts.cancel_tasks;
        case PacketType::ForgetTasks:
            return timeouts.forget_tasks;
        case PacketType::Ping:
            return timeouts.pong;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected request {} on a control connection",
                packetTypeName(static_cast<UInt64>(frame.type)));
    }
}

void WorkerControlLoop::enqueue(Connection & connection, OutgoingFrame frame)
{
    if (!connection.isOpen())
        return;
    if (!connection.isAccepted())
    {
        connection.held.push_back(std::move(frame));
        return;
    }
    const auto timeout = timeoutOf(frame);
    connection.requests.emplace(
        frame.request_id, Connection::PendingRequest{.type = frame.type, .deadline = Clock::now() + timeout, .timeout = timeout});
    connection.write_queue.push_back(std::move(frame));
}

void WorkerControlLoop::requestClose(Connection & connection)
{
    connection.close_requested = true;
    if (connection.state == State::Ready)
        connection.state = State::Closing;
    finishCloseIfDone(connection);
}

void WorkerControlLoop::finishCloseIfDone(Connection & connection)
{
    if (connection.state != State::Closing || !connection.requests.empty() || !connection.write_queue.empty())
        return;
    LOG_TRACE(log, "Closed the control connection of {} to {}", connection.params.unique_query_id, connection.description);
    endConnection(connection, /*broken=*/ false);
    connection.notify([](IWorkerControlHandler & handler) { handler.onClosed(); });
}

void WorkerControlLoop::receive(Connection & connection)
{
    while (connection.isOpen() && connection.read_budget > 0)
    {
        char * target = nullptr;
        size_t size = 0;
        if (connection.reading_body)
        {
            target = connection.read_body.data() + connection.body_bytes_read;
            size = connection.read_body.size() - connection.body_bytes_read;
        }
        else
        {
            target = reinterpret_cast<char *>(&connection.read_header) + connection.header_bytes_read;
            size = sizeof(PacketHeader) - connection.header_bytes_read;
        }

        const ssize_t received = ::recv(connection.fd, target, std::min(size, connection.read_budget), 0);
        if (received == 0)
            throw Exception(ErrorCodes::NETWORK_ERROR, "{}", connection.state == State::HelloSent
                ? "the worker closed the connection during the control handshake" : "the worker closed the connection");
        if (received < 0)
        {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            ErrnoException::throwWithErrno(socketErrorCode(errno), errno, "cannot receive");
        }
        const auto received_bytes = static_cast<size_t>(received);
        ProfileEvents::increment(ProfileEvents::DistributedPlanControlReceiveBytes, received_bytes);
        connection.read_budget -= received_bytes;

        if (connection.reading_body)
        {
            connection.body_bytes_read += received_bytes;
        }
        else
        {
            connection.header_bytes_read += received_bytes;
            if (connection.header_bytes_read == sizeof(PacketHeader))
            {
                checkHeader(connection.read_header, Sender::Worker);
                connection.read_body.resize(connection.read_header.bytes_size);
                connection.body_bytes_read = 0;
                connection.reading_body = true;
            }
        }

        if (connection.reading_body && connection.body_bytes_read == connection.read_body.size())
        {
            connection.reading_body = false;
            connection.header_bytes_read = 0;
            const PacketHeader header = connection.read_header;
            const String body = std::exchange(connection.read_body, {});
            handleFrame(connection, header, body);
        }
    }
}

void WorkerControlLoop::handleFrame(Connection & connection, const PacketHeader & header, std::string_view body)
{
    if (!connection.isAccepted())
    {
        handleHelloReply(connection, header, body);
        return;
    }

    const auto type = static_cast<PacketType>(header.packet_type);
    switch (type)
    {
        case PacketType::StartTasksReply:
        {
            const auto reply = parseBody<StartTasksReply>(header, body);
            takeRequest(connection, reply.request_id, type, PacketType::StartTasks);
            connection.notify([&](IWorkerControlHandler & handler) { handler.onStartTasksReply(reply); });
            break;
        }
        case PacketType::GetStatusesReply:
        {
            auto reply = parseBody<GetStatusesReply>(header, body);
            takeRequest(connection, reply.request_id, type, PacketType::GetStatuses);
            connection.notify([&](IWorkerControlHandler & handler) { handler.onGetStatusesReply(reply); });
            break;
        }
        case PacketType::CancelTasksReply:
        {
            const auto reply = parseBody<CancelTasksReply>(header, body);
            takeRequest(connection, reply.request_id, type, PacketType::CancelTasks);
            connection.notify([&](IWorkerControlHandler & handler) { handler.onCancelTasksReply(reply.request_id); });
            break;
        }
        case PacketType::ForgetTasksReply:
        {
            const auto reply = parseBody<ForgetTasksReply>(header, body);
            takeRequest(connection, reply.request_id, type, PacketType::ForgetTasks);
            connection.notify([&](IWorkerControlHandler & handler) { handler.onForgetTasksReply(reply.request_id); });
            break;
        }
        case PacketType::Pong:
        {
            const auto reply = parseBody<Pong>(header, body);
            takeRequest(connection, reply.request_id, type, PacketType::Ping);
            break;
        }
        case PacketType::Error:
        {
            const auto error = parseBody<Error>(header, body);
            if (error.request_id == 0)
                throw Exception(error.code, "the worker closed it: {}", error.message);
            const auto request = takeRequest(connection, error.request_id, type, std::nullopt);
            /// A ping is the loop's own request, which the error answers as a `Pong` would.
            if (request.type == PacketType::Ping)
                LOG_WARNING(log, "Stateless worker {} answered a ping of the control connection of {} with an error: {}",
                    connection.description, connection.params.unique_query_id, error.message);
            else
                connection.notify([&](IWorkerControlHandler & handler) { handler.onError(error, request.type); });
            break;
        }
        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "unexpected packet {} from the worker",
                packetTypeName(header.packet_type));
    }
    finishCloseIfDone(connection);
}

void WorkerControlLoop::handleHelloReply(Connection & connection, const PacketHeader & header, std::string_view body)
{
    /// A streaming-exchange server that serves no control connections answers with an `Error`.
    if (header.packet_type == static_cast<UInt64>(PacketType::Error))
    {
        const auto error = parseBody<Error>(header, body);
        throw Exception(error.code, "the worker refused it: {}", error.message);
    }

    const auto reply = parseBody<ControlHelloReply>(header, body);
    if (reply.code != 0)
    {
        LOG_DEBUG(log, "Stateless worker {} refused the control connection of {}: {}",
            connection.description, connection.params.unique_query_id, reply.message);
        endConnection(connection, /*broken=*/ false);
        connection.notify([&](IWorkerControlHandler & handler) { handler.onHelloRefused(reply); });
        return;
    }

    LOG_TRACE(log, "Stateless worker {} accepted the control connection of {}", connection.description, connection.params.unique_query_id);
    connection.state = connection.close_requested ? State::Closing : State::Ready;
    connection.last_send = Clock::now();
    auto held = std::exchange(connection.held, {});
    for (auto & frame : held)
        enqueue(connection, std::move(frame));
    connection.notify([&](IWorkerControlHandler & handler) { handler.onHelloAccepted(reply); });
    finishCloseIfDone(connection);
}

void WorkerControlLoop::flush(Connection & connection)
{
    refreshBudgets(connection, turn);
    if (connection.state != State::Connecting)
    {
        skipWritten(connection);
        while (connection.write_budget > 0 && !connection.write_queue.empty())
        {
            const std::string_view segment
                = connection.write_queue.front().segment(connection.write_segment).substr(connection.write_offset);
            const ssize_t sent = ::send(connection.fd, segment.data(), std::min(segment.size(), connection.write_budget), SEND_FLAGS);
            if (sent < 0)
            {
                if (errno == EINTR)
                    continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                ErrnoException::throwWithErrno(socketErrorCode(errno), errno, "cannot send");
            }
            const auto sent_bytes = static_cast<size_t>(sent);
            ProfileEvents::increment(ProfileEvents::DistributedPlanControlSendBytes, sent_bytes);
            connection.write_budget -= sent_bytes;
            connection.write_offset += sent_bytes;
            connection.last_send = Clock::now();
            skipWritten(connection);
        }
        finishCloseIfDone(connection);
        if (!connection.isOpen())
            return;
    }

    const uint32_t events = EPOLLIN | EPOLLERR | ((connection.state == State::Connecting || !connection.write_queue.empty()) ? EPOLLOUT : 0);
    if (events != connection.registered_events)
    {
        epoll.modify(connection.fd, events);
        connection.registered_events = events;
    }
}

void WorkerControlLoop::checkDeadlines()
{
    const auto now = Clock::now();
    bool break_all = false;
    fiu_do_on(FailPoints::worker_control_loop_break_connections, { break_all = true; });
    const auto is_due = [&](const Connection & connection)
    {
        if (break_all)
            return true;
        if (!connection.isAccepted())
            return now >= connection.hello_deadline;
        if (connection.write_queue.empty() && now - connection.last_send >= timeouts.ping_interval)
            return true;
        return std::ranges::any_of(connection.requests, [&](const auto & request) { return now >= request.second.deadline; });
    };

    std::vector<ConnectionPtr> due;
    for (const auto & [fd, connection] : connections)
        if (is_due(*connection))
            due.push_back(connection);

    for (const auto & connection : due)
    {
        ThreadGroupSwitcher switcher(connection->params.thread_group, ThreadName::WORKER_CONTROL_LOOP);
        try
        {
            if (break_all)
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected loss of the connection");
            if (connection->state == State::Connecting && now >= connection->hello_deadline)
                throw Exception(ErrorCodes::SOCKET_TIMEOUT, "cannot connect within {} ms", timeouts.hello.count());
            if (connection->state == State::HelloSent && now >= connection->hello_deadline)
                throw Exception(ErrorCodes::SOCKET_TIMEOUT, "no reply to the hello within {} ms", timeouts.hello.count());
            for (const auto & [request_id, request] : connection->requests)
                if (now >= request.deadline)
                    throw Exception(ErrorCodes::SOCKET_TIMEOUT, "no reply to {} (request {}) within {} ms",
                        packetTypeName(static_cast<UInt64>(request.type)), request_id, request.timeout.count());

            if (connection->isAccepted() && connection->write_queue.empty() && now - connection->last_send >= timeouts.ping_interval)
            {
                enqueue(*connection, makeFrame(Ping{.request_id = connection->next_request_id++}));
                ProfileEvents::increment(ProfileEvents::DistributedPlanControlPings);
                flush(*connection);
            }
        }
        catch (...)
        {
            breakConnection(*connection, std::current_exception());
        }
    }
}

void WorkerControlLoop::endConnection(Connection & connection, bool broken) noexcept
{
    if (connection.registered_events != 0)
    {
        try
        {
            epoll.remove(connection.fd);
        }
        catch (...)
        {
            /// Closing the socket below removes it from the epoll too.
            tryLogCurrentException(log, "Cannot remove a control connection from the epoll");
        }
        connection.registered_events = 0;
    }
    if (auto it = connections.find(connection.fd); it != connections.end() && it->second.get() == &connection)
        connections.erase(it);
    connection.socket.close();
    connection.fd = -1;
    connection.open_metric.reset();
    connection.state = broken ? State::Broken : State::Closed;
    connection.requests.clear();
    connection.held.clear();
    connection.write_queue.clear();
    connection.read_body = {};
}

void WorkerControlLoop::breakConnection(Connection & connection, std::exception_ptr exception) noexcept
{
    /// `exception` is often the query's memory limit, which must not keep the connection from ending.
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    const bool was_open = connection.isOpen();
    if (was_open)
        endConnection(connection, /*broken=*/ true);

    std::exception_ptr lost = exception;
    try
    {
        fiu_do_on(FailPoints::worker_control_loop_break_connection_fails,
            { throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while breaking a control connection"); });
        lost = makeLostConnectionException(connection.description, exception);
        if (was_open)
            LOG_DEBUG(log, "{} (query {})", getExceptionMessage(lost, /*with_stacktrace=*/ false), connection.params.unique_query_id);
        else
            /// A callback that ends the connection threw.
            LOG_WARNING(log, "{}", getExceptionMessage(lost, /*with_stacktrace=*/ false));
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot describe a lost control connection");
    }

    if (!was_open)
        return;
    try
    {
        connection.notify([&](IWorkerControlHandler & handler) { handler.onBroken(lost); });
    }
    catch (...)
    {
        tryLogCurrentException(log, "The handler of a lost control connection failed");
    }
}

void WorkerControlLoop::breakAll(std::exception_ptr reason) noexcept
{
    std::vector<Command> pending;
    {
        std::lock_guard lock(mutex);
        pending.swap(commands);
    }

    for (const auto & command : pending)
    {
        if (command.kind != Command::Kind::Open)
            continue;
        ThreadGroupSwitcher switcher(command.connection->params.thread_group, ThreadName::WORKER_CONTROL_LOOP);
        breakConnection(*command.connection, reason);
    }
    while (!connections.empty())
    {
        const ConnectionPtr connection = connections.begin()->second;
        connections.erase(connections.begin());
        ThreadGroupSwitcher switcher(connection->params.thread_group, ThreadName::WORKER_CONTROL_LOOP);
        breakConnection(*connection, reason);
    }
}

WorkerControlConnection::WorkerControlConnection(WorkerControlLoop & loop_, std::shared_ptr<WorkerControlLoop::Connection> connection_)
    : loop(loop_)
    , connection(std::move(connection_))
{
}

WorkerControlConnection::~WorkerControlConnection()
{
    try
    {
        detach();
        if (!connection->closing.exchange(true))
            loop.post(WorkerControlLoop::Command{.connection = connection, .kind = WorkerControlLoop::Command::Kind::Abort, .frame = {}});
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

UInt64 WorkerControlConnection::newRequestId()
{
    if (connection->closing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A request on the control connection to stateless worker {} after it was closed",
            connection->description);
    return connection->next_request_id++;
}

void WorkerControlConnection::post(WorkerControlLoop::OutgoingFrame frame)
{
    loop.post(WorkerControlLoop::Command{.connection = connection, .kind = WorkerControlLoop::Command::Kind::Send, .frame = std::move(frame)});
}

UInt64 WorkerControlConnection::startTasks(StartTasks request)
{
    if (request.request_id == 0)
        request.request_id = newRequestId();
    post(WorkerControlLoop::makeFrame(request));
    return request.request_id;
}

UInt64 WorkerControlConnection::getStatuses(UInt64 wait_milliseconds, Strings task_ids)
{
    const GetStatuses request{.request_id = newRequestId(), .wait_milliseconds = wait_milliseconds, .task_ids = std::move(task_ids)};
    post(WorkerControlLoop::makeFrame(request));
    return request.request_id;
}

UInt64 WorkerControlConnection::cancelTasks(Strings task_ids)
{
    const CancelTasks request{.request_id = newRequestId(), .task_ids = std::move(task_ids)};
    post(WorkerControlLoop::makeFrame(request));
    return request.request_id;
}

UInt64 WorkerControlConnection::forgetTasks(Strings task_ids)
{
    const ForgetTasks request{.request_id = newRequestId(), .task_ids = std::move(task_ids)};
    post(WorkerControlLoop::makeFrame(request));
    return request.request_id;
}

void WorkerControlConnection::close()
{
    if (connection->closing.exchange(true))
        return;
    loop.post(WorkerControlLoop::Command{.connection = connection, .kind = WorkerControlLoop::Command::Kind::Close, .frame = {}});
}

void WorkerControlConnection::abort()
{
    detach();
    connection->closing = true;
    loop.post(WorkerControlLoop::Command{.connection = connection, .kind = WorkerControlLoop::Command::Kind::Abort, .frame = {}});
}

void WorkerControlConnection::detach()
{
    connection->detach();
}

const String & WorkerControlConnection::getDescription() const
{
    return connection->description;
}

}

#endif
