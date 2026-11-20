#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <Common/Epoll.h>
#include <Common/EventFD.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <base/defines.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

/// Receives the events of one control connection, on the loop thread, one at a time, under the connection's thread group.
/// A callback must not block: the connections of every query wait for it.
class IWorkerControlHandler
{
public:
    virtual ~IWorkerControlHandler() = default;

    virtual void onHelloAccepted(const StatelessWorkerControlProtocol::ControlHelloReply & reply) = 0;
    /// The connection is closed; no callback follows.
    virtual void onHelloRefused(const StatelessWorkerControlProtocol::ControlHelloReply & reply) = 0;
    virtual void onStartTasksReply(const StatelessWorkerControlProtocol::StartTasksReply & reply) = 0;
    /// `dropUnsentStartTasks` dropped the request before any byte of it was sent, so no reply follows.
    virtual void onStartTasksDropped(UInt64 request_id) = 0;
    /// The handler may move from `reply`.
    virtual void onGetStatusesReply(StatelessWorkerControlProtocol::GetStatusesReply & reply) = 0;
    virtual void onCancelTasksReply(UInt64 request_id) = 0;
    virtual void onForgetTasksReply(UInt64 request_id) = 0;
    /// The worker failed the request `error.request_id`, of type `request_type`; the connection stays open.
    virtual void onError(const StatelessWorkerControlProtocol::Error & error, StatelessWorkerControlProtocol::PacketType request_type) = 0;
    /// The connection is lost with its unanswered requests; no callback follows.
    virtual void onBroken(std::exception_ptr exception) = 0;
    /// `close` finished: every request was answered. No callback follows.
    virtual void onClosed() = 0;
};

struct WorkerControlConnectionParams
{
    String unique_query_id;
    String host;
    /// The worker's streaming-exchange port.
    UInt16 port = 0;
    String auth_token;
    /// The interserver credentials, empty for none.
    String user;
    String password;
    /// The initiator's address, for the worker's messages.
    String initiator;
    /// The connection's I/O and callbacks run in it, so they count for its query.
    ThreadGroupPtr thread_group;
};

class WorkerControlConnection;
using WorkerControlConnectionPtr = std::unique_ptr<WorkerControlConnection>;

/// Runs the initiator's control connections to stateless workers on one thread over non-blocking sockets, each read and
/// written at once, so a reply is read while a large `StartTasks` is still being written.
class WorkerControlLoop : private boost::noncopyable
{
public:
    struct Timeouts
    {
        /// The connect and the hello reply together.
        std::chrono::milliseconds hello{10'000};
        std::chrono::milliseconds start_tasks{100'000};
        /// Added to the wait a `GetStatuses` allows the worker.
        std::chrono::milliseconds get_statuses_grace{10'000};
        std::chrono::milliseconds cancel_tasks{5'000};
        std::chrono::milliseconds forget_tasks{10'000};
        std::chrono::milliseconds pong{10'000};
        /// A `Ping` goes out after this long without sending, as the worker closes a connection silent for 30 s.
        std::chrono::milliseconds ping_interval{1'000};
    };

    /// The loop of the server; `Server` shuts it down.
    static WorkerControlLoop & instance();

    WorkerControlLoop();
    explicit WorkerControlLoop(Timeouts timeouts_);
    ~WorkerControlLoop();

    /// Resolves `params.host` and connects in the background; the hello reply comes to `handler`, which must outlive the
    /// connection's callbacks (see `WorkerControlConnection::detach`). The loop must outlive the connection.
    WorkerControlConnectionPtr open(WorkerControlConnectionParams params, IWorkerControlHandler & handler);

    /// Breaks every connection with `ABORTED` and joins the thread; `open` throws afterwards.
    void shutdown();

    /// Exposed for tests.
    [[nodiscard]] UInt64 getTurnCount() const { return turn; }

    /// The state of a connection, shared by the loop and its `WorkerControlConnection`.
    class Connection;
    using ConnectionPtr = std::shared_ptr<Connection>;

private:
    friend class WorkerControlConnection;

    struct OutgoingFrame;
    struct Command;

    template <typename Message>
    static OutgoingFrame makeFrame(const Message & message);

    void post(Command command);

    void run();
    /// Returns false at shutdown.
    bool processCommands();
    void handleEvents(const ConnectionPtr & connection, uint32_t events);
    void startConnecting(const ConnectionPtr & connection, OutgoingFrame hello);
    void completeConnect(Connection & connection, uint32_t events);
    void enqueue(Connection & connection, OutgoingFrame frame);
    void requestClose(Connection & connection);
    void finishCloseIfDone(Connection & connection);
    void receive(Connection & connection);
    void handleFrame(Connection & connection, const StatelessWorkerControlProtocol::PacketHeader & header, std::string_view body);
    void handleHelloReply(Connection & connection, const StatelessWorkerControlProtocol::PacketHeader & header, std::string_view body);
    void dropUnsentStartTasks(Connection & connection);
    /// Sends the queued frames until the socket is full or the turn's budget is spent, then sets the events it waits for.
    void flush(Connection & connection);
    void checkDeadlines();
    void endConnection(Connection & connection, bool broken) noexcept;
    /// Ends the connection and calls `onBroken` unless it ended already.
    void breakConnection(Connection & connection, std::exception_ptr exception) noexcept;
    /// Breaks every connection and those still to be opened with `reason`.
    void breakAll(std::exception_ptr reason) noexcept;
    std::chrono::milliseconds timeoutOf(const OutgoingFrame & frame) const;

    const Timeouts timeouts;
    const LoggerPtr log;

    std::mutex mutex;
    std::vector<Command> commands TSA_GUARDED_BY(mutex);
    bool shutdown_called TSA_GUARDED_BY(mutex) = false;
    std::optional<ThreadFromGlobalPoolNoTracingContextPropagation> thread TSA_GUARDED_BY(mutex);

    /// The rest belongs to the loop thread.
    EventFD wakeup;
    Epoll epoll{EpollNesting::Leaf};
    std::unordered_map<int, ConnectionPtr> connections;
    /// Counts the turns, each of which gives every connection a budget of bytes to read and to write.
    std::atomic<UInt64> turn = 0;
};

/// The owner's handle of a control connection. The requests are serialized on the calling thread, a stage plan of a
/// `StartTasks` without being copied, and sent by the loop in the order of the calls.
class WorkerControlConnection : private boost::noncopyable
{
public:
    WorkerControlConnection(WorkerControlLoop & loop_, std::shared_ptr<WorkerControlLoop::Connection> connection_);
    /// Detaches the handler and aborts the connection unless it was closed.
    ~WorkerControlConnection();

    /// Each request returns its id. Its reply can come before the call returns, so a caller that keeps state for the reply
    /// holds the lock its handler takes across the call. A request on a lost connection is dropped.
    /// A non-zero `request.request_id` comes from `newRequestId`, for a caller that records the request before the call.
    UInt64 startTasks(StatelessWorkerControlProtocol::StartTasks request);
    UInt64 getStatuses(UInt64 wait_milliseconds, Strings task_ids);
    UInt64 cancelTasks(Strings task_ids);
    UInt64 forgetTasks(Strings task_ids);

    /// Closes the connection once every request is answered, then calls `onClosed`. No request may follow.
    void close();
    /// Detaches the handler and closes the connection at once.
    void abort();
    /// Drops the queued `StartTasks` of which no byte was sent, so that the requests after them go out at once.
    void dropUnsentStartTasks();
    /// Returns the id of a new request; throws after `close` or `abort`.
    UInt64 newRequestId();
    /// Stops the callbacks: once it returns, none runs. It waits for a running one, so the caller must not hold a lock that
    /// the handler takes; it can be called from a callback.
    void detach();

    /// `host:port` of the worker.
    [[nodiscard]] const String & getDescription() const;

private:
    void post(WorkerControlLoop::OutgoingFrame frame);

    WorkerControlLoop & loop;
    const std::shared_ptr<WorkerControlLoop::Connection> connection;
};

}

#endif
