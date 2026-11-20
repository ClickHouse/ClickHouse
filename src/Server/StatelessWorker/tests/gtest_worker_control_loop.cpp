#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <Server/StatelessWorker/StatelessWorkerControlServer.h>
#include <Server/StatelessWorker/WorkerControlLoop.h>
#include <Interpreters/InterserverCredentials.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>

namespace ProfileEvents
{
    extern const Event DistributedPlanControlPings;
    extern const Event DistributedPlanControlSendBytes;
}

namespace DB::FailPoints
{
    extern const char worker_control_loop_break_connection_fails[];
    extern const char worker_control_loop_open_fails_after_post[];
}

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int AUTHENTICATION_FAILED;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

using namespace DB;
using namespace DB::StatelessWorkerControlProtocol;
using namespace std::chrono_literals;

namespace
{

struct Event
{
    enum class Kind : uint8_t
    {
        HelloAccepted,
        HelloRefused,
        StartTasksReply,
        GetStatusesReply,
        CancelTasksReply,
        ForgetTasksReply,
        Error,
        Broken,
        Closed,
    };

    Kind kind = Kind::Closed;
    UInt64 request_id = 0;
    int code = 0;
    String message = {};
    Strings statuses = {};
};

String kindName(Event::Kind kind)
{
    switch (kind)
    {
        case Event::Kind::HelloAccepted: return "HelloAccepted";
        case Event::Kind::HelloRefused: return "HelloRefused";
        case Event::Kind::StartTasksReply: return "StartTasksReply";
        case Event::Kind::GetStatusesReply: return "GetStatusesReply";
        case Event::Kind::CancelTasksReply: return "CancelTasksReply";
        case Event::Kind::ForgetTasksReply: return "ForgetTasksReply";
        case Event::Kind::Error: return "Error";
        case Event::Kind::Broken: return "Broken";
        case Event::Kind::Closed: return "Closed";
    }
    return "?";
}

/// Records the callbacks, for the test to wait on.
class RecordingHandler final : public IWorkerControlHandler
{
public:
    void onHelloAccepted(const ControlHelloReply & reply) override
    {
        push({.kind = Event::Kind::HelloAccepted, .code = reply.code, .message = reply.message});
    }

    void onHelloRefused(const ControlHelloReply & reply) override
    {
        push({.kind = Event::Kind::HelloRefused, .code = reply.code, .message = reply.message});
    }

    void onStartTasksReply(const StartTasksReply & reply) override
    {
        push({.kind = Event::Kind::StartTasksReply, .request_id = reply.request_id, .code = reply.code, .message = reply.message});
    }

    void onGetStatusesReply(GetStatusesReply & reply) override
    {
        Event event{.kind = Event::Kind::GetStatusesReply, .request_id = reply.request_id};
        for (const auto & entry : reply.entries)
            event.statuses.push_back(entry.task_id + ": " + entry.status.status);
        push(std::move(event));
    }

    void onCancelTasksReply(UInt64 request_id) override { push({.kind = Event::Kind::CancelTasksReply, .request_id = request_id}); }
    void onForgetTasksReply(UInt64 request_id) override { push({.kind = Event::Kind::ForgetTasksReply, .request_id = request_id}); }

    void onError(const Error & error) override
    {
        push({.kind = Event::Kind::Error, .request_id = error.request_id, .code = error.code, .message = error.message});
    }

    void onBroken(std::exception_ptr exception) override
    {
        push({.kind = Event::Kind::Broken, .code = getExceptionErrorCode(exception), .message = getExceptionMessage(exception, false)});
    }

    void onClosed() override { push({.kind = Event::Kind::Closed}); }

    /// The next event, none within `timeout`.
    std::optional<Event> next(std::chrono::milliseconds timeout = 10s)
    {
        std::unique_lock lock(mutex);
        if (!changed.wait_for(lock, timeout, [this] { return !events.empty(); }))
            return std::nullopt;
        Event event = std::move(events.front());
        events.pop_front();
        return event;
    }

    /// The next event, which must be of `kind`.
    Event expect(Event::Kind kind, std::chrono::milliseconds timeout = 10s)
    {
        auto event = next(timeout);
        if (!event)
        {
            ADD_FAILURE() << "No " << kindName(kind) << " within " << timeout.count() << " ms";
            return Event{.kind = kind};
        }
        EXPECT_EQ(kindName(event->kind), kindName(kind)) << event->message;
        return *event;
    }

private:
    void push(Event event)
    {
        {
            std::lock_guard lock(mutex);
            events.push_back(std::move(event));
        }
        changed.notify_all();
    }

    std::mutex mutex;
    std::condition_variable changed;
    std::deque<Event> events;
};

std::shared_ptr<StatelessWorkerControlServer> makeControlServer(
    size_t max_queries, InterserverCredentialsPtr credentials = nullptr, std::chrono::milliseconds silence_timeout = 30s)
{
    const StatelessWorkerControlServer::Limits limits{.max_queries = max_queries, .max_sessions = 4 * max_queries, .silence_timeout = silence_timeout};
    return std::make_shared<StatelessWorkerControlServer>(limits, [credentials] { return credentials; });
}

/// A streaming-exchange server on a free loopback port that hands control connections to `control_server`.
class Worker
{
public:
    explicit Worker(ExchangeControlConnectionHandlerPtr control_server)
        : exchange_server("127.0.0.1", 0, 64, std::make_shared<ExchangeConnections>(), {}, std::move(control_server))
    {
        exchange_server.start();
    }

    UInt16 port() const { return exchange_server.port(); }

private:
    ExchangeServer exchange_server;
};

/// One connection to `FakeWorker`, served by the test with blocking reads bounded by a timeout.
class FakePeer
{
public:
    explicit FakePeer(const Poco::Net::StreamSocket & socket_)
        : socket(socket_)
        , in(socket)
        , out(socket)
    {
        socket.setReceiveTimeout(Poco::Timespan(10, 0));
        socket.setSendTimeout(Poco::Timespan(10, 0));
    }

    ~FakePeer() { out.cancel(); }

    template <typename Message>
    void send(const Message & message)
    {
        writeFrame(message, out);
        out.next();
    }

    template <typename Message>
    Message receive()
    {
        const PacketHeader header = readHeader(in, Sender::Initiator);
        Message message;
        readBody(header, in, message);
        return message;
    }

    void acceptHello()
    {
        receive<ControlHello>();
        send(ControlHelloReply{});
    }

    bool closedByInitiator() { return in.eof(); }

    void setReceiveTimeout(std::chrono::seconds timeout) { socket.setReceiveTimeout(Poco::Timespan(timeout.count(), 0)); }

    void close()
    {
        out.cancel();
        socket.close();
    }

private:
    Poco::Net::StreamSocket socket;
    ReadBufferFromPocoSocket in;
    WriteBufferFromPocoSocket out;
};

/// A loopback listener whose connections the test serves by hand.
class FakeWorker
{
public:
    explicit FakeWorker(int receive_buffer_bytes = 0)
    {
        listener.bind(Poco::Net::SocketAddress("127.0.0.1", 0), /*reuseAddress=*/ true);
        /// Before `listen`, so the accepted sockets take it.
        if (receive_buffer_bytes)
            listener.setReceiveBufferSize(receive_buffer_bytes);
        listener.listen();
    }

    [[nodiscard]] UInt16 port() const { return listener.address().port(); }

    std::unique_ptr<FakePeer> accept()
    {
        auto peer = tryAccept(10s);
        if (!peer)
            throw std::runtime_error("No connection to the fake worker within 10 s");
        return peer;
    }

    /// Null when no connection comes within `timeout`.
    std::unique_ptr<FakePeer> tryAccept(std::chrono::seconds timeout)
    {
        if (!listener.poll(Poco::Timespan(timeout.count(), 0), Poco::Net::Socket::SELECT_READ))
            return nullptr;
        return std::make_unique<FakePeer>(listener.acceptConnection());
    }

private:
    Poco::Net::ServerSocket listener;
};

WorkerControlConnectionParams makeParams(UInt16 port, const String & unique_query_id, const String & user = "", const String & password = "")
{
    return WorkerControlConnectionParams{
        .unique_query_id = unique_query_id,
        .host = "127.0.0.1",
        .port = port,
        .auth_token = "",
        .user = user,
        .password = password,
        .initiator = "gtest",
        .thread_group = nullptr,
    };
}

/// No pings, so that a fake worker sees only the frames of the test.
WorkerControlLoop::Timeouts withoutPings()
{
    return WorkerControlLoop::Timeouts{.ping_interval = 600s};
}

UInt64 globalCount(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event];
}

}

TEST(WorkerControlLoop, HelloAndRequests)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);

    /// Held until the hello is accepted.
    const UInt64 statuses_id = connection->getStatuses(0, {"q1::a", "q1::b"});
    EXPECT_EQ(handler.expect(Event::Kind::HelloAccepted).code, 0);
    const Event statuses = handler.expect(Event::Kind::GetStatusesReply);
    EXPECT_EQ(statuses.request_id, statuses_id);
    EXPECT_EQ(statuses.statuses, (Strings{"q1::a: Unknown task", "q1::b: Unknown task"}));

    const UInt64 cancel_id = connection->cancelTasks({"q1::a"});
    EXPECT_EQ(handler.expect(Event::Kind::CancelTasksReply).request_id, cancel_id);

    /// The close waits for the reply.
    const UInt64 forget_id = connection->forgetTasks({"q1::a"});
    connection->close();
    EXPECT_EQ(handler.expect(Event::Kind::ForgetTasksReply).request_id, forget_id);
    handler.expect(Event::Kind::Closed);
    EXPECT_TRUE(control_server->waitForSessions(std::chrono::steady_clock::now() + 10s));
}

TEST(WorkerControlLoop, RefusedHellos)
{
    auto credentials = std::make_shared<InterserverCredentials>(
        "admin", SecretString("secret"), InterserverCredentials::CurrentCredentials{{"admin", SecretString("secret")}});
    auto control_server = makeControlServer(/*max_queries=*/ 1, credentials);
    Worker worker(control_server);
    WorkerControlLoop loop;

    RecordingHandler first;
    auto first_connection = loop.open(makeParams(worker.port(), "q1", "admin", "secret"), first);
    EXPECT_EQ(first.expect(Event::Kind::HelloAccepted).code, 0);

    RecordingHandler other_query;
    auto other_connection = loop.open(makeParams(worker.port(), "q2", "admin", "secret"), other_query);
    Event refused = other_query.expect(Event::Kind::HelloRefused);
    EXPECT_EQ(refused.code, ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
    EXPECT_TRUE(refused.message.contains("stateless_worker_server.max_queries")) << refused.message;

    RecordingHandler wrong_password;
    auto wrong_connection = loop.open(makeParams(worker.port(), "q1", "admin", "wrong"), wrong_password);
    refused = wrong_password.expect(Event::Kind::HelloRefused);
    EXPECT_EQ(refused.code, ErrorCodes::AUTHENTICATION_FAILED);
    EXPECT_TRUE(refused.message.contains("Incorrect user or password")) << refused.message;

    /// A request on a refused connection is dropped.
    other_connection->getStatuses(0, {"q2::a"});
    EXPECT_FALSE(other_query.next(200ms));
}

TEST(WorkerControlLoop, ParkedStatusesAreAnsweredAtTheirDeadline)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    handler.expect(Event::Kind::HelloAccepted);

    /// Without tasks nothing changes, so the worker answers when the wait ends.
    const Stopwatch watch;
    const UInt64 request_id = connection->getStatuses(300, {});
    const Event reply = handler.expect(Event::Kind::GetStatusesReply);
    EXPECT_EQ(reply.request_id, request_id);
    EXPECT_GE(watch.elapsedMilliseconds(), 290u);
}

TEST(WorkerControlLoop, PingsKeepTheConnectionAlive)
{
    auto control_server = makeControlServer(10, nullptr, /*silence_timeout=*/ 500ms);
    Worker worker(control_server);
    WorkerControlLoop loop(WorkerControlLoop::Timeouts{.ping_interval = 100ms});
    RecordingHandler handler;
    const UInt64 pings_before = globalCount(ProfileEvents::DistributedPlanControlPings);
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    handler.expect(Event::Kind::HelloAccepted);

    /// Three silence timeouts of the worker.
    EXPECT_FALSE(handler.next(1500ms));
    EXPECT_GT(globalCount(ProfileEvents::DistributedPlanControlPings), pings_before);
    connection->getStatuses(0, {"q1::a"});
    handler.expect(Event::Kind::GetStatusesReply);
}

TEST(WorkerControlLoop, SilentConnectionIsClosedByTheWorker)
{
    auto control_server = makeControlServer(10, nullptr, /*silence_timeout=*/ 300ms);
    Worker worker(control_server);
    WorkerControlLoop loop(withoutPings());
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    handler.expect(Event::Kind::HelloAccepted);

    const Event broken = handler.expect(Event::Kind::Broken);
    EXPECT_EQ(broken.code, ErrorCodes::SOCKET_TIMEOUT);
    const String expected = fmt::format("Lost the control connection to stateless worker 127.0.0.1:{}: the worker closed it", worker.port());
    EXPECT_TRUE(broken.message.contains(expected)) << broken.message;
}

TEST(WorkerControlLoop, WorkerShutdownBreaksTheConnection)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    WorkerControlLoop loop;
    RecordingHandler handler;
    RecordingHandler detached;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    auto detached_connection = loop.open(makeParams(worker.port(), "q1"), detached);
    handler.expect(Event::Kind::HelloAccepted);
    detached.expect(Event::Kind::HelloAccepted);
    detached_connection->detach();

    control_server->shutdown();
    EXPECT_EQ(handler.expect(Event::Kind::Broken).code, ErrorCodes::ABORTED);
    EXPECT_FALSE(detached.next(300ms));
}

TEST(WorkerControlLoop, DestroyedHandleClosesTheConnection)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    handler.expect(Event::Kind::HelloAccepted);

    connection.reset();
    EXPECT_TRUE(control_server->waitForSessions(std::chrono::steady_clock::now() + 10s));
    EXPECT_FALSE(handler.next(100ms));
}

TEST(WorkerControlLoop, ExchangePortWithoutControlServer)
{
    Worker worker(nullptr);
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);

    const Event broken = handler.expect(Event::Kind::Broken);
    EXPECT_EQ(broken.code, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    EXPECT_TRUE(broken.message.contains("stateless_worker_server.enabled")) << broken.message;
}

TEST(WorkerControlLoop, PeerThatClosesBreaksTheConnection)
{
    FakeWorker fake_worker;
    WorkerControlLoop loop(withoutPings());
    {
        RecordingHandler handler;
        auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
        auto peer = fake_worker.accept();
        peer->receive<ControlHello>();
        peer->close();
        const Event broken = handler.expect(Event::Kind::Broken);
        EXPECT_EQ(broken.code, ErrorCodes::NETWORK_ERROR);
        EXPECT_TRUE(broken.message.contains("during the control handshake")) << broken.message;
    }
    {
        RecordingHandler handler;
        auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
        auto peer = fake_worker.accept();
        peer->acceptHello();
        handler.expect(Event::Kind::HelloAccepted);
        peer->close();
        const Event broken = handler.expect(Event::Kind::Broken);
        EXPECT_EQ(broken.code, ErrorCodes::NETWORK_ERROR);
        EXPECT_TRUE(broken.message.contains("the worker closed the connection")) << broken.message;
    }
}

TEST(WorkerControlLoop, RefusedConnect)
{
    UInt16 port = 0;
    {
        Poco::Net::ServerSocket unused(Poco::Net::SocketAddress("127.0.0.1", 0));
        port = unused.address().port();
    }
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(port, "q1"), handler);
    const Event broken = handler.expect(Event::Kind::Broken);
    EXPECT_EQ(broken.code, ErrorCodes::NETWORK_ERROR);
    EXPECT_TRUE(broken.message.contains("cannot connect")) << broken.message;
}

TEST(WorkerControlLoop, UnansweredRequestTimesOut)
{
    FakeWorker fake_worker;
    WorkerControlLoop loop(WorkerControlLoop::Timeouts{.cancel_tasks = 200ms, .ping_interval = 600s});
    RecordingHandler handler;
    auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
    auto peer = fake_worker.accept();
    peer->acceptHello();
    handler.expect(Event::Kind::HelloAccepted);

    connection->cancelTasks({"q1::a"});
    peer->receive<CancelTasks>();
    const Event broken = handler.expect(Event::Kind::Broken);
    EXPECT_EQ(broken.code, ErrorCodes::SOCKET_TIMEOUT);
    EXPECT_TRUE(broken.message.contains("no reply to CancelTasks")) << broken.message;
    EXPECT_TRUE(peer->closedByInitiator());
}

TEST(WorkerControlLoop, ErrorAndUnexpectedReplies)
{
    FakeWorker fake_worker;
    WorkerControlLoop loop(withoutPings());
    RecordingHandler handler;
    auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
    auto peer = fake_worker.accept();
    peer->acceptHello();
    handler.expect(Event::Kind::HelloAccepted);

    const UInt64 request_id = connection->getStatuses(0, {"q1::a"});
    EXPECT_EQ(peer->receive<GetStatuses>().request_id, request_id);
    peer->send(Error{.request_id = request_id, .code = ErrorCodes::ABORTED, .message = "busy"});
    const Event error = handler.expect(Event::Kind::Error);
    EXPECT_EQ(error.request_id, request_id);
    EXPECT_EQ(error.code, ErrorCodes::ABORTED);
    EXPECT_EQ(error.message, "busy");

    /// Its request is answered already.
    peer->send(GetStatusesReply{.request_id = request_id, .entries = {}});
    EXPECT_EQ(handler.expect(Event::Kind::Broken).code, ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

TEST(WorkerControlLoop, LargeStartTasksIsWrittenWhileRepliesAreRead)
{
    /// Far more than the socket buffers hold, so the write stays blocked until the peer reads.
    const auto plan = std::make_shared<const String>(32 * 1024 * 1024, 'p');
    FakeWorker fake_worker(/*receive_buffer_bytes=*/ 64 * 1024);
    WorkerControlLoop loop(withoutPings());
    RecordingHandler handler;
    auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
    auto peer = fake_worker.accept();
    peer->acceptHello();
    handler.expect(Event::Kind::HelloAccepted);

    StartTasks request;
    request.temp_path = "tmp/q1";
    DistributedQueryTaskBatch::Task task;
    task.task.task_id = "main";
    request.batch.initial_query_id = "q1";
    request.batch.stages.push_back({"main", plan, {task}});

    const UInt64 sent_before = globalCount(ProfileEvents::DistributedPlanControlSendBytes);
    const UInt64 statuses_id = connection->getStatuses(0, {"q1::main"});
    const UInt64 start_id = connection->startTasks(request);

    EXPECT_EQ(peer->receive<GetStatuses>().request_id, statuses_id);
    peer->send(GetStatusesReply{.request_id = statuses_id, .entries = {}});
    EXPECT_EQ(handler.expect(Event::Kind::GetStatusesReply).request_id, statuses_id);
    EXPECT_LT(globalCount(ProfileEvents::DistributedPlanControlSendBytes) - sent_before, plan->size());

    const auto received = peer->receive<StartTasks>();
    EXPECT_EQ(received.request_id, start_id);
    ASSERT_EQ(received.batch.stages.size(), 1u);
    EXPECT_TRUE(*received.batch.stages[0].serialized_query_plan == *plan);
    peer->send(StartTasksReply{.request_id = start_id, .code = 0, .message = ""});
    EXPECT_EQ(handler.expect(Event::Kind::StartTasksReply).request_id, start_id);

    connection->close();
    handler.expect(Event::Kind::Closed);
    EXPECT_TRUE(peer->closedByInitiator());
}

TEST(WorkerControlLoop, FailureWhileBreakingAConnectionKeepsTheLoop)
{
    FakeWorker fake_worker;
    WorkerControlLoop loop(withoutPings());
    FailPointInjection::enableFailPoint(FailPoints::worker_control_loop_break_connection_fails);
    SCOPE_EXIT(FailPointInjection::disableFailPoint(FailPoints::worker_control_loop_break_connection_fails));

    RecordingHandler handler;
    auto connection = loop.open(makeParams(fake_worker.port(), "q1"), handler);
    auto peer = fake_worker.accept();
    peer->acceptHello();
    handler.expect(Event::Kind::HelloAccepted);
    peer->close();
    handler.expect(Event::Kind::Broken);
    EXPECT_FALSE(handler.next(300ms));

    /// The loop still serves the connections of other queries.
    RecordingHandler next_handler;
    WorkerControlConnectionPtr next_connection;
    ASSERT_NO_THROW(next_connection = loop.open(makeParams(fake_worker.port(), "q2"), next_handler));
    auto next_peer = fake_worker.accept();
    next_peer->acceptHello();
    next_handler.expect(Event::Kind::HelloAccepted);
}

TEST(WorkerControlLoop, OpenThatFailsLeavesNoConnection)
{
    FakeWorker fake_worker;
    WorkerControlLoop loop(withoutPings());
    FailPointInjection::enableFailPoint(FailPoints::worker_control_loop_open_fails_after_post);
    SCOPE_EXIT(FailPointInjection::disableFailPoint(FailPoints::worker_control_loop_open_fails_after_post));

    /// The handler belongs to the failed `open`, so no callback may reach it.
    RecordingHandler handler;
    EXPECT_THROW(loop.open(makeParams(fake_worker.port(), "q1"), handler), Exception);

    /// What the loop connected before the failure is closed.
    if (auto peer = fake_worker.tryAccept(1s))
    {
        peer->setReceiveTimeout(2s);
        const auto closed_by_initiator = [&]
        {
            try
            {
                return peer->closedByInitiator();
            }
            catch (const Exception & e)
            {
                /// A reset closes it too; a timeout leaves it open.
                return e.code() == ErrorCodes::NETWORK_ERROR;
            }
        };
        bool closed = closed_by_initiator();
        if (!closed)
        {
            /// The hello can go out before the connection is aborted.
            peer->receive<ControlHello>();
            closed = closed_by_initiator();
        }
        if (!closed)
            peer->send(ControlHelloReply{});
        EXPECT_TRUE(closed) << "The initiator kept a control connection that the failed `open` did not hand over";
    }
    EXPECT_FALSE(handler.next(500ms));
}

TEST(WorkerControlLoop, ShutdownBreaksTheConnections)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    WorkerControlLoop loop;
    RecordingHandler handler;
    auto connection = loop.open(makeParams(worker.port(), "q1"), handler);
    handler.expect(Event::Kind::HelloAccepted);

    loop.shutdown();
    const Event broken = handler.expect(Event::Kind::Broken);
    EXPECT_EQ(broken.code, ErrorCodes::ABORTED);
    EXPECT_TRUE(broken.message.contains("the server is shutting down")) << broken.message;

    RecordingHandler late;
    EXPECT_THROW(loop.open(makeParams(worker.port(), "q2"), late), Exception);
}

#endif
