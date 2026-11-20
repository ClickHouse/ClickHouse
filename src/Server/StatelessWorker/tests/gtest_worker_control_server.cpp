#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <Server/StatelessWorker/StatelessWorkerControlServer.h>
#include <Interpreters/InterserverCredentials.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <base/scope_guard.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <fmt/format.h>

#include <chrono>
#include <memory>
#include <thread>

namespace DB::FailPoints
{
    extern const char stateless_worker_pause_task_before_setup[];
    extern const char stateless_worker_pause_task_cancel[];
}

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int AUTHENTICATION_FAILED;
    extern const int INCORRECT_DATA;
    extern const int PROTOCOL_VERSION_MISMATCH;
    extern const int SOCKET_TIMEOUT;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

using namespace DB;
using namespace DB::StatelessWorkerControlProtocol;
using namespace std::chrono_literals;

namespace
{

std::shared_ptr<StatelessWorkerControlServer> makeControlServer(
    size_t max_queries, InterserverCredentialsPtr credentials = nullptr, std::chrono::milliseconds idle_timeout = 30s)
{
    const StatelessWorkerControlServer::Limits limits{.max_queries = max_queries, .max_sessions = 4 * max_queries, .idle_timeout = idle_timeout};
    return std::make_shared<StatelessWorkerControlServer>(limits, [credentials] { return credentials; });
}

/// A streaming-exchange server on a free loopback port that hands control connections to `control_server`.
class Worker
{
public:
    explicit Worker(ExchangeControlConnectionHandlerPtr control_server, ExchangeConnectionAuthenticator authenticate = {})
        : exchange_server(
            "127.0.0.1", 0, 64, std::make_shared<ExchangeConnections>(), std::move(authenticate), std::move(control_server))
    {
        exchange_server.start();
    }

    UInt16 port() const { return exchange_server.port(); }

private:
    ExchangeServer exchange_server;
};

/// The initiator's end of a control connection, with blocking reads bounded by a receive timeout.
class Initiator
{
public:
    explicit Initiator(UInt16 port)
        : socket(Poco::Net::SocketAddress("127.0.0.1", port))
        , in(socket)
        , out(socket)
    {
        socket.setReceiveTimeout(Poco::Timespan(10, 0));
        socket.setSendTimeout(Poco::Timespan(10, 0));
    }

    ~Initiator() { out.cancel(); }

    template <typename Message>
    void send(const Message & message)
    {
        writeFrame(message, out);
        out.next();
    }

    /// Sends the frame of `message` in `pieces` parts, `pause` apart.
    template <typename Message>
    void sendSlowly(const Message & message, size_t pieces, std::chrono::milliseconds pause)
    {
        WriteBufferFromOwnString frame;
        writeFrame(message, frame);
        frame.finalize();
        const std::string_view bytes = frame.stringView();
        const size_t piece = (bytes.size() + pieces - 1) / pieces;
        for (size_t offset = 0; offset < bytes.size(); offset += piece)
        {
            if (offset != 0)
                std::this_thread::sleep_for(pause);
            const std::string_view part = bytes.substr(offset, piece);
            out.write(part.data(), part.size());
            out.next();
        }
    }

    template <typename Message>
    Message receive()
    {
        const PacketHeader header = readHeader(in, Sender::Worker);
        Message message;
        readBody(header, in, message);
        return message;
    }

    ControlHelloReply hello(const ControlHello & message)
    {
        send(message);
        return receive<ControlHelloReply>();
    }

    /// True once the worker has closed the connection gracefully; a reset throws.
    bool closedByWorker() { return in.eof(); }

    /// Returns true when a frame starts to arrive within `timeout`.
    bool hasInputWithin(std::chrono::seconds timeout)
    {
        return in.hasPendingData() || socket.poll(Poco::Timespan(timeout.count(), 0), Poco::Net::Socket::SELECT_READ);
    }

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

ControlHello makeHello(const String & unique_query_id, const String & user = "", const String & password = "", const String & auth_token = "")
{
    return ControlHello{
        .version = PROTOCOL_VERSION,
        .purpose = Purpose::Query,
        .unique_query_id = unique_query_id,
        .auth_token = auth_token,
        .user = user,
        .password = password,
        .initiator = "gtest",
    };
}

/// Returns a batch of one task of the stage `main`, whose plan the task never reads in these tests.
StartTasks makeStartTasks(UInt64 request_id, const String & task_id)
{
    DistributedQueryTaskBatch::Task task;
    task.task.task_id = task_id;
    StartTasks request;
    request.request_id = request_id;
    request.temp_path = "gtest";
    request.batch.initial_query_id = "gtest";
    request.batch.stages.push_back({"main", std::make_shared<const String>("plan"), {task}});
    return request;
}

/// Returns the status of `task_id` on a new connection of `unique_query_id`, or the refusal of its hello.
String taskStatus(UInt16 port, const String & unique_query_id, const String & task_id)
{
    Initiator initiator(port);
    if (const auto reply = initiator.hello(makeHello(unique_query_id)); reply.code != 0)
        return reply.message;
    initiator.send(GetStatuses{.request_id = 1, .wait_milliseconds = 0, .task_ids = {task_id}});
    return initiator.receive<GetStatusesReply>().entries.at(0).status.status;
}

}

TEST(WorkerControlServer, HelloAndPing)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());

    const ControlHelloReply reply = initiator.hello(makeHello("q1"));
    EXPECT_EQ(reply.code, 0) << reply.message;
    EXPECT_EQ(reply.version, PROTOCOL_VERSION);
    EXPECT_EQ(reply.task_serialization_version, static_cast<UInt64>(DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION));
    EXPECT_EQ(reply.plan_serialization_version, static_cast<UInt64>(DBMS_QUERY_PLAN_SERIALIZATION_VERSION));

    initiator.send(Ping{.request_id = 1});
    EXPECT_EQ(initiator.receive<Pong>().request_id, 1u);

    /// Frames that arrive together are answered in order.
    initiator.send(Ping{.request_id = 2});
    initiator.send(Ping{.request_id = 3});
    EXPECT_EQ(initiator.receive<Pong>().request_id, 2u);
    EXPECT_EQ(initiator.receive<Pong>().request_id, 3u);
}

TEST(WorkerControlServer, VersionMismatchIsRefused)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());

    ControlHello hello = makeHello("q1");
    hello.version = PROTOCOL_VERSION + 1;
    initiator.send(hello);
    const auto reply = initiator.receive<ControlHelloReply>();
    EXPECT_EQ(reply.code, ErrorCodes::PROTOCOL_VERSION_MISMATCH);
    EXPECT_EQ(reply.version, PROTOCOL_VERSION);
    EXPECT_TRUE(reply.message.contains(fmt::format("version {}", PROTOCOL_VERSION + 1))) << reply.message;
    EXPECT_TRUE(initiator.closedByWorker());
}

TEST(WorkerControlServer, CredentialsAndTokenAreChecked)
{
    auto credentials = std::make_shared<InterserverCredentials>(
        "admin", SecretString("secret"), InterserverCredentials::CurrentCredentials{{"admin", SecretString("secret")}});
    auto control_server = makeControlServer(10, credentials);
    Worker worker(control_server, [](const String & token)
    {
        if (token != "good")
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Rejected token '{}'", token);
    });

    const auto refused = [&](const String & user, const String & password, const String & token)
    {
        Initiator initiator(worker.port());
        const auto reply = initiator.hello(makeHello("q1", user, password, token));
        EXPECT_TRUE(initiator.closedByWorker());
        return reply;
    };

    auto reply = refused("admin", "wrong", "good");
    EXPECT_EQ(reply.code, ErrorCodes::AUTHENTICATION_FAILED);
    EXPECT_TRUE(reply.message.contains("Incorrect user or password")) << reply.message;

    reply = refused("", "", "good");
    EXPECT_EQ(reply.code, ErrorCodes::AUTHENTICATION_FAILED);

    reply = refused("admin", "secret", "bad");
    EXPECT_EQ(reply.code, ErrorCodes::AUTHENTICATION_FAILED);
    EXPECT_TRUE(reply.message.contains("Rejected token 'bad'")) << reply.message;

    Initiator initiator(worker.port());
    reply = initiator.hello(makeHello("q1", "admin", "secret", "good"));
    EXPECT_EQ(reply.code, 0) << reply.message;
}

TEST(WorkerControlServer, CredentialsWithoutServerCredentialsAreRefused)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());

    const auto reply = initiator.hello(makeHello("q1", "admin", "secret"));
    EXPECT_EQ(reply.code, ErrorCodes::AUTHENTICATION_FAILED);
    EXPECT_TRUE(initiator.closedByWorker());
}

TEST(WorkerControlServer, MaxQueriesRefusesAnotherQuery)
{
    auto control_server = makeControlServer(/*max_queries=*/ 1);
    Worker worker(control_server);

    Initiator first(worker.port());
    EXPECT_EQ(first.hello(makeHello("q1")).code, 0);

    /// A second connection of the same query shares its slot.
    Initiator same_query(worker.port());
    EXPECT_EQ(same_query.hello(makeHello("q1")).code, 0);

    Initiator other_query(worker.port());
    const auto reply = other_query.hello(makeHello("q2"));
    EXPECT_EQ(reply.code, ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
    EXPECT_TRUE(reply.message.contains("stateless_worker_server.max_queries")) << reply.message;
    EXPECT_TRUE(other_query.closedByWorker());
}

TEST(WorkerControlServer, ClosedConnectionReleasesTheSlot)
{
    auto control_server = makeControlServer(/*max_queries=*/ 1);
    Worker worker(control_server);

    Initiator first(worker.port());
    EXPECT_EQ(first.hello(makeHello("q1")).code, 0);
    first.close();
    ASSERT_TRUE(control_server->waitForSessions(std::chrono::steady_clock::now() + 10s));

    Initiator second(worker.port());
    EXPECT_EQ(second.hello(makeHello("q2")).code, 0);
}

TEST(WorkerControlServer, UnknownTasksAreAnsweredAtOnce)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);

    /// Within the initiator's receive timeout, far below the requested wait.
    initiator.send(GetStatuses{.request_id = 3, .wait_milliseconds = 60'000, .task_ids = {"q1::a", "q1::b"}});
    const auto reply = initiator.receive<GetStatusesReply>();
    EXPECT_EQ(reply.request_id, 3u);
    ASSERT_EQ(reply.entries.size(), 2u);
    EXPECT_EQ(reply.entries[0].task_id, "q1::a");
    EXPECT_EQ(reply.entries[0].status.status, "Unknown task");
    EXPECT_EQ(reply.entries[1].task_id, "q1::b");
    EXPECT_EQ(reply.entries[1].status.status, "Unknown task");

    initiator.send(CancelTasks{.request_id = 4, .task_ids = {"q1::a"}});
    EXPECT_EQ(initiator.receive<CancelTasksReply>().request_id, 4u);
    initiator.send(ForgetTasks{.request_id = 5, .task_ids = {"q1::a"}});
    EXPECT_EQ(initiator.receive<ForgetTasksReply>().request_id, 5u);
}

TEST(WorkerControlServer, TaskOfAnotherQueryClosesTheConnection)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);

    initiator.send(CancelTasks{.request_id = 1, .task_ids = {"q1::a", "q1x::b"}});
    const auto error = initiator.receive<Error>();
    EXPECT_EQ(error.request_id, 0u);
    EXPECT_EQ(error.code, ErrorCodes::INCORRECT_DATA);
    EXPECT_TRUE(error.message.contains("q1x::b")) << error.message;
    EXPECT_TRUE(initiator.closedByWorker());
}

TEST(WorkerControlServer, ProtocolErrorsCloseTheConnection)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);

    {
        Initiator initiator(worker.port());
        initiator.send(Ping{.request_id = 1});
        const auto reply = initiator.receive<ControlHelloReply>();
        EXPECT_EQ(reply.code, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
        EXPECT_TRUE(initiator.closedByWorker());
    }
    {
        Initiator initiator(worker.port());
        ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);
        initiator.send(makeHello("q1"));
        const auto error = initiator.receive<Error>();
        EXPECT_EQ(error.request_id, 0u);
        EXPECT_EQ(error.code, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
        EXPECT_TRUE(initiator.closedByWorker());
    }
    {
        Initiator initiator(worker.port());
        ControlHello probe = makeHello("");
        probe.purpose = Purpose::Probe;
        initiator.send(probe);
        ASSERT_EQ(initiator.receive<ControlHelloReply>().code, 0);
        initiator.send(Ping{.request_id = 1});
        EXPECT_EQ(initiator.receive<Pong>().request_id, 1u);
        initiator.send(GetStatuses{.request_id = 2, .wait_milliseconds = 0, .task_ids = {}});
        EXPECT_EQ(initiator.receive<Error>().code, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
        EXPECT_TRUE(initiator.closedByWorker());
    }
}

TEST(WorkerControlServer, SilenceClosesTheConnection)
{
    auto control_server = makeControlServer(10, nullptr, /*idle_timeout=*/ 200ms);
    Worker worker(control_server);
    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);

    const auto error = initiator.receive<Error>();
    EXPECT_EQ(error.request_id, 0u);
    EXPECT_EQ(error.code, ErrorCodes::SOCKET_TIMEOUT);
    EXPECT_TRUE(initiator.closedByWorker());
}

TEST(WorkerControlServer, SlowStartTasksBodyKeepsTheConnection)
{
    auto control_server = makeControlServer(10, nullptr, /*idle_timeout=*/ 2s);
    Worker worker(control_server);
    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);

    /// The body arrives over 5 s, each part well within the idle timeout.
    StartTasks request = makeStartTasks(1, "t");
    request.batch.stages[0].serialized_query_plan = std::make_shared<const String>(64 * 1024, 'p');
    initiator.sendSlowly(request, /*pieces=*/ 50, /*pause=*/ 100ms);
    EXPECT_EQ(initiator.receive<StartTasksReply>().code, 0);

    /// The idle time counts from the end of the frame.
    if (initiator.hasInputWithin(1s))
        FAIL() << "The worker closed the connection after the frame: " << initiator.receive<Error>().message;
    initiator.send(Ping{.request_id = 2});
    EXPECT_EQ(initiator.receive<Pong>().request_id, 2u);
}

TEST(WorkerControlServer, ShutdownClosesTheConnections)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);

    control_server->shutdown();
    const auto error = initiator.receive<Error>();
    EXPECT_EQ(error.request_id, 0u);
    EXPECT_EQ(error.code, ErrorCodes::ABORTED);
    EXPECT_TRUE(initiator.closedByWorker());

    Initiator late(worker.port());
    EXPECT_EQ(late.hello(makeHello("q2")).code, ErrorCodes::ABORTED);
    EXPECT_TRUE(late.closedByWorker());
}

TEST(WorkerControlServer, TaskThatOutlivesTheReclaimIsForgottenWhenItEnds)
{
    FailPointInjection::enableFailPoint(FailPoints::stateless_worker_pause_task_before_setup);
    SCOPE_EXIT(FailPointInjection::disableFailPoint(FailPoints::stateless_worker_pause_task_before_setup));
    const StatelessWorkerControlServer::Limits limits{.max_queries = 1, .max_sessions = 4, .reclaim_timeout = 300ms};
    auto control_server = std::make_shared<StatelessWorkerControlServer>(limits, [] { return InterserverCredentialsPtr{}; });
    Worker worker(control_server);

    {
        Initiator initiator(worker.port());
        ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);
        initiator.send(makeStartTasks(1, "t"));
        ASSERT_EQ(initiator.receive<StartTasksReply>().code, 0);
        /// The task is held before it can see the cancellation of the reclaim.
        FailPointInjection::waitForPause(FailPoints::stateless_worker_pause_task_before_setup);
    }
    /// The reclaim of the closed connection gives up on the task.
    ASSERT_TRUE(control_server->waitForSessions(std::chrono::steady_clock::now() + 10s));

    FailPointInjection::disableFailPoint(FailPoints::stateless_worker_pause_task_before_setup);
    String status;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        status = taskStatus(worker.port(), "q1", "q1::t");
        if (status == "Unknown task")
            break;
        std::this_thread::sleep_for(50ms);
    }
    /// The ended task is forgotten, so its query no longer holds the only slot.
    EXPECT_EQ(status, "Unknown task");
    ASSERT_TRUE(control_server->waitForSessions(std::chrono::steady_clock::now() + 10s));
    Initiator other_query(worker.port());
    const auto reply = other_query.hello(makeHello("q2"));
    EXPECT_EQ(reply.code, 0) << reply.message;
}

TEST(WorkerControlServer, SlowCancellationDoesNotHoldHellos)
{
    auto control_server = makeControlServer(10);
    Worker worker(control_server);
    /// The task stays registered without running, and its cancellation is held.
    FailPointInjection::enableFailPoint(FailPoints::stateless_worker_pause_task_before_setup);
    FailPointInjection::enableFailPoint(FailPoints::stateless_worker_pause_task_cancel);
    SCOPE_EXIT({
        FailPointInjection::disableFailPoint(FailPoints::stateless_worker_pause_task_cancel);
        FailPointInjection::disableFailPoint(FailPoints::stateless_worker_pause_task_before_setup);
    });

    Initiator initiator(worker.port());
    ASSERT_EQ(initiator.hello(makeHello("q1")).code, 0);
    initiator.send(makeStartTasks(1, "t"));
    ASSERT_EQ(initiator.receive<StartTasksReply>().code, 0);
    initiator.send(CancelTasks{.request_id = 2, .task_ids = {"q1::t"}});
    FailPointInjection::waitForPause(FailPoints::stateless_worker_pause_task_cancel);

    /// The admission of another query does not wait for the cancellation.
    Initiator other_query(worker.port());
    other_query.send(makeHello("q2"));
    const bool answered = other_query.hasInputWithin(3s);
    FailPointInjection::disableFailPoint(FailPoints::stateless_worker_pause_task_cancel);
    EXPECT_TRUE(answered) << "The hello of another query waited for the cancellation of a task";
    EXPECT_EQ(other_query.receive<ControlHelloReply>().code, 0);
    EXPECT_EQ(initiator.receive<CancelTasksReply>().request_id, 2u);
}

TEST(WorkerControlServer, ExchangeServerWithoutControlServerAnswersError)
{
    Worker worker(nullptr);
    Initiator initiator(worker.port());

    initiator.send(makeHello("q1"));
    const auto error = initiator.receive<Error>();
    EXPECT_EQ(error.request_id, 0u);
    EXPECT_EQ(error.code, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    EXPECT_TRUE(error.message.contains("stateless_worker_server.enabled")) << error.message;
    EXPECT_TRUE(initiator.closedByWorker());
}

#endif
