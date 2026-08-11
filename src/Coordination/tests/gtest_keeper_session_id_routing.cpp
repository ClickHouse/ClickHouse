#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperRequestDispatcher.h>
#include <Coordination/KeeperRequestDispatcherOld.h>
#include <Coordination/KeeperServer.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/scope_guard_safe.h>

#include <Poco/Util/XMLConfiguration.h>

#include <sstream>

#include <gtest/gtest.h>

#include "gtest_coordination_common.h"

namespace
{

/// A server without a started Raft instance. Enough for onCommit and the error paths, which only
/// touch in_flight_batches and the response routing.
struct DispatcherFixture
{
    ChangelogDirTest dir{"./session_id_routing_logs"};
    DB::KeeperContextPtr keeper_context;
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config;
    DB::SnapshotsQueue snapshots_queue{1};
    DB::KeeperSnapshotManagerS3 snapshot_s3;
    std::unique_ptr<DB::KeeperServer> server;
    std::unique_ptr<DB::KeeperRequestDispatcher> dispatcher;

    /// Responses the router took, i.e. that did not go to the per-session response queue.
    std::vector<DB::KeeperResponseForSession> routed;

    DB::KeeperSpecialResponseRouter router()
    {
        return [this](const DB::KeeperResponseForSession & response)
        {
            if (response.response->getOpNum() != Coordination::OpNum::SessionID)
                return false;
            routed.push_back(response);
            return true;
        };
    }

    DispatcherFixture()
    {
        std::string xml = R"(<clickhouse><keeper_server>
            <server_id>1</server_id>
            <tcp_port>0</tcp_port>
            <raft_configuration><server>
                <id>1</id><hostname>localhost</hostname><port>44444</port>
            </server></raft_configuration>
        </keeper_server></clickhouse>)";
        std::stringstream stream(xml); // NOLINT(readability-isolate-declaration)
        config = new Poco::Util::XMLConfiguration(stream);

        keeper_context = ::makeKeeperContext(false, nullptr);
        keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", dir.path));
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", dir.path));
        keeper_context->setStateFileDisk(std::make_shared<DB::DiskLocal>("StateFile", dir.path));
        keeper_context->setLocalLogsPreprocessed();

        server = std::make_unique<DB::KeeperServer>(
            DB::KeeperConfiguration::loadFromConfig(*config, true),
            *config,
            [](DB::KeeperResponseForSession) {},
            snapshots_queue,
            keeper_context,
            snapshot_s3,
            [](uint64_t, const DB::KeeperRequestForSession &) {});

        dispatcher = std::make_unique<DB::KeeperRequestDispatcher>(server.get(), router());
    }
};

DB::KeeperRequestForSession makeSessionIDRequest(int32_t server_id, int64_t internal_id)
{
    auto request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    request->server_id = server_id;
    request->internal_id = internal_id;
    request->session_timeout_ms = 10000;
    /// KeeperDispatcher::getSessionID leaves xid at its default and uses session id -1, so every
    /// SessionID request in the cluster carries the same (session_id, xid).
    DB::KeeperRequestForSession request_for_session;
    request_for_session.request = request;
    request_for_session.session_id = DB::keeper_internal_get_session_id;
    return request_for_session;
}

}

/// A SessionID commit from another server must not retire our in-flight SessionID request, and our
/// own must still retire it.
TEST(KeeperDispatcher, SessionIDCommitCorrelation)
{
    DispatcherFixture fixture;
    auto & dispatcher = *fixture.dispatcher;

    auto ours = makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 7);

    /// Seed one in-flight batch holding our request, the way dispatchThread would.
    size_t batch_idx = dispatcher.tail_idx.load();
    auto & batch = dispatcher.in_flight_batches[batch_idx % dispatcher.in_flight_batches.size()];
    batch.requests = {ours};
    batch.activate({});
    dispatcher.tail_idx.store(batch_idx + 1);

    ASSERT_EQ(batch.committed_requests, 0u);

    /// Same degenerate (session_id, xid), different origin.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 2, /*internal_id=*/ 7));
    EXPECT_EQ(batch.committed_requests, 0u) << "a foreign server's SessionID commit retired our request";

    /// Same server, different client.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 8));
    EXPECT_EQ(batch.committed_requests, 0u) << "another client's SessionID commit retired our request";

    /// Ours: correlation must still work, otherwise the fix would stall every session request.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 7));
    EXPECT_EQ(batch.committed_requests, 1u) << "our own SessionID commit did not retire our request";
    EXPECT_EQ(dispatcher.head_idx.load(), batch_idx + 1) << "the fully committed batch was not popped";
}

/// A dropped SessionID request must reach its waiter instead of the per-session response queue,
/// where session id -1 has no callback and the response is discarded.
TEST(KeeperDispatcher, SessionIDErrorReachesWaiter)
{
    DispatcherFixture fixture;
    auto & dispatcher = *fixture.dispatcher;

    size_t batch_idx = dispatcher.tail_idx.load();
    auto & batch = dispatcher.in_flight_batches[batch_idx % dispatcher.in_flight_batches.size()];
    batch.requests = {makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 11)};
    batch.activate({});
    dispatcher.tail_idx.store(batch_idx + 1);

    dispatcher.dropInFlightRequests();

    ASSERT_EQ(fixture.routed.size(), 1u) << "the dropped SessionID error did not reach its waiter";
    const auto & response = fixture.routed.front();
    EXPECT_EQ(response.response->error, Coordination::Error::ZCONNECTIONLOSS);

    /// The identifiers the waiter is keyed by must survive makeResponse().
    const auto & session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response.response);
    EXPECT_EQ(session_id_response.server_id, 1);
    EXPECT_EQ(session_id_response.internal_id, 11);

    EXPECT_EQ(dispatcher.head_idx.load(), batch_idx + 1) << "the dropped batch was not popped";
}

/// use_new_dispatcher is a setting, so the old dispatcher is a live carrier of the same defect. It
/// has no in-flight batch tracking, so it synthesizes the error straight from addErrorResponses.
TEST(KeeperDispatcherOld, SessionIDErrorReachesWaiter)
{
    DispatcherFixture fixture;
    DB::KeeperRequestDispatcherOld dispatcher_old(fixture.server.get(), fixture.router());
    /// Its threads loop until the context says shutdown, the way KeeperDispatcher::shutdown does it.
    SCOPE_EXIT({
        fixture.keeper_context->setShutdownCalled();
        dispatcher_old.shutdown();
    });

    dispatcher_old.addErrorResponses(
        {makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 13)},
        Coordination::Error::ZCONNECTIONLOSS,
        /*may_have_dependent_reads=*/ false);

    ASSERT_EQ(fixture.routed.size(), 1u) << "the dropped SessionID error did not reach its waiter";
    const auto & response = fixture.routed.front();
    EXPECT_EQ(response.response->error, Coordination::Error::ZCONNECTIONLOSS);
    const auto & session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response.response);
    EXPECT_EQ(session_id_response.server_id, 1);
    EXPECT_EQ(session_id_response.internal_id, 13);
}

/// Unlike the arms above, which stop at the router seam, this one drives the production router and
/// asserts on the getSessionID waiter a client actually blocks on.
TEST(KeeperDispatcher, SessionIDErrorReachesRealWaiter)
{
    DispatcherFixture fixture;

    /// onSessionIDResponse reads only server->getServerID(), set by the KeeperServer constructor, so
    /// an un-started server suffices here.
    DB::KeeperDispatcher keeper_dispatcher;
    keeper_dispatcher.server = std::move(fixture.server);
    /// Holds a raw pointer to the server keeper_dispatcher now owns, and would outlive it.
    fixture.dispatcher.reset();

    DB::KeeperRequestDispatcher dispatcher(
        keeper_dispatcher.server.get(),
        [&keeper_dispatcher](const DB::KeeperResponseForSession & response)
        { return keeper_dispatcher.tryRouteSpecialResponse(response); });

    /// Register the waiter the way getSessionID does.
    constexpr int64_t internal_id = 17;
    std::future<int64_t> future;
    {
        std::lock_guard lock(keeper_dispatcher.new_session_id_mutex);
        auto [it, inserted] = keeper_dispatcher.new_session_id_requests.try_emplace(internal_id);
        ASSERT_TRUE(inserted);
        future = it->second.get_future();
    }

    auto seed_in_flight = [&dispatcher](const DB::KeeperRequestForSession & request)
    {
        size_t batch_idx = dispatcher.tail_idx.load();
        auto & batch = dispatcher.in_flight_batches[batch_idx % dispatcher.in_flight_batches.size()];
        batch.requests = {request};
        batch.activate({});
        dispatcher.tail_idx.store(batch_idx + 1);
    };

    /// A response for a different client must not wake our waiter.
    seed_in_flight(makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 18));
    dispatcher.dropInFlightRequests();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout)
        << "another client's SessionID error woke our waiter";
    {
        std::lock_guard lock(keeper_dispatcher.new_session_id_mutex);
        EXPECT_EQ(keeper_dispatcher.new_session_id_requests.count(internal_id), 1u);
    }

    seed_in_flight(makeSessionIDRequest(/*server_id=*/ 1, internal_id));
    dispatcher.dropInFlightRequests();

    /// Ready without waiting: the client does not sit out the session timeout.
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready)
        << "the dropped SessionID error did not reach the getSessionID waiter";

    try
    {
        FAIL() << "getSessionID returned session id " << future.get() << " instead of the error";
    }
    catch (const Coordination::Exception & e)
    {
        EXPECT_EQ(e.code, Coordination::Error::ZCONNECTIONLOSS);
    }

    std::lock_guard lock(keeper_dispatcher.new_session_id_mutex);
    EXPECT_EQ(keeper_dispatcher.new_session_id_requests.count(internal_id), 0u) << "the waiter entry leaked";
}

/// Both dispatchers refuse an empty router, so a caller cannot omit the wiring and silently lose
/// SessionID error responses. The rejection is a LOGICAL_ERROR, which aborts in debug and sanitizer
/// builds, so the contract is asserted on that abort rather than with EXPECT_THROW.
TEST(KeeperDispatcher, SessionIDRouterIsRequired)
{
    /// Re-executes the binary instead of forking a process that holds the fixture's threads.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    DispatcherFixture fixture;
    auto * server = fixture.server.get();

    EXPECT_DEATH(
        DB::KeeperRequestDispatcher(server, DB::KeeperSpecialResponseRouter{}),
        "KeeperRequestDispatcher requires a special response router");
    EXPECT_DEATH(
        DB::KeeperRequestDispatcherOld(server, DB::KeeperSpecialResponseRouter{}),
        "KeeperRequestDispatcherOld requires a special response router");
}

#endif
