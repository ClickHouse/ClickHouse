#include <gtest/gtest.h>

#if defined(OS_LINUX)

#include <Client/HedgedConnectionsFactory.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <IO/ConnectionTimeouts.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

using namespace DB;

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds connect_timeout_with_failover_ms;
}
}

namespace
{

ConnectionPoolPtr makeUnreachablePool()
{
    /// Non-routable address: connect() returns EINPROGRESS and never completes, so the
    /// async establisher stays in-process until it is cancelled - the state we need.
    return std::make_shared<ConnectionPool>(
        /* max_connections */ 1,
        /* host */ "10.255.255.1",
        /* port */ 9000,
        /* default_database */ "",
        /* user */ "default",
        /* password */ "",
        /* proto_send_chunked */ "",
        /* proto_recv_chunked */ "",
        /* quota_key */ "",
        /* cluster */ "",
        /* cluster_secret */ "",
        /* client_name */ "test",
        Protocol::Compression::Enable,
        Protocol::Secure::Disable,
        /* bind_host */ "");
}

/// Owns everything the factory references (establishers keep a `const Settings &`), so it
/// can outlive the test via a shared_ptr if the failure path leaks a detached worker.
struct Fixture
{
    Settings settings;
    ConnectionPoolWithFailoverPtr pool;
    ConnectionTimeouts timeouts;
    std::optional<HedgedConnectionsFactory> factory;

    Fixture()
    {
        /// High timeout so the connection attempt stays in-process for the whole test.
        settings[Setting::connect_timeout_with_failover_ms] = 60000;
        pool = std::make_shared<ConnectionPoolWithFailover>(
            ConnectionPoolPtrs{makeUnreachablePool()}, LoadBalancing::RANDOM);
        timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
        factory.emplace(
            pool,
            settings,
            timeouts,
            /* max_tries */ 0,
            /* fallback_to_stale_replicas */ true,
            /* max_parallel_replicas */ 1,
            /* skip_unavailable_shards */ false);
    }
};

}

/// Regression test: getNextIndex() must not keep handing back a cancelled establisher.
///
/// stopChoosingReplicas() cancels an in-progress establisher but leaves its result entry
/// null, which used to look "eligible" - so getNextIndex() returned it forever while
/// resumeConnectionEstablisher() only ever yields CANNOT_CHOOSE, spinning
/// startNewConnectionImpl(). The factory now skips cancelled establishers. This drives the
/// factory directly, so it isn't masked by the higher-level `if (cancelled) return;` guard
/// in HedgedConnections::startNewReplica().
TEST(HedgedConnectionsFactory, CancelledEstablisherDoesNotLoop)
{
    auto fixture = std::make_shared<Fixture>();

    Connection * connection = nullptr;

    /// The single unreachable replica goes in-process, then gets cancelled.
    ASSERT_EQ(fixture->factory->startNewConnection(connection), HedgedConnectionsFactory::State::NOT_READY);
    fixture->factory->stopChoosingReplicas();

    /// Asking again must return; run it off-thread so a regression surfaces as a failure
    /// rather than hanging the binary. (std::async is avoided because its future blocks in
    /// the destructor; std::packaged_task's does not, so we can detach on timeout.)
    std::packaged_task<HedgedConnectionsFactory::State()> task([fixture]
    {
        Connection * next = nullptr;
        return fixture->factory->startNewConnection(next);
    });
    auto future = task.get_future();
    std::thread worker(std::move(task));

    if (future.wait_for(std::chrono::seconds(10)) == std::future_status::ready)
    {
        worker.join();
        EXPECT_EQ(future.get(), HedgedConnectionsFactory::State::CANNOT_CHOOSE);
    }
    else
    {
        /// Leak the spinning thread; it only touches the still-shared fixture and dies at
        /// process exit after this failed test.
        worker.detach();
        ADD_FAILURE() << "startNewConnection() did not return: getNextIndex() looped on a cancelled establisher";
    }
}

#endif
