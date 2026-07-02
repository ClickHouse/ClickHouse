#include <gtest/gtest.h>

#if defined(OS_LINUX)

#include <Client/HedgedConnectionsFactory.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <IO/ConnectionTimeouts.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

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

/// A local TCP listener that accepts connections but never replies to the handshake.
///
/// The async establisher connects (succeeds instantly on loopback), sends its `Hello`, then
/// blocks reading the server `Hello` that never arrives - so it yields a pending file
/// descriptor and stays in-process (NOT_READY) until cancelled. Unlike pointing at a
/// supposedly non-routable address, this is deterministic: it does not depend on host
/// routing tables and never falls into a real connect timeout.
class BlackholeListener
{
public:
    BlackholeListener()
    {
        listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0)
            throw std::runtime_error("socket() failed, errno=" + std::to_string(errno));

        int one = 1;
        ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0; /// Ephemeral port.
        if (::bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error("bind() failed, errno=" + std::to_string(errno));

        socklen_t addr_len = sizeof(addr);
        if (::getsockname(listen_fd, reinterpret_cast<sockaddr *>(&addr), &addr_len) < 0)
            throw std::runtime_error("getsockname() failed, errno=" + std::to_string(errno));
        port = ntohs(addr.sin_port);

        if (::listen(listen_fd, 16) < 0)
            throw std::runtime_error("listen() failed, errno=" + std::to_string(errno));

        shutdown_event = ::eventfd(0, EFD_NONBLOCK);
        if (shutdown_event < 0)
            throw std::runtime_error("eventfd() failed, errno=" + std::to_string(errno));

        accept_thread = std::thread([this] { acceptLoop(); });
    }

    ~BlackholeListener()
    {
        /// Signal the accept loop to stop, then drain it.
        uint64_t value = 1;
        [[maybe_unused]] ssize_t written = ::write(shutdown_event, &value, sizeof(value));
        if (accept_thread.joinable())
            accept_thread.join();

        for (int fd : accepted_fds)
            ::close(fd);
        ::close(shutdown_event);
        ::close(listen_fd);
    }

    UInt16 getPort() const { return port; }

private:
    void acceptLoop()
    {
        pollfd fds[2];
        fds[0] = {listen_fd, POLLIN, 0};
        fds[1] = {shutdown_event, POLLIN, 0};
        while (true)
        {
            if (::poll(fds, 2, -1) < 0)
            {
                if (errno == EINTR)
                    continue;
                break;
            }
            if (fds[1].revents & POLLIN)
                break;
            if (fds[0].revents & POLLIN)
            {
                /// Accept and hold the connection open without responding.
                int fd = ::accept(listen_fd, nullptr, nullptr);
                if (fd >= 0)
                    accepted_fds.push_back(fd);
            }
        }
    }

    int listen_fd = -1;
    int shutdown_event = -1;
    UInt16 port = 0;
    std::vector<int> accepted_fds;
    std::thread accept_thread;
};

ConnectionPoolPtr makeBlackholePool(UInt16 port)
{
    return std::make_shared<ConnectionPool>(
        /* max_connections */ 1,
        /* host */ "127.0.0.1",
        /* port */ port,
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
    BlackholeListener listener;
    Settings settings;
    ConnectionPoolWithFailoverPtr pool;
    ConnectionTimeouts timeouts;
    std::optional<HedgedConnectionsFactory> factory;

    Fixture()
    {
        /// High timeout so the connection attempt stays in-process for the whole test.
        settings[Setting::connect_timeout_with_failover_ms] = 60000;
        pool = std::make_shared<ConnectionPoolWithFailover>(
            ConnectionPoolPtrs{makeBlackholePool(listener.getPort())}, LoadBalancing::RANDOM);
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
