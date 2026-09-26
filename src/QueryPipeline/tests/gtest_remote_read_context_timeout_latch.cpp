#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Client/Connection.h>
#include <Common/NetException.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <cerrno>
#include <chrono>
#include <poll.h>
#include <unistd.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int SOCKET_TIMEOUT;
}

/// Drives RemoteQueryExecutorReadContext::checkTimeout and cancelBefore directly. The read
/// context only needs a socket file descriptor and a timer, so a plain connected TCP socket pair
/// is enough - no ClickHouse server and no query are involved.
struct RemoteReadContextTestAccess
{
    Connection connection;
    RemoteQueryExecutor executor;
    RemoteQueryExecutorReadContext context;

    explicit RemoteReadContextTestAccess(const ContextPtr & query_context)
        : connection(
              "127.0.0.1",
              /*port_=*/0,
              /*default_database_=*/"",
              /*user_=*/"default",
              /*password_=*/"",
              /*proto_send_chunked_=*/"notchunked",
              /*proto_recv_chunked_=*/"notchunked",
              SSHKey(),
              /*jwt_=*/"",
              /*quota_key_=*/"",
              /*cluster_=*/"",
              /*cluster_secret_=*/"",
              /*client_name_=*/"gtest",
              Protocol::Compression::Disable,
              Protocol::Secure::Disable,
              /*tls_sni_override_=*/"",
              /*bind_host_=*/"")
        , executor(connection, "SELECT 1", std::make_shared<const Block>(Block{}), query_context)
        , context(executor, /*suspend_when_query_sent_=*/false, /*read_packet_type_separately_=*/false)
    {
    }

    /// Register `fd` as the connection descriptor and arm the receive timer, exactly as the
    /// async read path does when a read on the socket would block.
    void armReceiveTimeout(int fd, Poco::Timespan receive_timeout)
    {
        context.processAsyncEvent(fd, receive_timeout, AsyncEventTimeoutType::RECEIVE, "socket (test)", AsyncTaskExecutor::Event::READ);
    }

    void disarm() { context.clearAsyncEvent(); }

    /// Re-arm only the timer, leaving the epoll registration and `is_in_progress` untouched.
    /// Used to put a bound on a blocking checkTimeout so that a lost timeout verdict shows up
    /// as a failure instead of an unobservable hang.
    void armTimerOnly(Poco::Timespan relative) { context.timer.setRelative(relative); }

    /// The receive timer's descriptor, so a test can wait for it to actually fire instead of
    /// assuming a fixed sleep was long enough.
    int timerFd() const { return context.timer.getDescriptor(); }

    bool checkTimeout() { return context.checkTimeout(); }

    /// cancelBefore is only reachable through AsyncTaskExecutor::cancel, which also destroys
    /// the fiber; calling it directly keeps the test to the state machine under test.
    void cancelBefore() { context.cancelBefore(); }

    bool isInProgress() const { return context.isInProgress(); }
};

namespace
{

/// A connected TCP socket pair whose client end can be made readable on demand. The kernel
/// listen backlog completes the handshake, so no server thread is needed.
struct ConnectedPair
{
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 1};
    Poco::Net::StreamSocket client;
    Poco::Net::StreamSocket server;

    ConnectedPair()
    {
        client.connect(listener.address());
        server = listener.acceptConnection();
    }

    int clientFd() const { return const_cast<Poco::Net::StreamSocket &>(client).impl()->sockfd(); }

    /// Make the client end readable and wait until the kernel reports it as such, so a
    /// subsequent epoll wake deterministically sees the socket ready.
    void makeClientReadable()
    {
        const char byte = 'x';
        server.sendBytes(&byte, 1);
        ASSERT_TRUE(client.poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));
    }

    /// epoll is level-triggered, so unread bytes keep the socket ready in every later wake.
    /// Consuming them is what lets a subsequent wake observe "neither descriptor ready".
    void drainClient()
    {
        char buf[64];
        while (client.poll(Poco::Timespan(0, 0), Poco::Net::Socket::SELECT_READ))
        {
            if (client.receiveBytes(buf, sizeof(buf)) <= 0)
                break;
        }
    }
};

/// Wait until the receive timer's descriptor is readable, so the following epoll wake is
/// guaranteed to report it. A fixed sleep can return early (EINTR, or an oversubscribed host),
/// and in a test where the socket is deliberately readable that would leave a socket-only wake,
/// which satisfies the same assertions for the wrong reason.
[[nodiscard]] bool waitTimerReady(int timer_fd, Poco::Timespan limit)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(limit.totalMicroseconds());
    while (true)
    {
        pollfd p{.fd = timer_fd, .events = POLLIN, .revents = 0};
        const int rc = ::poll(&p, 1, 50);
        if (rc == 1 && (p.revents & POLLIN))
            return true;
        if (rc == -1 && errno != EINTR)
            return false;
        if (std::chrono::steady_clock::now() >= deadline)
            return false;
    }
}

}

/// Timer readiness in one epoll wake is a receive timeout only if the socket was NOT ready in
/// that same wake, so it must leave no residue on the context.
TEST(RemoteReadContextTimeoutLatch, SimultaneousReadinessLeavesNoTimeoutResidue)
{
    ConnectedPair pair;
    RemoteReadContextTestAccess ctx(getContext().context);

    /// Arm a short receive timeout, let it expire, and make the socket readable as well, so the
    /// next epoll wake reports BOTH descriptors.
    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(0, 10'000));
    pair.makeClientReadable();
    ASSERT_TRUE(waitTimerReady(ctx.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";

    /// Both ready => a packet is available => this is not a receive timeout.
    EXPECT_NO_THROW(EXPECT_TRUE(ctx.checkTimeout()));

    /// The packet is consumed and the timer re-armed for the next one, as the async read path does
    /// via clearAsyncEvent/processAsyncEvent. Consuming the bytes matters: epoll is
    /// level-triggered, so an unread byte would keep the socket ready and mask a latched timer.
    pair.drainClient();
    ctx.disarm();
    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(30, 0));

    /// Now NEITHER descriptor is ready. A latched observation from the previous wake is the only
    /// thing that could make this throw.
    EXPECT_NO_THROW(EXPECT_TRUE(ctx.checkTimeout()))
        << "a wake in which the socket was ready must not leave the connection permanently timed out";
}

/// The regression guard for the opposite direction: a genuine receive timeout (timer ready, socket
/// NOT ready) must still throw SOCKET_TIMEOUT.
TEST(RemoteReadContextTimeoutLatch, GenuineTimeoutStillThrows)
{
    ConnectedPair pair;
    RemoteReadContextTestAccess ctx(getContext().context);

    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(0, 10'000));
    ASSERT_TRUE(waitTimerReady(ctx.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";

    try
    {
        ctx.checkTimeout();
        FAIL() << "a genuine receive timeout must throw";
    }
    catch (const NetException & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SOCKET_TIMEOUT);
        EXPECT_NE(std::string(e.message()).find("Timeout exceeded while reading from socket"), std::string::npos) << e.message();
    }
}

/// cancelBefore skips the drain when a timeout was declared, so that verdict must survive across
/// calls: losing it would block cancellation for a full receive_timeout inside the drain loop.
TEST(RemoteReadContextTimeoutLatch, CancelAfterDeclaredTimeoutDoesNotWaitForPendingPacket)
{
    ConnectedPair pair;
    RemoteReadContextTestAccess ctx(getContext().context);

    /// Declare a timeout: timer ready, socket not ready.
    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(0, 10'000));
    ASSERT_TRUE(waitTimerReady(ctx.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";
    EXPECT_THROW(ctx.checkTimeout(), NetException);

    ASSERT_TRUE(ctx.isInProgress());

    /// Bounds the blocking checkTimeout the drain loop would make, so a lost verdict fails instead
    /// of hanging. With the fix cancelBefore never looks at the timer at all.
    ctx.armTimerOnly(Poco::Timespan(1, 0));

    /// The absence of a throw is the signal that the drain loop was skipped: entering it calls
    /// checkTimeout(blocking = true), which with the re-armed timer necessarily throws out of
    /// cancelBefore. Asserting elapsed time would only measure the re-armed expiry.
    EXPECT_NO_THROW(ctx.cancelBefore())
        << "cancelBefore() must not re-enter the read path for a connection that already timed out";
}

/// The other direction: without a declared timeout the drain must still happen, because skipping
/// it leaves the connection unsynchronised and teardown surfaces as
/// "Connection to ... terminated (NETWORK_ERROR)" - the state a simultaneous-readiness wake used
/// to produce.
TEST(RemoteReadContextTimeoutLatch, CancelWithoutDeclaredTimeoutDrainsPendingPacket)
{
    ConnectedPair pair;
    RemoteReadContextTestAccess ctx(getContext().context);

    /// A simultaneous-readiness wake: the packet is there, so no timeout is declared.
    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(0, 10'000));
    pair.makeClientReadable();
    ASSERT_TRUE(waitTimerReady(ctx.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";
    EXPECT_NO_THROW(EXPECT_TRUE(ctx.checkTimeout()));

    ASSERT_TRUE(ctx.isInProgress());

    /// The throw is the observable: it can only originate in the drain loop's blocking
    /// checkTimeout, so it proves the loop was entered, and it stops the loop short of
    /// resumeUnlocked on a connection with no server behind it.
    pair.drainClient();
    ctx.armTimerOnly(Poco::Timespan(0, 100'000));

    try
    {
        ctx.cancelBefore();
        FAIL() << "cancelBefore() must drain the pending packet when no timeout was declared";
    }
    catch (const NetException & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SOCKET_TIMEOUT);
    }
}

#endif
