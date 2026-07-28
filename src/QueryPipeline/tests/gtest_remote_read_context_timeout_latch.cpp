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

/// Drives RemoteQueryExecutorReadContext::checkTimeout() and cancelBefore() directly. The read
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
    /// Used to put a bound on a blocking checkTimeout() so that a lost timeout verdict shows up
    /// as a failure instead of an unobservable hang.
    void armTimerOnly(Poco::Timespan relative) { context.timer.setRelative(relative); }

    /// The receive timer's descriptor, so a test can wait for it to actually fire instead of
    /// assuming a fixed sleep was long enough.
    int timerFd() const { return context.timer.getDescriptor(); }

    bool checkTimeout() { return context.checkTimeout(); }

    /// cancelBefore() is only reachable through AsyncTaskExecutor::cancel(), which also destroys
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

/// RemoteQueryExecutorReadContext::checkTimeout() observes the timer fd and the connection fd of
/// one epoll wake. Timer readiness alone means nothing: it is a receive timeout only if the socket
/// was NOT ready in the SAME wake. Historically the observation was stored in the
/// `is_timer_alarmed` MEMBER before the conjunction was evaluated, and nothing ever cleared it, so
/// a single wake carrying both a data packet and the timer expiry returned true (delivering the
/// packet) and then left the context permanently "timed out": every later wake without socket
/// readiness threw a spurious SOCKET_TIMEOUT even though the remote was sending progress packets
/// on time.
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
    /// via clearAsyncEvent()/processAsyncEvent(). Consuming the bytes matters: epoll is
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

/// cancelBefore() uses the "a timeout was declared" fact to decide whether it is safe to wait for
/// the pending packet ("One should not try to wait for the current packet here in case of timeout
/// because this will exceed the timeout"). That fact must survive across calls even though the raw
/// per-wake observation must not: if it were lost, cancelling a genuinely timed-out connection
/// would block for a full receive_timeout inside the drain loop.
TEST(RemoteReadContextTimeoutLatch, CancelAfterDeclaredTimeoutDoesNotWaitForPendingPacket)
{
    ConnectedPair pair;
    RemoteReadContextTestAccess ctx(getContext().context);

    /// Declare a timeout: timer ready, socket not ready.
    ctx.armReceiveTimeout(pair.clientFd(), Poco::Timespan(0, 10'000));
    ASSERT_TRUE(waitTimerReady(ctx.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";
    EXPECT_THROW(ctx.checkTimeout(), NetException);

    /// The read is still in progress (no packet was delivered), so if the timeout verdict were
    /// lost, cancelBefore() would enter its drain loop and call checkTimeout(blocking = true).
    ASSERT_TRUE(ctx.isInProgress());

    /// Re-arm the timer with a short expiry so that such a blocking call is guaranteed to return
    /// (by throwing SOCKET_TIMEOUT from inside cancelBefore()) instead of hanging the test
    /// process forever. With the fix, cancelBefore() never looks at the timer at all.
    ctx.armTimerOnly(Poco::Timespan(1, 0));

    /// The absence of a throw IS the structural signal that the drain loop was skipped: entering
    /// it would call checkTimeout(blocking = true), which with the re-armed timer necessarily
    /// throws SOCKET_TIMEOUT out of cancelBefore(). Asserting elapsed time instead would only
    /// measure the re-armed expiry, not the branch taken.
    EXPECT_NO_THROW(ctx.cancelBefore())
        << "cancelBefore() must not re-enter the read path for a connection that already timed out";
}

/// The other direction of the same contract, and the one the changelog entry promises: when NO
/// timeout was declared, cancellation must still wait for the pending packet, because skipping
/// the drain leaves the connection unsynchronised and its teardown surfaces as
/// "Connection to ... terminated (NETWORK_ERROR)". This is exactly the state produced by a
/// simultaneous-readiness wake, which the latch used to turn into "a timeout was seen".
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

    /// The read is still pending, so cancellation owes it a drain.
    ASSERT_TRUE(ctx.isInProgress());

    /// Consume the byte and re-arm the timer with a short expiry. The drain loop's first statement
    /// is checkTimeout(blocking = true), which then observes "timer ready, socket not ready" and
    /// throws SOCKET_TIMEOUT out of cancelBefore(). That throw is the observable: it can only
    /// originate inside the drain loop, so it proves the loop was entered, and it also keeps the
    /// loop from reaching resumeUnlocked() on a connection that has no server behind it. A guard
    /// that always skipped the drain would make cancelBefore() return without throwing.
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
