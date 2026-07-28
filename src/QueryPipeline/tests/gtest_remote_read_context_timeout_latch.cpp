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

#include <chrono>
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
    ::usleep(200'000);

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
    ::usleep(200'000);

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
    ::usleep(200'000);
    EXPECT_THROW(ctx.checkTimeout(), NetException);

    /// The read is still in progress (no packet was delivered), so if the timeout verdict were
    /// lost, cancelBefore() would enter its drain loop and call checkTimeout(blocking = true).
    ASSERT_TRUE(ctx.isInProgress());

    /// Re-arm the timer with a short expiry so that such a blocking call is guaranteed to return
    /// (by throwing SOCKET_TIMEOUT from inside cancelBefore()) instead of hanging the test
    /// process forever. With the fix, cancelBefore() never looks at the timer at all.
    ctx.armTimerOnly(Poco::Timespan(1, 0));

    const auto start = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(ctx.cancelBefore())
        << "cancelBefore() must not re-enter the read path for a connection that already timed out";
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500)
        << "cancelBefore() must skip waiting for a packet that can no longer arrive within the timeout";
}

#endif
