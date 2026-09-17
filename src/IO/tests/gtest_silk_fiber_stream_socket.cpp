#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK && USE_SSL

#include <IO/SilkFiberStreamSocketImpl.h>
#include <IO/SilkSecureFiberStreamSocketImpl.h>
#include <IO/SocketPeerClosed.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/tests/gtest_ephemeral_certificate.h>
#include <Common/tests/gtest_silk_scheduler.h>

#include <silk/fibers/future.h>

#include <Poco/Exception.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/Socket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <cstdint>
#include <latch>
#include <string>


namespace
{

struct PlainPolicy
{
    using Listener = Poco::Net::ServerSocket;

    Listener makeListener() const { return Listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1); }

    Poco::Net::StreamSocketImpl * makeClient() const { return new Silk::FiberStreamSocketImpl; }
};

struct SecurePolicy
{
    using Listener = Poco::Net::SecureServerSocket;

    EphemeralCert cert;
    Poco::Net::Context::Ptr server_ctx{cert.makeContext(Poco::Net::Context::SERVER_USE)};
    Poco::Net::Context::Ptr client_ctx{cert.makeContext(Poco::Net::Context::CLIENT_USE)};

    Listener makeListener() const { return Listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_ctx); }

    Poco::Net::StreamSocketImpl * makeClient() const { return new Silk::SecureFiberStreamSocketImpl(client_ctx); }
};

}


template <typename Policy>
class SilkFiberSocketTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }

    Policy policy;
};

using Policies = ::testing::Types<PlainPolicy, SecurePolicy>;
TYPED_TEST_SUITE(SilkFiberSocketTest, Policies);


TYPED_TEST(SilkFiberSocketTest, RequestResponse)
{
    auto listener = this->policy.makeListener();
    const uint16_t port = listener.address().port();

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = this->policy.makeClient()]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            const auto throttler = std::make_shared<DB::Throttler>(/*max_speed_*/ 1'000'000);
            socket.setSendThrottler(throttler);
            socket.setReceiveThrottler(throttler);
            socket.bind(Poco::Net::SocketAddress("127.0.0.1", 0), /*reuseAddress*/ true);
            const uint16_t bound_port = socket.address().port();
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));
            EXPECT_EQ(socket.address().port(), bound_port);

            socket.sendBytes("Hello ", 6);
            socket.sendBytes("world", 5);
            socket.sendBytes("!", 1);

            std::string response;
            char buf[16] = {};
            while (response.size() < 3)
            {
                int n = socket.receiveBytes(buf, sizeof(buf));
                EXPECT_GT(n, 0);
                if (n <= 0)
                    return 1;
                response.append(buf, n);
            }
            EXPECT_EQ(response, "ACK");
            socket.close();
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    std::string request;
    char buf[16] = {};
    while (request.size() < 12)
    {
        int n = peer.receiveBytes(buf, sizeof(buf));
        ASSERT_GT(n, 0);
        request.append(buf, n);
    }
    EXPECT_EQ(request, "Hello world!");
    peer.sendBytes("ACK", 3);

    client_future.wait();
    peer.close();
}


TYPED_TEST(SilkFiberSocketTest, PollAndReceiveTimeout)
{
    auto listener = this->policy.makeListener();
    const uint16_t port = listener.address().port();

    std::latch negative_poll_done{1};

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = this->policy.makeClient(), &negative_poll_done]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));

            socket.sendBytes("ping", 4);
            char prime[4] = {};
            int received = 0;
            while (received < 4)
            {
                int n = socket.receiveBytes(prime + received, 4 - received);
                EXPECT_GT(n, 0);
                if (n <= 0)
                    return 1;
                received += n;
            }

            EXPECT_FALSE(socket.poll(Poco::Timespan(0, 50'000), Poco::Net::Socket::SELECT_READ));
            negative_poll_done.count_down();
            EXPECT_TRUE(socket.poll(Poco::Timespan(0, 500'000), Poco::Net::Socket::SELECT_READ));

            char data[1] = {};
            EXPECT_EQ(socket.receiveBytes(data, 1), 1);

            socket.setReceiveTimeout(Poco::Timespan(0, 100'000));
            EXPECT_THROW(socket.receiveBytes(data, sizeof(data)), Poco::TimeoutException);
            socket.close();
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char prime[4] = {};
    int received = 0;
    while (received < 4)
    {
        int n = peer.receiveBytes(prime + received, 4 - received);
        ASSERT_GT(n, 0);
        received += n;
    }
    peer.sendBytes("pong", 4);

    negative_poll_done.wait();
    peer.sendBytes("x", 1);

    client_future.wait();
    peer.close();
}


TYPED_TEST(SilkFiberSocketTest, ConnectRefused)
{
    /// Plain bound socket suffices for both variants.
    /// TCP-layer refusal happens before any TLS handshake would start.
    Poco::Net::ServerSocket bound_socket;
    bound_socket.bind(Poco::Net::SocketAddress("127.0.0.1", 0), true);
    const uint16_t closed_port = bound_socket.address().port();

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [closed_port, impl = this->policy.makeClient()]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            EXPECT_THROW(
                socket.connect(Poco::Net::SocketAddress("127.0.0.1", closed_port)),
                Poco::Net::ConnectionRefusedException);
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    client_future.wait();
}


TYPED_TEST(SilkFiberSocketTest, ThrottlerLimitEnforced)
{
    auto listener = this->policy.makeListener();
    const uint16_t port = listener.address().port();

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = this->policy.makeClient()]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));

            /// An unthrottled exchange first: it drives the TLS handshake for the secure
            /// variant and keeps the connection open until the server is done with it.
            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);

            socket.setSendThrottler(std::make_shared<DB::Throttler>(/*max_speed_*/ 1, /*limit_*/ 1, "Send limit exceeded"));
            EXPECT_THROW(socket.sendBytes("x", 1), DB::Exception);

            socket.setReceiveThrottler(std::make_shared<DB::Throttler>(/*max_speed_*/ 1, /*limit_*/ 1, "Receive limit exceeded"));
            char buf[1] = {};
            EXPECT_THROW(socket.receiveBytes(buf, sizeof(buf)), DB::Exception);

            socket.close();
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char buf[1] = {};
    ASSERT_EQ(peer.receiveBytes(buf, sizeof(buf)), 1);
    peer.sendBytes("y", 1);

    client_future.wait();
    peer.close();
}


/// Secure-only: the bug is TLS-specific (a blocking-only `silkBioRead`/`silkBioWrite` surfaces
/// through `SSL_peek`, not through a raw, BIO-less socket read). Reuses the
/// `SecurePolicy policy` member from the typed fixture rather than redeclaring it.
using SilkFiberSecureSocketTest = SilkFiberSocketTest<SecurePolicy>;


TEST_F(SilkFiberSecureSocketTest, NonBlockingPeekDoesNotBlockOnIdleConnection)
{
    auto listener = policy.makeListener();
    const uint16_t port = listener.address().port();

    uint64_t elapsed_us = 0;
    DB::SocketState state = DB::SocketState::Closed;

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = policy.makeClient(), &elapsed_us, &state]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));

            /// Drive the TLS handshake to completion and drain the exchange, so that by the time
            /// of the probe below the connection is idle: alive, but with nothing pending.
            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);

            /// A long receive timeout. Pre-fix, `silkBioRead` has no non-blocking mode and always
            /// parks the caller in a fiber wait up to this timeout, so a slow probe below proves
            /// the bug; a fast one proves the fix.
            socket.setReceiveTimeout(Poco::Timespan(5, 0));

            /// The actual production sequence (`DB::getSocketState(StreamSocket)`, the core of the
            /// connection pool's staleness check in `HTTPConnectionPool.cpp`): it puts the silk
            /// socket into don't-wait mode with `setDontWait` - `Socket::setBlocking(false)` is rejected
            /// by silk sockets - and calls `SSL_peek`, which reaches OpenSSL's socket BIO, i.e. `silkBioRead`.
            Stopwatch watch;
            state = DB::getSocketState(socket);
            elapsed_us = watch.elapsedMicroseconds();

            socket.close();
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char ping[1] = {};
    ASSERT_EQ(peer.receiveBytes(ping, sizeof(ping)), 1);
    peer.sendBytes("y", 1);

    /// Keep `peer` connected and silent until the probe above has run, so the connection is
    /// genuinely idle (alive, no pending data) rather than closed.
    client_future.wait();
    peer.close();

    EXPECT_EQ(state, DB::SocketState::Idle);
    EXPECT_LT(elapsed_us, 500'000U)
        << "getSocketState() took " << elapsed_us
        << "us: silkBioRead ignored non-blocking mode and blocked on the receive timeout instead "
           "of returning EAGAIN immediately";
}


/// The same bug at the raw level, without any ClickHouse helper: a plain `SSL_peek` on a
/// non-blocking TLS connection with no data pending must return `SSL_ERROR_WANT_READ`
/// immediately. This is exactly how a non-blocking consumer uses the socket - and the only way,
/// since silk sockets reject `Socket::setBlocking(false)`: the socket is put into don't-wait
/// mode with `setDontWait` and only `silkBioRead` ever observes it. Pre-fix, the BIO has no
/// non-blocking mode and parks the caller for the full receive timeout.
TEST_F(SilkFiberSecureSocketTest, NonBlockingSslPeekReturnsWantReadImmediately)
{
    auto listener = policy.makeListener();
    const uint16_t port = listener.address().port();

    uint64_t elapsed_us = 0;
    int ssl_error = 0;

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = policy.makeClient(), &elapsed_us, &ssl_error]() -> int
        {
            Poco::Net::StreamSocket socket(impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));

            /// Complete the TLS handshake and drain the exchange, so the connection is idle.
            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);

            socket.setReceiveTimeout(Poco::Timespan(5, 0));

            /// Put the socket into don't-wait mode - what any non-blocking user must do here,
            /// since `Socket::setBlocking(false)` throws on a silk socket.
            auto * secure = dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(socket.impl());
            SSL * ssl = secure->ssl();
            secure->setDontWait(true);

            char c = 0;
            ERR_clear_error();
            Stopwatch watch;
            const int res = SSL_peek(ssl, &c, 1);
            ssl_error = SSL_get_error(ssl, res);
            elapsed_us = watch.elapsedMicroseconds();

            secure->setDontWait(false);
            socket.close();
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char ping[1] = {};
    ASSERT_EQ(peer.receiveBytes(ping, sizeof(ping)), 1);
    peer.sendBytes("y", 1);

    /// Stay connected and silent until the peek has run: the connection must be idle, not closed.
    client_future.wait();
    peer.close();

    EXPECT_EQ(ssl_error, SSL_ERROR_WANT_READ);
    EXPECT_LT(elapsed_us, 500'000U)
        << "SSL_peek() took " << elapsed_us
        << "us on an idle non-blocking connection: silkBioRead ignored non-blocking mode and "
           "blocked on the receive timeout instead of returning EAGAIN immediately";
}


#endif
