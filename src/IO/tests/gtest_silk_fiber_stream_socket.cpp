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

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <cerrno>
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

struct RetryOnceWriteBIOState
{
    int write_calls = 0;
};

int retryOnceWrite(BIO * bio, const char * data, int length)
{
    auto * state = static_cast<RetryOnceWriteBIOState *>(BIO_get_data(bio));
    ++state->write_calls;

    BIO_clear_retry_flags(bio);
    if (state->write_calls == 1)
    {
        errno = EAGAIN;
        BIO_set_retry_write(bio);
        return -1;
    }

    const int result = BIO_write(BIO_next(bio), data, length);
    BIO_copy_next_retry(bio);
    return result;
}

long retryOnceWriteCtrl(BIO * bio, int command, long argument, void * data) // NOLINT(google-runtime-int)
{
    return BIO_ctrl(BIO_next(bio), command, argument, data);
}

int retryOnceWriteCreate(BIO * bio)
{
    BIO_set_init(bio, 1);
    BIO_set_data(bio, nullptr);
    return 1;
}

int retryOnceWriteDestroy(BIO *)
{
    return 1;
}

const BIO_METHOD * retryOnceWriteBIOMethod()
{
    static const BIO_METHOD * method = []
    {
        BIO_METHOD * result = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_FILTER, "retry-once-write");
        BIO_meth_set_write(result, retryOnceWrite);
        BIO_meth_set_ctrl(result, retryOnceWriteCtrl);
        BIO_meth_set_create(result, retryOnceWriteCreate);
        BIO_meth_set_destroy(result, retryOnceWriteDestroy);
        return result;
    }();
    return method;
}

bool installRetryOnceWriteBIO(SSL * ssl, RetryOnceWriteBIOState & state)
{
    BIO * original_write_bio = SSL_get_wbio(ssl);
    if (!original_write_bio || BIO_up_ref(original_write_bio) != 1)
        return false;

    BIO * retry_write_bio = BIO_new(retryOnceWriteBIOMethod());
    if (!retry_write_bio)
    {
        BIO_free(original_write_bio);
        return false;
    }

    BIO_set_data(retry_write_bio, &state);
    BIO_push(retry_write_bio, original_write_bio);
    SSL_set0_wbio(ssl, retry_write_bio);
    return true;
}

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


/// Secure-only tests for the TLS BIO and direct OpenSSL operations. Reuses the
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

            /// A long receive timeout. The probe must remain non-blocking regardless of it.
            socket.setReceiveTimeout(Poco::Timespan(5, 0));

            /// The actual production sequence (`DB::getSocketState(StreamSocket)`, the core of the
            /// connection pool's staleness check in `HTTPConnectionPool.cpp`) calls `SSL_peek`,
            /// which reaches the always non-blocking Silk TLS BIO.
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
        << "us: the Silk TLS BIO blocked on the receive timeout instead of returning EAGAIN immediately";
}


/// At the raw level, without a ClickHouse helper, a plain `SSL_peek` on an idle TLS connection
/// must return `SSL_ERROR_WANT_READ` immediately. Silk sockets reject `Socket::setBlocking(false)`,
/// so the BIO itself has to be non-blocking.
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

            /// The Silk TLS BIO is always non-blocking. OpenSSL operations return WANT_READ or
            /// WANT_WRITE and `SecureSocketImpl` performs the fiber-aware wait outside OpenSSL.
            auto * secure = dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(socket.impl());
            SSL * ssl = secure->ssl();

            char c = 0;
            ERR_clear_error();
            Stopwatch watch;
            const int res = SSL_peek(ssl, &c, 1);
            ssl_error = SSL_get_error(ssl, res);
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

    /// Stay connected and silent until the peek has run: the connection must be idle, not closed.
    client_future.wait();
    peer.close();

    EXPECT_EQ(ssl_error, SSL_ERROR_WANT_READ);
    EXPECT_LT(elapsed_us, 500'000U)
        << "SSL_peek() took " << elapsed_us
        << "us on an idle connection: the Silk TLS BIO blocked on the receive timeout instead "
           "of returning EAGAIN immediately";
}


/// `SSL_read` itself must not suspend a fiber: otherwise the fiber can resume on another OS-thread,
/// and the subsequent `SSL_get_error` would inspect a different thread's OpenSSL error queue.
TEST_F(SilkFiberSecureSocketTest, SslReadReturnsWantReadWithoutSuspending)
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

            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);
            socket.setReceiveTimeout(Poco::Timespan(2, 0));

            auto * secure = dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(socket.impl());
            SSL * ssl = secure->ssl();
            char c = 0;
            ERR_clear_error();
            Stopwatch watch;
            const int res = SSL_read(ssl, &c, 1);
            ssl_error = SSL_get_error(ssl, res);
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

    client_future.wait();
    peer.close();

    EXPECT_EQ(ssl_error, SSL_ERROR_WANT_READ);
    EXPECT_LT(elapsed_us, 500'000U)
        << "SSL_read took " << elapsed_us
        << "us: the Silk TLS BIO suspended inside the OpenSSL operation";
}


TEST_F(SilkFiberSecureSocketTest, BlockingShutdownRetriesWantWrite)
{
    auto listener = policy.makeListener();
    const uint16_t port = listener.address().port();

    int shutdown_write_calls = 0;

    silk::FiberFuture client_future;
    const int run_result = Silk::spawn(
        [port, impl = policy.makeClient(), &shutdown_write_calls]() -> int
        {
            RetryOnceWriteBIOState retry_state;
            Poco::Net::StreamSocket socket(impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", port));

            /// Complete the TLS handshake before injecting the retry into the next TLS write.
            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);

            auto * secure = dynamic_cast<Silk::SecureFiberStreamSocketImpl *>(socket.impl());
            SSL * ssl = secure->ssl();
            if (!installRetryOnceWriteBIO(ssl, retry_state))
            {
                ADD_FAILURE() << "Could not install the retry-once write BIO";
                return 1;
            }

            socket.shutdown();
            shutdown_write_calls = retry_state.write_calls;
            return 0;
        },
        client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char ping[1] = {};
    ASSERT_EQ(peer.receiveBytes(ping, sizeof(ping)), 1);
    peer.sendBytes("y", 1);

    auto * peer_impl = dynamic_cast<Poco::Net::SecureStreamSocketImpl *>(peer.impl());
    SSL * peer_ssl = peer_impl->ssl();
    char data = 0;
    ERR_clear_error();
    const int read_result = SSL_read(peer_ssl, &data, 1);
    const int ssl_error = SSL_get_error(peer_ssl, read_result);

    EXPECT_EQ(client_future.wait(), 0);
    peer.close();

    EXPECT_GE(shutdown_write_calls, 2);
    EXPECT_EQ(read_result, 0);
    EXPECT_EQ(ssl_error, SSL_ERROR_ZERO_RETURN);
}


#endif
