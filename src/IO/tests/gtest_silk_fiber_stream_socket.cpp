#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK && USE_SSL

#include <IO/SilkFiberStreamSocketImpl.h>
#include <IO/SilkSecureFiberStreamSocketImpl.h>

#include <Common/Exception.h>
#include <Common/SilkThrottler.h>
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/init.h>

#include <Poco/Exception.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/Socket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <cstdint>
#include <latch>
#include <string>


namespace
{

class SilkEnvironment : public ::testing::Environment
{
public:
    void SetUp() override
    {
        /// TODO(mstetsyuk): Silk::initializeFiberScheduler and Silk::destroyFiberScheduler are coming in another PR.
        silk::initialize();
        silk::FiberScheduler::Options options;
        /// OpenSSL handshakes run on fiber stacks and need more room than the silk default.
        options.fiberStackSize = 320 * 1024;
        silk::FiberScheduler::initialize(&options);
    }

    void TearDown() override
    {
        silk::FiberScheduler::destroy();
        silk::destroy();
    }
};

::testing::Environment * const silk_env = ::testing::AddGlobalTestEnvironment(new SilkEnvironment);


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
    Policy policy;
};

using Policies = ::testing::Types<PlainPolicy, SecurePolicy>;
TYPED_TEST_SUITE(SilkFiberSocketTest, Policies);


TYPED_TEST(SilkFiberSocketTest, RequestResponse)
{
    auto listener = this->policy.makeListener();
    const uint16_t port = listener.address().port();

    struct Params
    {
        uint16_t port;
        Poco::Net::StreamSocketImpl * impl;
    };

    silk::FiberFuture client_future;
    const int run_result = silk::FiberScheduler::run(
        +[](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            const auto throttler = std::make_shared<Silk::Throttler>(/*max_speed_*/ 1'000'000);
            socket.setSendThrottler(throttler);
            socket.setReceiveThrottler(throttler);
            socket.bind(Poco::Net::SocketAddress("127.0.0.1", 0), /*reuseAddress*/ true);
            const uint16_t bound_port = socket.address().port();
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port));
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
        Params{port, this->policy.makeClient()},
        &client_future);
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

    struct Params
    {
        uint16_t port;
        Poco::Net::StreamSocketImpl * impl;
        std::latch * negative_poll_done;
    };

    silk::FiberFuture client_future;
    const int run_result = silk::FiberScheduler::run(
        +[](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port));

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
            p->negative_poll_done->count_down();
            EXPECT_TRUE(socket.poll(Poco::Timespan(0, 500'000), Poco::Net::Socket::SELECT_READ));

            char data[1] = {};
            EXPECT_EQ(socket.receiveBytes(data, 1), 1);

            socket.setReceiveTimeout(Poco::Timespan(0, 100'000));
            EXPECT_THROW(socket.receiveBytes(data, sizeof(data)), Poco::TimeoutException);
            socket.close();
            return 0;
        },
        Params{port, this->policy.makeClient(), &negative_poll_done},
        &client_future);
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

    struct Params
    {
        uint16_t port;
        Poco::Net::StreamSocketImpl * impl;
    };

    silk::FiberFuture client_future;
    const int run_result = silk::FiberScheduler::run(
        +[](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            EXPECT_THROW(
                socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port)),
                Poco::Net::ConnectionRefusedException);
            return 0;
        },
        Params{closed_port, this->policy.makeClient()},
        &client_future);
    ASSERT_EQ(run_result, 0);

    client_future.wait();
}


TYPED_TEST(SilkFiberSocketTest, ThrottlerLimitEnforced)
{
    auto listener = this->policy.makeListener();
    const uint16_t port = listener.address().port();

    struct Params
    {
        uint16_t port;
        Poco::Net::StreamSocketImpl * impl;
    };

    silk::FiberFuture client_future;
    const int run_result = silk::FiberScheduler::run(
        +[](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port));

            /// An unthrottled exchange first: it drives the TLS handshake for the secure
            /// variant and keeps the connection open until the server is done with it.
            socket.sendBytes("x", 1);
            char pong[1] = {};
            EXPECT_EQ(socket.receiveBytes(pong, sizeof(pong)), 1);

            socket.setSendThrottler(std::make_shared<Silk::Throttler>(/*max_speed_*/ 1, /*limit_*/ 1, "Send limit exceeded"));
            EXPECT_THROW(socket.sendBytes("x", 1), DB::Exception);

            socket.setReceiveThrottler(std::make_shared<Silk::Throttler>(/*max_speed_*/ 1, /*limit_*/ 1, "Receive limit exceeded"));
            char buf[1] = {};
            EXPECT_THROW(socket.receiveBytes(buf, sizeof(buf)), DB::Exception);

            socket.close();
            return 0;
        },
        Params{port, this->policy.makeClient()},
        &client_future);
    ASSERT_EQ(run_result, 0);

    auto peer = listener.acceptConnection();
    char buf[1] = {};
    ASSERT_EQ(peer.receiveBytes(buf, sizeof(buf)), 1);
    peer.sendBytes("y", 1);

    client_future.wait();
    peer.close();
}


#endif
