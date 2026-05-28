#include "config.h"

#include <gtest/gtest.h>

#if defined(OS_LINUX) && USE_SSL

#include <IO/SilkFiberStreamSocketImpl.h>
#include <IO/SilkSecureFiberStreamSocketImpl.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

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

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>


namespace
{

class SilkEnvironment : public ::testing::Environment
{
public:
    void SetUp() override { silk::FiberScheduler::initialize(); }
    void TearDown() override { silk::FiberScheduler::destroy(); }
};

::testing::Environment * const silk_env = ::testing::AddGlobalTestEnvironment(new SilkEnvironment);


Poco::Net::Context::Ptr makeContext(Poco::Net::Context::Usage usage)
{
    Poco::Net::Context::Params params;
    params.privateKeyFile = std::string(CLICKHOUSE_TESTS_CONFIG_DIR) + "/server.key";
    params.certificateFile = std::string(CLICKHOUSE_TESTS_CONFIG_DIR) + "/server.crt";
    params.verificationMode = Poco::Net::Context::VERIFY_NONE;
    return new Poco::Net::Context(usage, params);
}


struct PlainPolicy
{
    using Listener = Poco::Net::ServerSocket;

    Listener makeListener() { return Listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1); }

    Poco::Net::StreamSocketImpl * makeClient() { return new Silk::FiberStreamSocketImpl; }
};

struct SecurePolicy
{
    using Listener = Poco::Net::SecureServerSocket;

    Poco::Net::Context::Ptr server_ctx{makeContext(Poco::Net::Context::SERVER_USE)};
    Poco::Net::Context::Ptr client_ctx{makeContext(Poco::Net::Context::CLIENT_USE)};

    Listener makeListener() { return Listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_ctx); }

    Poco::Net::StreamSocketImpl * makeClient() { return new Silk::SecureFiberStreamSocketImpl(client_ctx); }
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
    silk::FiberScheduler::run(
        [](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port));

            socket.sendBytes("Hello ", 6);
            socket.sendBytes("world", 5);
            socket.sendBytes("!", 1);

            std::string response;
            char buf[16];
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

    auto peer = listener.acceptConnection();
    std::string request;
    char buf[16];
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

    struct Params
    {
        uint16_t port;
        Poco::Net::StreamSocketImpl * impl;
    };

    silk::FiberFuture client_future;
    silk::FiberScheduler::run(
        [](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port));

            socket.sendBytes("ping", 4);
            char prime[4];
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
            EXPECT_TRUE(socket.poll(Poco::Timespan(0, 500'000), Poco::Net::Socket::SELECT_READ));

            char data[1];
            EXPECT_EQ(socket.receiveBytes(data, 1), 1);

            socket.setReceiveTimeout(Poco::Timespan(0, 100'000));
            EXPECT_THROW(socket.receiveBytes(data, sizeof(data)), Poco::TimeoutException);
            socket.close();
            return 0;
        },
        Params{port, this->policy.makeClient()},
        &client_future);

    auto peer = listener.acceptConnection();
    char prime[4];
    int received = 0;
    while (received < 4)
    {
        int n = peer.receiveBytes(prime + received, 4 - received);
        ASSERT_GT(n, 0);
        received += n;
    }
    peer.sendBytes("pong", 4);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
    silk::FiberScheduler::run(
        [](Params * p) noexcept -> int
        {
            Poco::Net::StreamSocket socket(p->impl);
            EXPECT_THROW(
                socket.connect(Poco::Net::SocketAddress("127.0.0.1", p->port)),
                Poco::Net::ConnectionRefusedException);
            return 0;
        },
        Params{closed_port, this->policy.makeClient()},
        &client_future);

    client_future.wait();
}


#endif
