#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/ByteOrder.h>
#include <Poco/Exception.h>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/SecureServerSocket.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <exception>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>

#include <sys/ioctl.h>

namespace
{

using namespace std::chrono_literals;

constexpr int32_t TIMEOUT_MS = 15000;
constexpr auto MAX_DURATION = 120s;

/// The size of the request and of the response. By default, a send buffer grows to at most 4 MiB on Linux, and cannot hold the
/// request. With a larger limit, the test may pass also without what it checks.
constexpr size_t LARGE = 8 << 20;

/// For the accepted socket.
constexpr int SERVER_BUFFER_SIZE = 65536;

std::string toWire(auto value)
{
    value = Poco::ByteOrder::toBigEndian(value);
    return {reinterpret_cast<const char *>(&value), sizeof(value)};
}

/// Does not read while it writes, like Keeper.
class FakeKeeper
{
public:
    explicit FakeKeeper(Poco::Net::ServerSocket & listener_) : listener(listener_), thread([this] { run(); }) { }

    ~FakeKeeper()
    {
        stop = true;
        thread.join();
    }

private:
    void run()
    {
        while (!stop)
        {
            try
            {
                if (listener.poll(Poco::Timespan(0, 100'000), Poco::Net::Socket::SELECT_READ))
                    serve();
            }
            catch (const Poco::IOException &) /// Ok: the client went away. It can come again. NOLINT(bugprone-empty-catch)
            {
            }
            catch (const std::exception & e)
            {
                ADD_FAILURE() << "FakeKeeper: " << e.what();
            }
        }
    }

    void serve()
    {
        socket = listener.acceptConnection();
        socket.setReceiveTimeout({MAX_DURATION.count(), 0});
        socket.setSendTimeout({MAX_DURATION.count(), 0});

        receive(receiveSize());
        send(toWire(Coordination::SERVER_HANDSHAKE_LENGTH) + toWire(Coordination::ZOOKEEPER_PROTOCOL_VERSION) + toWire(TIMEOUT_MS)
            + toWire(Poco::Int64{1}) + toWire(int32_t{Coordination::PASSWORD_LENGTH}) + std::string(Coordination::PASSWORD_LENGTH, '\0'));

        while (true)
        {
            const size_t size = receiveSize();
            const auto xid = receiveInt();
            const auto op = static_cast<Coordination::OpNum>(receiveInt());
            const std::string request = receive(size - 8);

            auto error = Coordination::Error::ZOK;
            std::string body;
            if (op == Coordination::OpNum::Create)
                body = toWire(int32_t{2}) + "/x";
            else if (op == Coordination::OpNum::Get && request.contains("/large"))
            {
                waitUntilNothingArrives();
                body = toWire(static_cast<int32_t>(LARGE)) + std::string(LARGE, 'v') + std::string(68 /* stat */, '\0');
            }
            else if (op == Coordination::OpNum::Get)
                error = Coordination::Error::ZNONODE;
            send(toWire(static_cast<int32_t>(16 + body.size())) + toWire(xid) + toWire(Poco::Int64{1})
                + toWire(static_cast<int32_t>(error)) + body);
        }
    }

    /// Waits until the client, which is sending its next request, cannot send more of it. What it has in its send buffer cannot
    /// be seen from here, and there is no event to wait for, so this looks until there is much in the receive buffer, and it has
    /// not changed for a while. If that is too early, the test is weaker, but it does not fail.
    void waitUntilNothingArrives() const
    {
        const auto deadline = std::chrono::steady_clock::now() + MAX_DURATION / 2;
        int last_available = 0;
        for (int unchanged = 0; unchanged < 10; ++unchanged)
        {
            if (std::chrono::steady_clock::now() > deadline)
                throw std::runtime_error("the client does not send its next request, or does not stop");
            std::this_thread::sleep_for(20ms);
            int available = 0;
            socket.impl()->ioctl(FIONREAD, available);
            if (available != last_available || available < socket.impl()->getReceiveBufferSize() / 4)
                unchanged = 0;
            last_available = available;
        }
    }

    void send(const std::string & data) { socket.sendBytes(data.data(), static_cast<int>(data.size())); }

    std::string receive(size_t size)
    {
        std::string data(size, '\0');
        for (size_t received = 0; received < size;)
        {
            const int rc = socket.receiveBytes(data.data() + received, static_cast<int>(size - received));
            if (rc <= 0)
                throw Poco::IOException("connection closed");
            received += static_cast<size_t>(rc);
        }
        return data;
    }

    size_t receiveSize()
    {
        const int32_t size = receiveInt();
        if (size < 8 || static_cast<size_t>(size) > 2 * LARGE)
            throw std::runtime_error("unexpected size " + std::to_string(size));
        return static_cast<size_t>(size);
    }

    int32_t receiveInt()
    {
        int32_t value = 0;
        memcpy(&value, receive(sizeof(value)).data(), sizeof(value));
        return Poco::ByteOrder::fromBigEndian(value);
    }

    Poco::Net::ServerSocket & listener;
    Poco::Net::StreamSocket socket;
    std::atomic<bool> stop{false};
    std::thread thread;
};

class ZooKeeperClientIO : public testing::TestWithParam<bool /* tls13 */>
{
};

}

/// The client has to read a response while its request does not go out, because the server reads the rest of the request
/// only after it has got rid of the response.
TEST_P(ZooKeeperClientIO, LargeRequestDuringLargeResponse)
{
    EphemeralCert cert;
    auto server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    if (!GetParam())
        server_context->disableProtocols(Poco::Net::Context::PROTO_TLSV1_3);
    Poco::Net::SecureServerSocket listener(Poco::Net::SocketAddress("127.0.0.1", 0), 4, server_context);
    listener.setReceiveBufferSize(SERVER_BUFFER_SIZE);
    listener.setSendBufferSize(SERVER_BUFFER_SIZE);

    /// The client uses the default context. Until the end of the test, this overrules the handler that comes with it.
    Poco::Net::SSLManager::instance().defaultClientContext();
    Poco::Net::AcceptCertificateHandler accept_certificate(/* handleErrorsOnServerSide = */ false);

    FakeKeeper keeper(listener);

    std::promise<std::string> value_promise;
    std::promise<Coordination::Error> create_promise;
    auto value = value_promise.get_future();
    auto create_result = create_promise.get_future();

    zkutil::ShuffleHost host;
    host.host = "127.0.0.1:" + std::to_string(listener.address().port());
    host.secure = true;
    zkutil::ZooKeeperArgs args;
    args.connection_timeout_ms = TIMEOUT_MS;
    args.operation_timeout_ms = TIMEOUT_MS;
    Coordination::ZooKeeper zookeeper({host}, args, nullptr, nullptr);
    zookeeper.get("/large", [&](const Coordination::GetResponse & response) { value_promise.set_value(response.data); }, {});
    zookeeper.create("/x", std::string(LARGE, 'c'), false, false, {},
        [&](const Coordination::CreateResponse & response) { create_promise.set_value(response.error); });

    const auto deadline = std::chrono::steady_clock::now() + MAX_DURATION;
    while (create_result.wait_for(10ms) != std::future_status::ready && !zookeeper.isExpired())
        if (std::chrono::steady_clock::now() > deadline)
            break;
    ASSERT_FALSE(zookeeper.isExpired());
    ASSERT_EQ(create_result.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(create_result.get(), Coordination::Error::ZOK);
    ASSERT_EQ(value.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(value.get() == std::string(LARGE, 'v'));
}

INSTANTIATE_TEST_SUITE_P(TLS, ZooKeeperClientIO, testing::Bool(), [](const auto & param) { return param.param ? "TLS13" : "TLS12"; });

#endif
