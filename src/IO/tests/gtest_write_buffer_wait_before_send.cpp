#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <IO/WriteBufferFromPocoSocket.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/Exception.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace
{

using namespace std::chrono_literals;

/// Only limits how long a failure takes.
constexpr auto PATIENCE = 30s;
constexpr int MIN_SIZE = 4096;
constexpr int MAX_TLS_RECORD_SIZE = 16384;

/// Records waits for the socket (with their timeouts) and writes (with their sizes), and calls the hook instead of waiting.
class HookedSocketImpl : public Poco::Net::SecureStreamSocketImpl
{
public:
    using Poco::Net::SecureStreamSocketImpl::SecureStreamSocketImpl;

    bool pollImpl(Poco::Timespan & timeout, int mode) override
    {
        events.emplace_back(mode == SELECT_WRITE ? "wait" : "other wait", timeout.totalMilliseconds());
        return hook ? hook() : Poco::Net::SecureStreamSocketImpl::pollImpl(timeout, mode);
    }

    int sendBytes(const void * buffer, int length, int flags) override
    {
        events.emplace_back("write", length);
        return Poco::Net::SecureStreamSocketImpl::sendBytes(buffer, length, flags);
    }

    std::function<bool()> hook;
    std::vector<std::pair<std::string, Int64>> events;
};

class WriteBufferWaitBeforeSend : public testing::Test
{
protected:
    void SetUp() override
    {
        impl->setLazyHandshake(true);
        impl->connect(listener.address());
        peer = listener.acceptConnection();
        for (Poco::Net::SocketImpl * s : {socket.impl(), peer.impl()})
        {
            s->setReceiveTimeout({PATIENCE.count(), 0});
            s->setSendTimeout({PATIENCE.count(), 0});
        }
        auto handshake = std::async(std::launch::async, [&] { return peer.sendBytes("data", 4); });
        ASSERT_EQ(impl->completeHandshake(), 1);
        ASSERT_EQ(handshake.get(), 4);
        out.emplace(socket);
        out->setWaitBeforeSend();
    }

    void TearDown() override
    {
        impl->hook = {};
        if (out)
            out->cancel();
    }

    std::string receive(size_t size)
    {
        std::string data(size, '\0');
        for (size_t received = 0; received < size;)
        {
            const int rc = peer.receiveBytes(data.data() + received, static_cast<int>(size - received));
            if (rc <= 0)
                return data.substr(0, received);
            received += static_cast<size_t>(rc);
        }
        return data;
    }

    void write(const std::string & data)
    {
        out->write(data.data(), data.size());
        out->next();
    }

    struct Socket : Poco::Net::Socket
    {
        explicit Socket(Poco::Net::SocketImpl * impl_) : Poco::Net::Socket(impl_) { }
    };

    EphemeralCert cert;
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 1, cert.makeContext(Poco::Net::Context::SERVER_USE)};
    HookedSocketImpl * impl = new HookedSocketImpl(cert.makeContext(Poco::Net::Context::CLIENT_USE));
    Socket socket{impl};
    Poco::Net::StreamSocket peer;
    std::optional<DB::WriteBufferFromPocoSocket> out;
};

}

/// Every write is preceded by a wait, and is small compared to the socket buffer, but not too small.
TEST_F(WriteBufferWaitBeforeSend, WritesAreSmall)
{
    for (const int buffer_size : {10'000, 150'000, 1'000'000})
    {
        impl->events.clear();
        socket.setSendBufferSize(buffer_size);
        std::string data(500'000, '\0');
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = static_cast<char>(i * 2654435761 >> 16);
        auto received = std::async(std::launch::async, [&] { return receive(data.size()); });
        write(data);
        EXPECT_TRUE(received.get() == data);

        const int actual_buffer_size = socket.getSendBufferSize();
        Int64 largest = 0;
        for (size_t i = 0; i < impl->events.size(); ++i)
        {
            EXPECT_EQ(impl->events[i].first, i % 2 ? "write" : "wait");
            if (i % 2)
                largest = std::max(largest, impl->events[i].second);
        }
        EXPECT_GE(largest, MIN_SIZE);
        EXPECT_LE(largest, MAX_TLS_RECORD_SIZE);
        EXPECT_LE(largest, std::max(MIN_SIZE, actual_buffer_size / 4));
        if (actual_buffer_size >= 16 * MAX_TLS_RECORD_SIZE)
            EXPECT_EQ(largest, MAX_TLS_RECORD_SIZE);
    }
}

/// If the socket does not become writable in time, the write is made anyway, and the next ones do not wait until it does.
TEST_F(WriteBufferWaitBeforeSend, WritesAfterWaitInVain)
{
    socket.setSendTimeout({20, 0});
    socket.setSendBufferSize(1'000'000);
    const std::string data(30'000, 'x');
    auto received = std::async(std::launch::async, [&] { return receive(3 * data.size()); });

    impl->hook = [calls = 0]() mutable
    {
        if (++calls > 100)
            throw Poco::IOException("too many waits");
        return false;
    };
    write(data);
    write(data);
    impl->hook = [] { return true; };
    write(data);
    EXPECT_TRUE(received.get() == data + data + data);

    using Events = std::vector<std::pair<std::string, Int64>>;
    ASSERT_GE(impl->events.size(), 7u);
    EXPECT_EQ(Events(impl->events.begin(), impl->events.begin() + 5),
        (Events{{"wait", 20'000}, {"write", 30'000}, {"wait", 0}, {"write", 30'000}, {"wait", 0}}));
    EXPECT_LE(impl->events[5].second, MAX_TLS_RECORD_SIZE);
    EXPECT_EQ(impl->events[6], (std::pair<std::string, Int64>("wait", 20'000)));
}

/// The wait does not prevent another thread from reading.
TEST_F(WriteBufferWaitBeforeSend, ReaderIsNotBlocked)
{
    std::promise<void> waiting;
    std::promise<void> writable;
    impl->hook = [&, writable_future = writable.get_future().share()]
    {
        waiting.set_value();
        return writable_future.wait_for(2 * PATIENCE) == std::future_status::ready;
    };
    auto writer = std::async(std::launch::async, [&] { write("x"); });
    EXPECT_EQ(waiting.get_future().wait_for(PATIENCE), std::future_status::ready);

    auto reader = std::async(std::launch::async, [&]
    {
        char data[4];
        return impl->receiveBytes(data, sizeof(data), 0);
    });
    EXPECT_EQ(reader.wait_for(PATIENCE), std::future_status::ready);
    writable.set_value();
    EXPECT_EQ(reader.get(), 4);
    EXPECT_NO_THROW(writer.get());
}

#endif
