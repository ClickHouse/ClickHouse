#include <Storages/Kafka/Clients/Net/TCPTransport.h>

#include <gtest/gtest.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <chrono>
#include <thread>

using namespace Kafka;
using namespace Kafka::Net;
using namespace std::chrono_literals;

namespace
{

/// Listening socket on an ephemeral loopback port.
Poco::Net::ServerSocket makeServer()
{
    return Poco::Net::ServerSocket(Poco::Net::SocketAddress("127.0.0.1", 0));
}

}

TEST(KafkaClientTCPTransport, ConnectWriteReadRoundTrip)
{
    auto server = makeServer();
    std::thread echo(
        [&server]
        {
            Poco::Net::StreamSocket peer = server.acceptConnection();
            char buf[5];
            int got = 0;
            while (got < 5)
                got += peer.receiveBytes(buf + got, 5 - got);
            peer.sendBytes(buf, 5);
        });

    TCPTransport transport(nullptr);
    transport.connect("127.0.0.1", server.address().port(), Deadline::after(5s));
    EXPECT_TRUE(transport.isOpen());

    transport.writeAll("hello", Deadline::after(5s));
    char buf[5];
    size_t got = 0;
    while (got < 5)
        got += transport.readSome(buf + got, 5 - got, Deadline::after(5s));
    EXPECT_EQ(String(buf, 5), "hello");

    echo.join();
}

TEST(KafkaClientTCPTransport, ReadHonorsDeadline)
{
    auto server = makeServer();
    std::thread silent(
        [&server]
        {
            Poco::Net::StreamSocket peer = server.acceptConnection();
            /// Send nothing; wait for the client to hang up.
            char byte;
            try
            {
                peer.receiveBytes(&byte, 1);
            }
            catch (...) /// NOLINT(bugprone-empty-catch)
            {
            }
        });

    TCPTransport transport(nullptr);
    transport.connect("127.0.0.1", server.address().port(), Deadline::after(5s));

    char byte;
    try
    {
        transport.readSome(&byte, 1, Deadline::after(100ms));
        FAIL() << "expected a timeout";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Timeout);
    }

    transport.close();
    silent.join();
}

TEST(KafkaClientTCPTransport, WakeupInterruptsBlockedRead)
{
    auto server = makeServer();
    std::thread silent(
        [&server]
        {
            Poco::Net::StreamSocket peer = server.acceptConnection();
            char byte;
            try
            {
                peer.receiveBytes(&byte, 1);
            }
            catch (...) /// NOLINT(bugprone-empty-catch)
            {
            }
        });

    auto wakeup = std::make_shared<WakeupSource>();
    TCPTransport transport(wakeup);
    transport.connect("127.0.0.1", server.address().port(), Deadline::after(5s));

    std::thread interrupter(
        [&wakeup]
        {
            std::this_thread::sleep_for(50ms);
            wakeup->wakeup();
        });

    char byte;
    try
    {
        transport.readSome(&byte, 1, Deadline::after(30s));
        FAIL() << "expected a wakeup";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Wakeup);
    }

    interrupter.join();
    transport.close();
    silent.join();
}

TEST(KafkaClientTCPTransport, ConnectToClosedPortFails)
{
    /// Grab an ephemeral port and close the listener so the connect is refused.
    UInt16 port;
    {
        auto server = makeServer();
        port = server.address().port();
    }

    TCPTransport transport(nullptr);
    try
    {
        transport.connect("127.0.0.1", port, Deadline::after(5s));
        FAIL() << "expected a connection failure";
    }
    catch (const Kafka::Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Connection);
    }
    EXPECT_FALSE(transport.isOpen());
}

TEST(KafkaClientTCPTransport, ReadableReflectsBufferedData)
{
    auto server = makeServer();
    std::thread responder(
        [&server]
        {
            Poco::Net::StreamSocket peer = server.acceptConnection();
            /// Respond only after the client's trigger byte, so the client can
            /// assert readable() == false before it.
            char trigger;
            int got = 0;
            while (got < 1)
                got += peer.receiveBytes(&trigger, 1);
            peer.sendBytes("!", 1);
        });

    TCPTransport transport(nullptr);
    transport.connect("127.0.0.1", server.address().port(), Deadline::after(5s));
    EXPECT_FALSE(transport.readable());

    transport.writeAll("x", Deadline::after(5s));
    const auto deadline = Deadline::after(5s);
    while (!transport.readable() && !deadline.expired())
        std::this_thread::sleep_for(1ms);
    EXPECT_TRUE(transport.readable());

    char byte;
    ASSERT_EQ(transport.readSome(&byte, 1, Deadline::after(5s)), 1u);
    EXPECT_EQ(byte, '!');

    responder.join();
}
