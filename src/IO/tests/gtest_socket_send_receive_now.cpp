#include <gtest/gtest.h>

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/SocketImpl.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/Throttler.h>

#include <poll.h>

#include <memory>
#include <string>
#include <vector>

namespace
{

constexpr int select_read = Poco::Net::SocketImpl::SELECT_READ;
constexpr int select_write = Poco::Net::SocketImpl::SELECT_WRITE;

/// A connected pair of plain TCP sockets on the loopback interface. The timeouts make a test fail
/// instead of hang when a call blocks by mistake.
struct LoopbackPair
{
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0)};
    Poco::Net::StreamSocket client;
    Poco::Net::StreamSocket server;

    LoopbackPair()
    {
        client.connect(listener.address());
        server = listener.acceptConnection();
        client.setReceiveTimeout(Poco::Timespan(5, 0));
        client.setSendTimeout(Poco::Timespan(5, 0));
        server.setReceiveTimeout(Poco::Timespan(5, 0));
    }

    Poco::Net::SocketImpl & socket() { return *client.impl(); }

    /// Sends `answer` from the server, and then resets the connection. Returns after the reset has
    /// arrived at the client.
    void answerAndReset(const std::string & answer)
    {
        server.sendBytes(answer.data(), static_cast<int>(answer.size()));
        server.setLinger(true, 0);
        server.close();

        /// Only an error or a hang-up wakes a `poll` with no events: the answer alone does not.
        pollfd fd{.fd = client.impl()->sockfd(), .events = 0, .revents = 0};
        ASSERT_EQ(::poll(&fd, 1, 5000), 1);
        ASSERT_NE(fd.revents & (POLLERR | POLLHUP), 0);
    }
};

/// Records how the socket calls the throttler, and never waits.
struct RecordingThrottler : public Poco::Net::Throttler
{
    bool throttle(size_t /*amount*/, size_t max_block_ns) override
    {
        max_block_ns_of_calls.push_back(max_block_ns);
        return false;
    }

    std::vector<size_t> max_block_ns_of_calls;
};

}


/// A peer that reads nothing fills the socket: then `sendNow` returns 0 at once, where a blocking
/// send would wait for the send timeout.
TEST(SocketSendReceiveNow, SendNowReturnsZeroWhenTheSocketIsFull)
{
    LoopbackPair pair;
    pair.client.setSendBufferSize(4096);

    const std::string data(64 * 1024, 'x');
    size_t total = 0;
    int sent = 0;
    while ((sent = pair.socket().sendNow(data.data(), static_cast<int>(data.size()))) > 0)
        total += static_cast<size_t>(sent);

    EXPECT_EQ(sent, 0);
    EXPECT_GT(total, 0U);

    Poco::Timespan timeout(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read | select_write), 0);
}


TEST(SocketSendReceiveNow, WaitReportsTheDirectionsThatAreReady)
{
    LoopbackPair pair;

    Poco::Timespan timeout(5, 0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read | select_write), select_write);

    Poco::Timespan check(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(check, select_read), 0);

    pair.server.sendBytes("abc", 3);
    timeout = Poco::Timespan(5, 0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read), select_read);
    timeout = Poco::Timespan(5, 0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read | select_write), select_read | select_write);

    char buffer[16];
    EXPECT_EQ(pair.socket().receiveNow(buffer, static_cast<int>(sizeof(buffer))), 3);
    EXPECT_EQ(std::string(buffer, 3), "abc");
    EXPECT_EQ(pair.socket().receiveNow(buffer, static_cast<int>(sizeof(buffer))), 0);
}


TEST(SocketSendReceiveNow, WaitSpendsTheTimeout)
{
    LoopbackPair pair;

    Poco::Timespan timeout(0, 20'000);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read), 0);
    EXPECT_EQ(timeout.totalMicroseconds(), 0);
}


/// The peer answers and resets the connection before the next send. The send fails, but the answer
/// is not lost: the error is held until the answer is read.
TEST(SocketSendReceiveNow, AnswerIsReadableAfterTheResetThatFailsTheSend)
{
    LoopbackPair pair;
    const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    ASSERT_NO_FATAL_FAILURE(pair.answerAndReset(answer));

    const std::string body(1024, 'x');
    EXPECT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), -1);
    EXPECT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), -1);

    Poco::Timespan timeout(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read | select_write), select_read | select_write);

    std::string received(answer.size() + 16, '\0');
    const int length = pair.socket().receiveNow(received.data(), static_cast<int>(received.size()));
    ASSERT_GT(length, 0);
    EXPECT_EQ(received.substr(0, static_cast<size_t>(length)), answer);
    EXPECT_EQ(pair.socket().receiveNow(received.data(), static_cast<int>(received.size())), -1);

    EXPECT_THROW(pair.socket().throwHeldSendError(), Poco::Net::ConnectionResetException);
}


/// A session reconnects with the same socket object: the error of the old connection must not stay.
TEST(SocketSendReceiveNow, NewConnectionHasNoHeldError)
{
    LoopbackPair pair;
    ASSERT_NO_FATAL_FAILURE(pair.answerAndReset("x"));

    const std::string body(1024, 'x');
    ASSERT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), -1);

    pair.client.close();
    pair.client.connect(pair.listener.address());
    pair.server = pair.listener.acceptConnection();

    EXPECT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), static_cast<int>(body.size()));
    EXPECT_THROW(pair.socket().throwHeldSendError(), Poco::IllegalStateException);
}


/// A non-blocking throttle only books the budget. `sendNow` must let the throttler wait, as a
/// blocking send does, or the send would not be slowed down at all.
TEST(SocketSendReceiveNow, SendNowLetsTheThrottlerWait)
{
    LoopbackPair pair;
    auto throttler = std::make_shared<RecordingThrottler>();
    pair.client.setSendThrottler(throttler);

    const std::string data(100, 'x');
    EXPECT_EQ(pair.socket().sendNow(data.data(), static_cast<int>(data.size())), 100);

    ASSERT_EQ(throttler->max_block_ns_of_calls.size(), 1U);
    EXPECT_GT(throttler->max_block_ns_of_calls[0], 0U);
}


TEST(SocketSendReceiveNow, ReceiveNowReportsTheEndOfTheStream)
{
    LoopbackPair pair;
    pair.server.shutdownSend();

    Poco::Timespan timeout(5, 0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read), select_read);

    char buffer[16];
    EXPECT_EQ(pair.socket().receiveNow(buffer, static_cast<int>(sizeof(buffer))), -1);
}
