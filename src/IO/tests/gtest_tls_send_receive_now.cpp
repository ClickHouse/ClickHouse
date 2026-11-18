#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/Exception.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/Throttler.h>

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <poll.h>

#include <cerrno>

#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace
{

constexpr int select_read = Poco::Net::SocketImpl::SELECT_READ;
constexpr int select_write = Poco::Net::SocketImpl::SELECT_WRITE;

/// A TLS connection on the loopback interface, with the handshake done on both sides. The
/// timeouts make a test fail instead of hang when a call blocks by mistake.
struct TLSLoopbackPair
{
    EphemeralCert cert;
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 1, cert.makeContext(Poco::Net::Context::SERVER_USE)};
    Poco::Net::SecureStreamSocket client{cert.makeContext(Poco::Net::Context::CLIENT_USE)};
    Poco::Net::StreamSocket server;

    TLSLoopbackPair()
    {
        const Poco::Timespan timeout(5, 0);

        /// The TCP connection needs no `accept`, so a failure of the client cannot leave the
        /// server thread waiting for a connection.
        client.setLazyHandshake(true);
        client.connect(Poco::Net::SocketAddress("127.0.0.1", listener.address().port()));
        client.setSendTimeout(timeout);
        client.setReceiveTimeout(timeout);

        std::exception_ptr server_exception;
        {
            std::jthread server_thread([&]
            {
                try
                {
                    server = listener.acceptConnection();
                    server.setSendTimeout(timeout);
                    server.setReceiveTimeout(timeout);
                    static_cast<Poco::Net::SecureStreamSocketImpl *>(server.impl())->completeHandshake();
                }
                catch (...)
                {
                    server_exception = std::current_exception();
                }
            });
            static_cast<Poco::Net::SecureStreamSocketImpl *>(client.impl())->completeHandshake();
        }
        if (server_exception)
            std::rethrow_exception(server_exception);
    }

    Poco::Net::SocketImpl & socket() { return *client.impl(); }

    SSL * ssl() { return static_cast<Poco::Net::SecureStreamSocketImpl *>(client.impl())->ssl(); }

    /// The encrypted bytes that the client has queued.
    size_t queued() { return static_cast<size_t>(BIO_wpending(SSL_get_wbio(ssl()))); }

    /// Receives at least one byte with `receiveNow`. Records without data, such as session
    /// tickets, can arrive first.
    int receiveSome(char * buffer, int length)
    {
        while (true)
        {
            Poco::Timespan timeout(5, 0);
            if (socket().waitForSendOrReceive(timeout, select_read) == 0)
                return 0;
            const int received = socket().receiveNow(buffer, length);
            if (received != 0)
                return received;
        }
    }

    /// Fills the socket and the receive window of the server, which does not read. Returns the number
    /// of plaintext bytes that `sendNow` accepted. The kernel moves the send buffer to the server
    /// while the window is open, so the socket is full only when it stays unwritable.
    size_t fill()
    {
        const std::string data(64 * 1024, 'x');
        size_t total = 0;
        while (true)
        {
            int sent = 0;
            while ((sent = socket().sendNow(data.data(), static_cast<int>(data.size()))) > 0)
                total += static_cast<size_t>(sent);
            EXPECT_EQ(sent, 0);
            Poco::Timespan timeout(0, 200'000);
            if (socket().waitForSendOrReceive(timeout, select_write) == 0)
                return total;
        }
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

/// Throws on its first call, and never waits.
struct ThrowingOnceThrottler : public Poco::Net::Throttler
{
    bool throttle(size_t /*amount*/, size_t /*max_block_ns*/) override
    {
        if (!thrown)
        {
            thrown = true;
            throw Poco::IOException("The send limit is exceeded");
        }
        return false;
    }

    bool thrown = false;
};

}


/// The server answers and resets the connection. The send fails, but OpenSSL never sees the failure:
/// the answer is decrypted afterwards, and the error is held.
TEST(TLSSendReceiveNow, AnswerIsReadableAfterTheResetThatFailsTheSend)
{
    TLSLoopbackPair pair;
    const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    pair.server.sendBytes(answer.data(), static_cast<int>(answer.size()));
    pair.server.setLinger(true, 0);
    pair.server.close();

    /// Only an error or a hang-up wakes a `poll` with no events: the answer alone does not.
    pollfd fd{.fd = pair.client.impl()->sockfd(), .events = 0, .revents = 0};
    ASSERT_EQ(::poll(&fd, 1, 5000), 1);

    const std::string body(1024, 'x');
    EXPECT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), -1);
    EXPECT_EQ(pair.socket().sendNow(body.data(), static_cast<int>(body.size())), -1);

    std::string received(answer.size() + 16, '\0');
    const int length = pair.receiveSome(received.data(), static_cast<int>(received.size()));
    ASSERT_GT(length, 0);
    EXPECT_EQ(received.substr(0, static_cast<size_t>(length)), answer);
    /// The `close_notify` of the server.
    EXPECT_EQ(pair.receiveSome(received.data(), static_cast<int>(received.size())), -1);

    /// A TLS socket sends `close_notify` and shuts the TCP connection down before the reset, so
    /// the reset arrives after the end of the stream, and the send fails with `EPIPE`.
    try
    {
        pair.socket().throwHeldSendError();
        FAIL() << "No send error is held";
    }
    catch (const Poco::IOException & e)
    {
        EXPECT_EQ(e.code(), EPIPE) << e.displayText();
    }
}


/// A peer that reads nothing fills the socket: then `sendNow` returns 0 at once, and the filter
/// holds at most one record.
TEST(TLSSendReceiveNow, QueueHoldsAtMostOneRecord)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);

    EXPECT_GT(pair.fill(), 0U);
    EXPECT_GT(pair.queued(), 0U);
    EXPECT_LE(pair.queued(), static_cast<size_t>(SSL3_RT_HEADER_LENGTH + SSL3_RT_MAX_PLAIN_LENGTH + SSL3_RT_MAX_ENCRYPTED_OVERHEAD));

    const std::string data(100, 'x');
    EXPECT_EQ(pair.socket().sendNow(data.data(), static_cast<int>(data.size())), 0);

    Poco::Timespan timeout(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_write), 0);
}


/// A blocking receive sends the queued bytes first: the server waits for the whole body before it
/// answers.
TEST(TLSSendReceiveNow, BlockingReceiveSendsTheQueuedBytesFirst)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);
    const size_t body_size = pair.fill();
    ASSERT_GT(pair.queued(), 0U);

    std::exception_ptr server_exception;
    std::jthread server_thread([&]
    {
        try
        {
            std::string buffer(64 * 1024, '\0');
            size_t received = 0;
            while (received < body_size)
            {
                const int length = pair.server.receiveBytes(buffer.data(), static_cast<int>(buffer.size()));
                if (length <= 0)
                    throw Poco::Net::NetException("The body ended early");
                received += static_cast<size_t>(length);
            }
            pair.server.sendBytes("done", 4);
        }
        catch (...)
        {
            server_exception = std::current_exception();
        }
    });

    char answer[16];
    const int length = pair.client.receiveBytes(answer, static_cast<int>(sizeof(answer)));
    server_thread.join();
    if (server_exception)
        std::rethrow_exception(server_exception);
    ASSERT_EQ(length, 4);
    EXPECT_EQ(std::string(answer, 4), "done");
    EXPECT_EQ(pair.queued(), 0U);
}


/// The server answers and resets the connection while encrypted bytes of the body are queued. A
/// blocking receive gets the answer: the reset does not reach OpenSSL as a failed write.
TEST(TLSSendReceiveNow, BlockingReceiveGetsTheAnswerBeforeAReset)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);
    pair.fill();
    ASSERT_GT(pair.queued(), 0U);

    const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    pair.server.sendBytes(answer.data(), static_cast<int>(answer.size()));
    pair.server.setLinger(true, 0);
    pair.server.close();
    pollfd fd{.fd = pair.client.impl()->sockfd(), .events = 0, .revents = 0};
    ASSERT_EQ(::poll(&fd, 1, 5000), 1);

    std::string received(answer.size() + 16, '\0');
    const int length = pair.client.receiveBytes(received.data(), static_cast<int>(received.size()));
    ASSERT_GT(length, 0);
    EXPECT_EQ(received.substr(0, static_cast<size_t>(length)), answer);
}


/// The server answers and does not read the rest of the body. A blocking receive gets the answer
/// without waiting for the queued bytes to leave.
TEST(TLSSendReceiveNow, BlockingReceiveGetsTheAnswerOfAPeerThatDoesNotRead)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);
    pair.fill();
    ASSERT_GT(pair.queued(), 0U);

    const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    pair.server.sendBytes(answer.data(), static_cast<int>(answer.size()));

    std::string received(answer.size() + 16, '\0');
    const int length = pair.client.receiveBytes(received.data(), static_cast<int>(received.size()));
    ASSERT_GT(length, 0);
    EXPECT_EQ(received.substr(0, static_cast<size_t>(length)), answer);
}


/// A wait for data alone sends the queued bytes: the server waits for the whole body before it
/// answers.
TEST(TLSSendReceiveNow, WaitForDataSendsTheQueuedBytes)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);
    const size_t body_size = pair.fill();
    ASSERT_GT(pair.queued(), 0U);

    std::exception_ptr server_exception;
    std::jthread server_thread([&]
    {
        try
        {
            std::string buffer(64 * 1024, '\0');
            size_t received = 0;
            while (received < body_size)
            {
                const int length = pair.server.receiveBytes(buffer.data(), static_cast<int>(buffer.size()));
                if (length <= 0)
                    throw Poco::Net::NetException("The body ended early");
                received += static_cast<size_t>(length);
            }
            pair.server.sendBytes("done", 4);
        }
        catch (...)
        {
            server_exception = std::current_exception();
        }
    });

    char answer[16];
    const int length = pair.receiveSome(answer, static_cast<int>(sizeof(answer)));
    server_thread.join();
    if (server_exception)
        std::rethrow_exception(server_exception);
    ASSERT_EQ(length, 4);
    EXPECT_EQ(std::string(answer, 4), "done");
    EXPECT_EQ(pair.queued(), 0U);
}


/// A blocking send sends the queued bytes first, so that the stream keeps its order.
TEST(TLSSendReceiveNow, BlockingSendKeepsTheOrder)
{
    TLSLoopbackPair pair;
    pair.client.setSendBufferSize(4096);
    const size_t body_size = pair.fill();
    ASSERT_GT(pair.queued(), 0U);

    std::string received;
    std::exception_ptr server_exception;
    std::jthread server_thread([&]
    {
        try
        {
            std::string buffer(64 * 1024, '\0');
            while (received.size() < body_size + 3)
            {
                const int length = pair.server.receiveBytes(buffer.data(), static_cast<int>(buffer.size()));
                if (length <= 0)
                    throw Poco::Net::NetException("The stream ended early");
                received.append(buffer.data(), static_cast<size_t>(length));
            }
        }
        catch (...)
        {
            server_exception = std::current_exception();
        }
    });

    ASSERT_EQ(pair.client.sendBytes("end", 3), 3);
    server_thread.join();
    if (server_exception)
        std::rethrow_exception(server_exception);
    ASSERT_EQ(received.size(), body_size + 3);
    EXPECT_EQ(received.substr(body_size), "end");
    EXPECT_EQ(received.find_first_not_of('x'), body_size);
}


/// The rest of a record that OpenSSL has decrypted is not visible to a `poll` of the descriptor.
TEST(TLSSendReceiveNow, WaitSeesDataThatOpenSSLHasDecrypted)
{
    TLSLoopbackPair pair;
    pair.server.sendBytes("abcdef", 6);

    char buffer[3];
    ASSERT_EQ(pair.receiveSome(buffer, 3), 3);
    EXPECT_EQ(std::string(buffer, 3), "abc");

    Poco::Timespan timeout(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read), select_read);
    ASSERT_EQ(pair.socket().receiveNow(buffer, 3), 3);
    EXPECT_EQ(std::string(buffer, 3), "def");

    timeout = Poco::Timespan(0);
    EXPECT_EQ(pair.socket().waitForSendOrReceive(timeout, select_read), 0);
}


/// `sendNow` lets the throttler wait once for each write, as a blocking send does.
TEST(TLSSendReceiveNow, SendNowThrottlesOnce)
{
    TLSLoopbackPair pair;
    auto throttler = std::make_shared<RecordingThrottler>();
    pair.client.setSendThrottler(throttler);

    ASSERT_EQ(pair.socket().sendNow("abc", 3), 3);
    ASSERT_EQ(throttler->max_block_ns_of_calls.size(), 1U);
    EXPECT_GT(throttler->max_block_ns_of_calls[0], 0U);
}


/// The throttler waits outside OpenSSL: a wait inside a BIO callback could suspend a fiber in the
/// middle of an OpenSSL call. An error of the throttler then leaves the connection usable; inside a
/// callback, it would fail the TLS connection.
TEST(TLSSendReceiveNow, ThrottlerErrorKeepsTheConnection)
{
    TLSLoopbackPair pair;
    auto throttler = std::make_shared<ThrowingOnceThrottler>();
    pair.client.setSendThrottler(throttler);

    EXPECT_THROW(pair.socket().sendNow("abc", 3), Poco::IOException);
    ASSERT_EQ(pair.socket().sendNow("def", 3), 3);

    char buffer[3];
    ASSERT_EQ(pair.server.receiveBytes(buffer, 3), 3);
    EXPECT_EQ(std::string(buffer, 3), "def");
}

#endif
