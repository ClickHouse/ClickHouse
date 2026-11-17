#include "config.h"

#include <gtest/gtest.h>

#include <IO/HTTP/HTTPClientIO.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPResponseParser.h>
#include <Poco/Net/HTTPSession.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#if USE_SSL
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#endif

#include <poll.h>

#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace
{

const Poco::Timespan socket_timeout(5, 0);

/// A connected pair of plain TCP sockets on the loopback interface.
struct PlainPolicy
{
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0)};

    void connect(Poco::Net::StreamSocket & client, Poco::Net::StreamSocket & server)
    {
        client.connect(listener.address());
        server = listener.acceptConnection();
    }
};

#if USE_SSL
/// A TLS connection on the loopback interface, with the handshake done on both sides, and with the
/// records that the server sends after the handshake read by the client: an answer then arrives alone.
struct SecurePolicy
{
    EphemeralCert cert;
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 1, cert.makeContext(Poco::Net::Context::SERVER_USE)};

    void connect(Poco::Net::StreamSocket & client, Poco::Net::StreamSocket & server)
    {
        Poco::Net::SecureStreamSocket secure_client(cert.makeContext(Poco::Net::Context::CLIENT_USE));
        /// The TCP connection needs no `accept`, so a failure of the client cannot leave the
        /// server thread waiting for a connection.
        secure_client.setLazyHandshake(true);
        secure_client.connect(Poco::Net::SocketAddress("127.0.0.1", listener.address().port()));
        secure_client.setSendTimeout(socket_timeout);
        secure_client.setReceiveTimeout(socket_timeout);

        std::exception_ptr server_exception;
        {
            std::jthread server_thread([&]
            {
                try
                {
                    server = listener.acceptConnection();
                    server.setSendTimeout(socket_timeout);
                    server.setReceiveTimeout(socket_timeout);
                    static_cast<Poco::Net::SecureStreamSocketImpl *>(server.impl())->completeHandshake();
                    server.sendBytes("!", 1);
                }
                catch (...)
                {
                    server_exception = std::current_exception();
                }
            });
            static_cast<Poco::Net::SecureStreamSocketImpl *>(secure_client.impl())->completeHandshake();
        }
        if (server_exception)
            std::rethrow_exception(server_exception);

        char ready = 0;
        ASSERT_EQ(secure_client.receiveBytes(&ready, 1), 1);
        client = secure_client;
    }
};
#endif

/// A server that answers a request as soon as it has read the request header, before the body.
/// The client and the server run on one thread: the answer is in the receive buffer of the client
/// before the client writes the body, so the client sees it while it sends the body.
template <typename Policy>
class HTTPEarlyResponseTest : public ::testing::Test
{
protected:
    static constexpr size_t body_size = 1 << 20;

    Policy policy;
    Poco::Net::StreamSocket peer;
    std::unique_ptr<Poco::Net::HTTPClientSession> session;

    void SetUp() override
    {
        Poco::Net::StreamSocket client;
        ASSERT_NO_FATAL_FAILURE(policy.connect(client, peer));
        peer.setReceiveTimeout(socket_timeout);
        /// The session has no host, so it keeps the connected socket.
        session = std::make_unique<Poco::Net::HTTPClientSession>(client);
        session->setTimeout(socket_timeout, socket_timeout, Poco::Timespan(2, 0));
    }

    /// Sends the request header, and reads it on the server.
    std::unique_ptr<DB::HTTPRequestBodyWriteBuffer> send(const Poco::Net::HTTPHeaderLimits & limits = {})
    {
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
        request.setContentLength(body_size);
        auto body = DB::sendHTTPRequest(*session, request, DB::DBMS_DEFAULT_BUFFER_SIZE, nullptr, nullptr, limits);

        std::string header;
        char ch = 0;
        while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
            header += ch;
        return body;
    }

    /// Sends the request header, and answers it with `answer` before the body.
    std::unique_ptr<DB::HTTPRequestBodyWriteBuffer> sendAndAnswer(const std::string & answer, const Poco::Net::HTTPHeaderLimits & limits = {})
    {
        auto body = send(limits);
        peer.sendBytes(answer.data(), static_cast<int>(answer.size()));
        EXPECT_TRUE(session->socket().poll(socket_timeout, Poco::Net::Socket::SELECT_READ));
        return body;
    }

    /// Resets the connection from the server. Returns after the reset has arrived at the client.
    void reset()
    {
        peer.setLinger(true, 0);
        peer.close();

        /// Only an error or a hang-up wakes a `poll` with no events: data alone does not.
        pollfd fd{.fd = session->socket().impl()->sockfd(), .events = 0, .revents = 0};
        ASSERT_EQ(::poll(&fd, 1, 5000), 1);
    }
};

/// Runs `answer` once, in the first send that starts after `armed` is set. The data hooks and the
/// send throttler wait in the same place, before the send.
struct AnswerBeforeSend : public Poco::Net::IHTTPSessionDataHooks
{
    explicit AnswerBeforeSend(std::function<void()> answer_)
        : answer(std::move(answer_))
    {
    }

    void atStart(int /*bytes*/) override
    {
        if (armed && !answered)
        {
            answered = true;
            answer();
        }
    }
    void atFinish(int /*bytes*/) override {}
    void atFail() override {}

    bool armed = false;
    bool answered = false;
    std::function<void()> answer;
};

#if USE_SSL
using Policies = ::testing::Types<PlainPolicy, SecurePolicy>;
#else
using Policies = ::testing::Types<PlainPolicy>;
#endif

}

TYPED_TEST_SUITE(HTTPEarlyResponseTest, Policies);


/// An error that arrives before the body stops it: nothing of the body is sent, the error is
/// received, and the request stays incomplete, so the connection is not reused.
TYPED_TEST(HTTPEarlyResponseTest, ErrorStopsTheBody)
{
    auto body = this->sendAndAnswer("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    DB::writeString(std::string(this->body_size, 'x'), *body);
    body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(*this->session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    EXPECT_FALSE(this->session->isRequestBodyComplete());
    EXPECT_FALSE(this->peer.poll(Poco::Timespan(0, 100'000), Poco::Net::Socket::SELECT_READ));
}


/// A field longer than the read buffer of the session, but within its limit, is accepted also when
/// it arrives while the body is sent.
TYPED_TEST(HTTPEarlyResponseTest, LongFieldWithinItsLimit)
{
    const std::string value(9000, 'v');
    Poco::Net::HTTPHeaderLimits limits;
    limits.value_length = 16384;
    auto body = this->sendAndAnswer("HTTP/1.1 403 Forbidden\r\nX-Long: " + value + "\r\nContent-Length: 0\r\n\r\n", limits);
    DB::writeString(std::string(this->body_size, 'x'), *body);
    body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(*this->session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
    EXPECT_EQ(response.get("X-Long"), value);
}

/// The field limit holds before the header ends: a peer that keeps sending fields gets an error at
/// the first field over the limit, instead of making the client collect them all.
TYPED_TEST(HTTPEarlyResponseTest, FieldLimitHoldsBeforeTheHeaderEnds)
{
    std::string answer = "HTTP/1.1 404 Not Found\r\n";
    for (size_t i = 0; i < 200; ++i)
        answer += "X-" + std::to_string(i) + ": v\r\n";
    auto body = this->sendAndAnswer(answer);

    try
    {
        DB::writeString(std::string(this->body_size, 'x'), *body);
        body->finalize();
        Poco::Net::HTTPResponse response;
        DB::receiveHTTPResponse(*this->session, response);
        FAIL() << "A response header with too many fields was accepted";
    }
    catch (const Poco::Net::MessageException & e)
    {
        EXPECT_EQ(e.message(), "Too many header fields");
    }
}

/// The server answers and resets the connection after the socket was found writable, while the
/// send waits in the data hooks. The send fails, but the answer that arrived before the reset is
/// the result of the request.
TYPED_TEST(HTTPEarlyResponseTest, AnswerBeforeTheResetThatFailsTheSend)
{
    auto hooks = std::make_shared<AnswerBeforeSend>([this]
    {
        const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        this->peer.sendBytes(answer.data(), static_cast<int>(answer.size()));
        this->reset();
    });
    this->session->setSendDataHooks(hooks);

    auto body = this->send();
    hooks->armed = true;

    DB::writeString(std::string(this->body_size, 'x'), *body);
    body->finalize();
    EXPECT_TRUE(hooks->answered);

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(*this->session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
}


/// Without an answer, the error of the reset is the result of the request, as for a blocking send.
TYPED_TEST(HTTPEarlyResponseTest, ResetWithoutAnswerFailsTheSend)
{
    auto body = this->send();
    ASSERT_NO_FATAL_FAILURE(this->reset());

    EXPECT_THROW(
        {
            DB::writeString(std::string(this->body_size, 'x'), *body);
            body->finalize();
        },
        Poco::IOException);
}
