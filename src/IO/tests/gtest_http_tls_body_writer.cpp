#include "config.h"

#include <gtest/gtest.h>

#if USE_SSL

#include <IO/HTTP/HTTPClientIO.h>
#include <IO/WriteHelpers.h>
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/Exception.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPSession.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <openssl/ssl.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>


namespace
{

/// A TLS 1.2 server that reads the request header, asks the client to renegotiate, and then stops:
/// it answers neither the handshake nor the body.
class StalledRenegotiationServer
{
public:
    StalledRenegotiationServer()
    {
        SSL_CTX_set_max_proto_version(server_context->sslContext(), TLS1_2_VERSION);
        thread = std::thread([this] { run(); });
    }

    ~StalledRenegotiationServer()
    {
        stop.set_value();
        thread.join();
    }

    uint16_t port() const { return listener.address().port(); }
    Poco::Net::Context::Ptr clientContext() const { return client_context; }

    /// Waits until the server has asked for the renegotiation.
    void waitForRenegotiationRequest() { requested_future.wait(); }

private:
    void run()
    {
        Poco::Net::StreamSocket peer;
        try
        {
            if (listener.poll(Poco::Timespan(10, 0), Poco::Net::Socket::SELECT_READ))
            {
                peer = listener.acceptConnection();
                std::string header;
                char ch = 0;
                while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
                    header += ch;

                peer.setBlocking(false);
                SSL * ssl = static_cast<Poco::Net::SecureStreamSocketImpl *>(peer.impl())->ssl();
                EXPECT_EQ(SSL_renegotiate(ssl), 1);
                /// Sends the request to renegotiate; the answer of the client is never read.
                SSL_do_handshake(ssl);
            }
        }
        catch (const Poco::Exception & e)
        {
            ADD_FAILURE() << e.displayText();
        }
        requested.set_value();
        stop_future.wait();
    }

    EphemeralCert cert;
    Poco::Net::Context::Ptr server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    Poco::Net::Context::Ptr client_context = cert.makeContext(Poco::Net::Context::CLIENT_USE);
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 64, server_context};
    std::promise<void> requested;
    std::future<void> requested_future = requested.get_future();
    std::promise<void> stop;
    std::future<void> stop_future = stop.get_future();
    std::thread thread;
};


/// A TLS 1.2 server that reads the request header, sends `answer` with one `SSL_write`, and then
/// stops: it reads neither the body nor anything else.
class EarlyAnswerServer
{
public:
    explicit EarlyAnswerServer(std::string answer_)
        : answer(std::move(answer_))
    {
        SSL_CTX_set_max_proto_version(server_context->sslContext(), TLS1_2_VERSION);
        thread = std::thread([this] { run(); });
    }

    ~EarlyAnswerServer()
    {
        stop.set_value();
        thread.join();
    }

    uint16_t port() const { return listener.address().port(); }
    Poco::Net::Context::Ptr clientContext() const { return client_context; }

private:
    void run()
    {
        Poco::Net::StreamSocket peer;
        try
        {
            if (listener.poll(Poco::Timespan(10, 0), Poco::Net::Socket::SELECT_READ))
            {
                peer = listener.acceptConnection();
                std::string header;
                char ch = 0;
                while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
                    header += ch;
                EXPECT_EQ(peer.sendBytes(answer.data(), static_cast<int>(answer.size())), static_cast<int>(answer.size()));
            }
        }
        catch (const Poco::Exception & e)
        {
            ADD_FAILURE() << e.displayText();
        }
        stop_future.wait();
    }

    const std::string answer;
    EphemeralCert cert;
    Poco::Net::Context::Ptr server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    Poco::Net::Context::Ptr client_context = cert.makeContext(Poco::Net::Context::CLIENT_USE);
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 64, server_context};
    std::promise<void> stop;
    std::future<void> stop_future = stop.get_future();
    std::thread thread;
};


/// Reports the first send that would block: a TLS socket has a record write pending then.
struct FirstBlockedSend : public Poco::Net::IHTTPSessionDataHooks
{
    void atStart(int /*bytes*/) override {}
    void atFinish(int /*bytes*/) override {}
    void atFail() override
    {
        if (!reported.exchange(true))
            blocked.set_value();
    }

    std::atomic<bool> reported = false;
    std::promise<void> blocked;
};


/// A TLS 1.2 server that does not read the body until the client has a record write pending. Then it
/// asks the client to renegotiate, reads the whole body, and answers `200`.
class RenegotiationDuringPendingWriteServer
{
public:
    RenegotiationDuringPendingWriteServer(std::shared_future<void> client_blocked_, size_t body_size_)
        : client_blocked(std::move(client_blocked_))
        , body_size(body_size_)
    {
        SSL_CTX_set_max_proto_version(server_context->sslContext(), TLS1_2_VERSION);
        thread = std::thread([this] { run(); });
    }

    ~RenegotiationDuringPendingWriteServer()
    {
        stop.set_value();
        thread.join();
    }

    uint16_t port() const { return listener.address().port(); }
    Poco::Net::Context::Ptr clientContext() const { return client_context; }

private:
    void run()
    {
        Poco::Net::StreamSocket peer;
        try
        {
            if (!listener.poll(Poco::Timespan(10, 0), Poco::Net::Socket::SELECT_READ))
                return;
            peer = listener.acceptConnection();
            std::string header;
            char ch = 0;
            while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
                header += ch;

            if (client_blocked.wait_for(std::chrono::seconds(10)) != std::future_status::ready)
            {
                ADD_FAILURE() << "The client did not block on a send";
                return;
            }

            peer.setBlocking(false);
            SSL * ssl = static_cast<Poco::Net::SecureStreamSocketImpl *>(peer.impl())->ssl();
            EXPECT_EQ(SSL_renegotiate(ssl), 1);
            SSL_do_handshake(ssl);
            peer.setBlocking(true);

            char buf[16384];
            size_t received = 0;
            while (received < body_size)
            {
                const int n = peer.receiveBytes(buf, sizeof(buf));
                if (n <= 0)
                {
                    ADD_FAILURE() << "The body ended after " << received << " bytes";
                    return;
                }
                received += static_cast<size_t>(n);
            }

            const std::string answer = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
            peer.sendBytes(answer.data(), static_cast<int>(answer.size()));
        }
        catch (const Poco::Exception & e)
        {
            ADD_FAILURE() << e.displayText();
        }
        stop_future.wait();
    }

    std::shared_future<void> client_blocked;
    const size_t body_size;
    EphemeralCert cert;
    Poco::Net::Context::Ptr server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    Poco::Net::Context::Ptr client_context = cert.makeContext(Poco::Net::Context::CLIENT_USE);
    Poco::Net::SecureServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 64, server_context};
    std::promise<void> stop;
    std::future<void> stop_future = stop.get_future();
    std::thread thread;
};

}


/// During a renegotiation, `SSL_write` waits for the socket to become readable while it stays
/// writable. The body writer must wait for that, and the send timeout must still end the wait.
TEST(HTTPTLSBodyWriter, StalledRenegotiationTimesOut)
{
    StalledRenegotiationServer server;
    Poco::Net::HTTPSClientSession session("127.0.0.1", server.port(), server.clientContext());
    session.setTimeout(Poco::Timespan(5, 0), Poco::Timespan(1, 0), Poco::Timespan(5, 0));

    const std::string body(1 << 20, 'x');
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentLength(body.size());
    auto request_body = DB::sendHTTPRequest(session, request);

    server.waitForRenegotiationRequest();
    ASSERT_TRUE(session.socket().poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));

    EXPECT_THROW(
        {
            DB::writeString(body, *request_body);
            request_body->finalize();
        },
        Poco::TimeoutException);
}

/// OpenSSL allows no other operation that can do I/O while a record write is pending: a read that
/// starts a renegotiation then would have to write the handshake on top of it. The body writer
/// reads only when no record write is pending.
TEST(HTTPTLSBodyWriter, RenegotiationDuringPendingWrite)
{
    constexpr size_t body_size = 16 << 20;
    auto hooks = std::make_shared<FirstBlockedSend>();
    RenegotiationDuringPendingWriteServer server(hooks->blocked.get_future().share(), body_size);
    Poco::Net::HTTPSClientSession session("127.0.0.1", server.port(), server.clientContext());
    session.setTimeout(Poco::Timespan(5, 0), Poco::Timespan(5, 0), Poco::Timespan(5, 0));
    session.setSendDataHooks(hooks);

    const std::string body(body_size, 'x');
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentLength(body.size());
    auto request_body = DB::sendHTTPRequest(session, request);
    session.socket().setSendBufferSize(64 * 1024);

    DB::writeString(body, *request_body);
    request_body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_OK);
}

/// `SSL_read` takes a whole TLS record from the socket, and keeps what does not fit into the read
/// buffer of the session: `poll` does not see these bytes. Here an interim response longer than the
/// buffer and the final error come in one record.
TEST(HTTPTLSBodyWriter, ErrorAfterLongInterimResponseInOneRecord)
{
    std::string answer = "HTTP/1.1 100 Continue\r\n";
    for (size_t i = 0; i < 60; ++i)
        answer += "X-Pad-" + std::to_string(i) + ": " + std::string(180, 'p') + "\r\n";
    answer += "\r\nHTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    ASSERT_GT(answer.size(), 8192);
    ASSERT_LT(answer.size(), 16384);

    EarlyAnswerServer server(answer);
    Poco::Net::HTTPSClientSession session("127.0.0.1", server.port(), server.clientContext());
    session.setTimeout(Poco::Timespan(5, 0), Poco::Timespan(2, 0), Poco::Timespan(5, 0));

    const std::string body(16 << 20, 'x');
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentLength(body.size());
    auto request_body = DB::sendHTTPRequest(session, request);
    ASSERT_TRUE(session.socket().poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));

    DB::writeString(body, *request_body);
    request_body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
}

#endif
