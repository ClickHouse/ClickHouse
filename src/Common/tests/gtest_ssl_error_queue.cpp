#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/SocketAddress.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/sslerr.h>

#include <atomic>
#include <exception>
#include <latch>
#include <memory>
#include <stdexcept>
#include <thread>

#include <sys/socket.h>


namespace
{

void leaveShutdownWhileInInit(Poco::Net::Context::Ptr context)
{
    ERR_clear_error();
    SSL * ssl = SSL_new(context->sslContext());
    ASSERT_NE(ssl, nullptr);
    SSL_set_connect_state(ssl);
    ASSERT_LT(SSL_shutdown(ssl), 0);
    SSL_free(ssl);

    const unsigned long error = ERR_peek_last_error();
    /// Unwrap the error manually because OpenSSL declares `ERR_GET_LIB` and
    /// `ERR_GET_REASON` with `ossl_unused`, which causes compiler warnings when used.
    ASSERT_EQ((error >> ERR_LIB_OFFSET) & ERR_LIB_MASK, ERR_LIB_SSL);
    ASSERT_EQ(error & ERR_REASON_MASK, SSL_R_SHUTDOWN_WHILE_IN_INIT);
}

class LiveTLSPair
{
public:
    LiveTLSPair()
        : server_context(cert.makeContext(Poco::Net::Context::SERVER_USE))
        , client_context(cert.makeContext(Poco::Net::Context::CLIENT_USE))
        , listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_context)
        , server_thread([this]
        {
            runServer();
        })
    {
        try
        {
            client = std::make_unique<Poco::Net::SecureStreamSocket>(
                Poco::Net::SocketAddress("127.0.0.1", listener.address().port()), client_context);
            if (client->sendBytes("x", 1) != 1)
                throw std::runtime_error("TLS client could not send the handshake byte");
            server_ready.wait();
            if (server_exception)
                std::rethrow_exception(server_exception);
        }
        catch (...)
        {
            stop();
            throw;
        }
    }

    ~LiveTLSPair()
    {
        stop();
    }

    void sendMalformedRecord()
    {
        send_malformed_record.store(true, std::memory_order_release);
        action_requested.count_down();
        action_was_requested = true;
        action_done.wait();
        if (server_exception)
            std::rethrow_exception(server_exception);
    }

    Poco::Net::SecureStreamSocket & getClient()
    {
        return *client;
    }

    Poco::Net::Context::Ptr getClientContext() const
    {
        return client_context;
    }

private:
    void runServer()
    {
        try
        {
            Poco::Net::SecureStreamSocket peer(listener.acceptConnection());
            char byte = 0;
            if (peer.receiveBytes(&byte, 1) != 1)
                throw std::runtime_error("TLS server did not receive the handshake byte");

            server_ready.count_down();
            action_requested.wait();

            if (send_malformed_record.load(std::memory_order_acquire))
            {
                /// A syntactically TLS-looking application-data record with an invalid payload.
                /// It bypasses the server's `SSL` object and causes a fatal record-layer error on
                /// the client while leaving the underlying TCP connection open.
                constexpr unsigned char malformed_record[] = {0x17, 0x03, 0x03, 0x00, 0x01, 0xff};
                const ssize_t sent = ::send(
                    peer.impl()->sockfd(), malformed_record, sizeof(malformed_record), MSG_NOSIGNAL);
                if (sent != static_cast<ssize_t>(sizeof(malformed_record)))
                    throw std::runtime_error("TLS server could not send the malformed record");
            }

            action_done.count_down();
            finish.wait();
            peer.abort();
        }
        catch (...)
        {
            server_exception = std::current_exception();
            server_ready.count_down();
            action_done.count_down();
        }
    }

    void stop() noexcept
    {
        if (stopped)
            return;
        stopped = true;

        if (client)
        {
            try
            {
                client->abort();
            }
            catch (...)
            {
            }
        }

        if (!action_was_requested)
            action_requested.count_down();
        finish.count_down();
        try
        {
            listener.close();
        }
        catch (...)
        {
        }
        if (server_thread.joinable())
            server_thread.join();
    }

    EphemeralCert cert;
    Poco::Net::Context::Ptr server_context;
    Poco::Net::Context::Ptr client_context;
    Poco::Net::SecureServerSocket listener;
    std::latch server_ready{1};
    std::latch action_requested{1};
    std::latch action_done{1};
    std::latch finish{1};
    std::atomic<bool> send_malformed_record{false};
    std::exception_ptr server_exception;
    std::thread server_thread;
    std::unique_ptr<Poco::Net::SecureStreamSocket> client;
    bool action_was_requested = false;
    bool stopped = false;
};

}


TEST(SSLErrorQueue, StaleErrorDoesNotChangeNonBlockingReadRetry)
{
    LiveTLSPair pair;
    auto & client = pair.getClient();
    client.setBlocking(false);

    char byte = 0;
    leaveShutdownWhileInInit(pair.getClientContext());
    EXPECT_EQ(client.receiveBytes(&byte, 1), Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ);
    EXPECT_EQ(ERR_peek_error(), 0UL);

    /// The external async socket path retries `receiveBytes` after polling. The queue must be
    /// cleared for every attempt, not only for the first operation on the connection.
    leaveShutdownWhileInInit(pair.getClientContext());
    EXPECT_EQ(client.receiveBytes(&byte, 1), Poco::Net::SecureStreamSocket::ERR_SSL_WANT_READ);
    EXPECT_EQ(ERR_peek_error(), 0UL);
}


TEST(SSLErrorQueue, FatalReadSkipsTLSShutdown)
{
    LiveTLSPair pair;
    pair.sendMalformedRecord();

    char byte = 0;
    EXPECT_THROW(pair.getClient().receiveBytes(&byte, 1), Poco::Net::SSLException);

    /// `shutdown` must close the transport directly. Calling `SSL_shutdown` after the fatal read
    /// would raise another SSL exception and could contaminate the thread's error queue.
    EXPECT_NO_THROW(pair.getClient().shutdown());
    EXPECT_EQ(ERR_peek_error(), 0UL);
}

#endif
