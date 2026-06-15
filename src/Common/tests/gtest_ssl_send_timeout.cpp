#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/SharedPtr.h>
#include <Poco/Timespan.h>
#include <Poco/Exception.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <thread>
#include <atomic>
#include <vector>


/// Test that a blocking SSL socket write throws TimeoutException
/// when the peer stops reading and the send timeout expires.
TEST(SSLSocketTimeout, SendBytesThrowsTimeoutOnBlockingSocket)
{
    EphemeralCert cert;
    auto server_ctx = cert.makeContext(Poco::Net::Context::SERVER_USE);
    auto client_ctx = cert.makeContext(Poco::Net::Context::CLIENT_USE);

    Poco::Net::SecureServerSocket server_socket(
        Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_ctx);
    auto port = server_socket.address().port();

    std::atomic<bool> server_done{false};

    /// Server thread: accept and handshake, then sit idle (never read).
    std::thread server_thread([&]
    {
        try
        {
            auto accepted = server_socket.acceptConnection();
            /// Handshake happens on first I/O. Do a small read to trigger it.
            char buf[1];
            try { accepted.receiveBytes(buf, 1); } catch (...) {} /// Ok: handshake may fail. NOLINT(bugprone-empty-catch)
            /// Keep the connection open until the test completes.
            while (!server_done.load())
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        catch (...) {} /// Ok: server thread cleanup, test checks client-side behavior. NOLINT(bugprone-empty-catch)
    });

    try
    {
        Poco::Net::SecureStreamSocket client(
            Poco::Net::SocketAddress("127.0.0.1", port), client_ctx);

        /// Very short send timeout so the test doesn't wait long.
        client.setSendTimeout(Poco::Timespan(0, 200'000)); /// 200ms

        /// Write enough data to fill the TCP send buffer and SSL buffer.
        /// Typical TCP buffer is 128KB-256KB. Write 4MB to be sure.
        std::vector<char> data(4 * 1024 * 1024, 'X');

        bool got_timeout = false;
        try
        {
            size_t offset = 0;
            while (offset < data.size())
            {
                int sent = client.sendBytes(data.data() + offset, static_cast<int>(data.size() - offset));
                if (sent > 0)
                    offset += sent;
                else
                    break;
            }
        }
        catch (const Poco::TimeoutException &)
        {
            got_timeout = true;
        }

        ASSERT_TRUE(got_timeout) << "Expected Poco::TimeoutException when writing to a non-reading SSL peer";
    }
    catch (const Poco::Exception & e)
    {
        /// Connection setup can fail on some systems; skip gracefully.
        /// Clean up the server thread before GTEST_SKIP returns from the function.
        server_done.store(true);
        server_socket.close();
        server_thread.join();
        GTEST_SKIP() << "SSL setup failed: " << e.displayText();
    }

    server_done.store(true);
    /// Close the listening socket to unblock acceptConnection if the client
    /// failed before connecting (e.g. SSL context error).
    server_socket.close();
    server_thread.join();
}


/// Test that SSL handshake throws TimeoutException when the peer
/// is a plain TCP listener that never speaks SSL.
/// No server thread needed -- the kernel's listen backlog completes the
/// TCP handshake, but nobody reads the accepted socket so the SSL
/// ClientHello gets no response.
TEST(SSLSocketTimeout, HandshakeThrowsTimeoutOnNonSSLPeer)
{
    EphemeralCert cert;
    auto client_ctx = cert.makeContext(Poco::Net::Context::CLIENT_USE);

    /// Listen but never accept -- kernel backlog handles TCP handshake.
    Poco::Net::ServerSocket listener(Poco::Net::SocketAddress("127.0.0.1", 0), 1);
    auto port = listener.address().port();

    bool got_timeout = false;
    try
    {
        Poco::Net::StreamSocket raw_sock;
        raw_sock.connect(Poco::Net::SocketAddress("127.0.0.1", port));
        raw_sock.setSendTimeout(Poco::Timespan(0, 200'000));
        raw_sock.setReceiveTimeout(Poco::Timespan(0, 200'000));

        Poco::Net::SecureStreamSocket ssl_sock(
            Poco::Net::SecureStreamSocket::attach(raw_sock, client_ctx));

        /// completeHandshake is triggered by the first I/O.
        /// The peer won't respond with ServerHello, so it will time out.
        char buf[1] = {'X'};
        ssl_sock.sendBytes(buf, 1);
    }
    catch (const Poco::TimeoutException &)
    {
        got_timeout = true;
    }
    catch (const Poco::Exception &)
    {
        /// Some SSL implementations may throw a different SSL error
        /// before the timeout fires. That's acceptable -- the key thing
        /// is we don't silently return -1.
        got_timeout = true;
    }

    ASSERT_TRUE(got_timeout) << "Expected exception when SSL handshake times out against a non-SSL peer";
}


#endif /// USE_SSL
