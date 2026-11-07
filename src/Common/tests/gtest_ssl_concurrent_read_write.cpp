#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Timespan.h>
#include <Poco/Exception.h>

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <base/scope_guard.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <optional>
#include <thread>
#include <vector>


namespace
{

/// Large enough that neither direction fits into the socket buffers of both ends, so that each
/// writer has to wait for the reader on the other side.
constexpr size_t PAYLOAD_SIZE = 32 * 1024 * 1024;
constexpr int CHUNK_SIZE = 1024 * 1024;

void sendAll(Poco::Net::StreamSocket & socket, const std::vector<char> & data)
{
    size_t offset = 0;
    while (offset < data.size())
    {
        const int to_send = static_cast<int>(std::min<size_t>(CHUNK_SIZE, data.size() - offset));
        const int sent = socket.sendBytes(data.data() + offset, to_send);
        ASSERT_GT(sent, 0);
        offset += static_cast<size_t>(sent);
    }
}

void receiveExactly(Poco::Net::StreamSocket & socket, size_t size)
{
    std::vector<char> buffer(CHUNK_SIZE);
    size_t received = 0;
    while (received < size)
    {
        const int to_receive = static_cast<int>(std::min<size_t>(buffer.size(), size - received));
        const int rc = socket.receiveBytes(buffer.data(), to_receive);
        ASSERT_GT(rc, 0) << "peer closed the connection after " << received << " of " << size << " bytes";
        received += static_cast<size_t>(rc);
    }
}

}


/// One thread writes a large request to a blocking TLS socket while another thread reads a large
/// reply from it, against a peer that, like a single-threaded server, does not read until it has
/// written. This is how a multiplexing client such as the ZooKeeper client uses its connection.
///
/// `SecureSocketImpl` serialises `SSL_read` and `SSL_write` with one lock. It used to hold that
/// lock while waiting for the socket, so the writer, waiting for a peer that was itself waiting
/// for our reader, kept the reader out: nothing moved until a socket timeout expired. A plain
/// socket has no such lock, and the same exchange over it always completes.
TEST(SSLSocketConcurrentReadWrite, WriterWaitingForPeerDoesNotBlockReader)
{
    EphemeralCert cert;
    auto server_ctx = cert.makeContext(Poco::Net::Context::SERVER_USE);
    auto client_ctx = cert.makeContext(Poco::Net::Context::CLIENT_USE);

    Poco::Net::SecureServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_ctx);
    const auto port = server_socket.address().port();

    /// The budget after which the unfixed socket gave up with `Poco::TimeoutException`. The
    /// exchange itself takes a fraction of it.
    const Poco::Timespan client_timeout(10, 0);
    /// Longer than the client's, so that a stall surfaces on the side under test.
    const Poco::Timespan server_timeout(20, 0);

    const std::vector<char> request(PAYLOAD_SIZE, 'q');
    const std::vector<char> reply(PAYLOAD_SIZE, 'r');

    std::exception_ptr server_exception;
    std::thread server_thread([&]
    {
        try
        {
            auto peer = server_socket.acceptConnection();
            peer.setSendTimeout(server_timeout);
            peer.setReceiveTimeout(server_timeout);
            /// Write the whole reply before reading anything, then acknowledge the request with
            /// one more byte. Whichever client thread the lock favours, the unfixed client stalls:
            /// either its writer waits for this read while keeping its reader out, or its reader,
            /// having drained the reply, waits for the acknowledgement while keeping its writer out.
            sendAll(peer, reply);
            receiveExactly(peer, request.size());
            const char ack = 'a';
            ASSERT_EQ(peer.sendBytes(&ack, 1), 1);
            /// Keep the connection open until the client has read everything and shuts it down.
            char end = 0;
            try
            {
                peer.receiveBytes(&end, 1);
            }
            catch (...) /// Ok: how the client ends the connection is not under test. NOLINT(bugprone-empty-catch)
            {
            }
        }
        catch (...)
        {
            server_exception = std::current_exception();
        }
    });
    /// Declared after the thread so that it runs before the thread is destroyed on every exit path;
    /// closing the listener also unblocks `acceptConnection` if the client never connected.
    SCOPE_EXIT({
        server_socket.close();
        if (server_thread.joinable())
            server_thread.join();
    });

    std::optional<Poco::Net::SecureStreamSocket> client;
    try
    {
        client.emplace(Poco::Net::SocketAddress("127.0.0.1", port), client_ctx);
    }
    catch (const Poco::Exception & e)
    {
        GTEST_SKIP() << "SSL setup failed: " << e.displayText();
    }
    client->setSendTimeout(client_timeout);
    client->setReceiveTimeout(client_timeout);
    ASSERT_TRUE(client->getBlocking());

    const auto started = std::chrono::steady_clock::now();

    std::exception_ptr writer_exception;
    std::thread writer_thread([&]
    {
        try
        {
            sendAll(*client, request);
        }
        catch (...)
        {
            writer_exception = std::current_exception();
        }
    });

    std::exception_ptr reader_exception;
    try
    {
        receiveExactly(*client, reply.size() + 1);
    }
    catch (...)
    {
        reader_exception = std::current_exception();
    }

    writer_thread.join();
    /// Lets the server thread finish.
    try
    {
        client->shutdown();
    }
    catch (...) /// Ok: after a failed exchange the shutdown may fail too; the exchange is what is checked. NOLINT(bugprone-empty-catch)
    {
    }

    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();

    auto description = [](const std::exception_ptr & exception) -> std::string
    {
        if (!exception)
            return "none";
        try
        {
            std::rethrow_exception(exception);
        }
        catch (const Poco::Exception & e)
        {
            return e.displayText();
        }
        catch (const std::exception & e)
        {
            return e.what();
        }
        catch (...) /// Ok: only the text for the failure message is made here, the exception itself is already recorded.
        {
            return "unknown";
        }
    };

    EXPECT_FALSE(reader_exception) << "reader: " << description(reader_exception);
    EXPECT_FALSE(writer_exception) << "writer: " << description(writer_exception);
    EXPECT_LT(elapsed_ms, client_timeout.totalMilliseconds())
        << "the exchange took " << elapsed_ms << " ms, so it only completed after a socket timeout";

    server_thread.join();
    EXPECT_FALSE(server_exception) << "server: " << description(server_exception);
}


#endif
