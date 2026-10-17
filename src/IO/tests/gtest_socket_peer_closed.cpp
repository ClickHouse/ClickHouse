#include <gtest/gtest.h>

#include "config.h"

#include <IO/SocketPeerClosed.h>

#include <sys/socket.h>
#include <unistd.h>

#if USE_SSL
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <base/scope_guard.h>

#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Timespan.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <fcntl.h>

#include <atomic>
#include <exception>
#include <string>
#include <thread>
#include <vector>
#endif

using DB::SocketState;
using DB::getSocketState;
using DB::isSocketPeerClosed;

namespace
{

struct SocketPair
{
    int fds[2] = {-1, -1};
    SocketPair()
    {
        /// `SOCK_STREAM` is a self-referential macro; compute the call outside the assertion macro
        /// to avoid `-Wdisabled-macro-expansion`.
        const int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
        EXPECT_EQ(0, rc);
    }
    ~SocketPair()
    {
        for (int fd : fds)
            if (fd >= 0)
                ::close(fd);
    }
};

}

TEST(SocketPeerClosed, AliveNoData)
{
    SocketPair p;
    EXPECT_EQ(SocketState::Idle, getSocketState(p.fds[0]));
    EXPECT_EQ(SocketState::Idle, getSocketState(p.fds[1]));
    EXPECT_FALSE(isSocketPeerClosed(p.fds[0]));
    EXPECT_FALSE(isSocketPeerClosed(p.fds[1]));
}

TEST(SocketPeerClosed, DataPending)
{
    SocketPair p;
    const char payload = 'x';
    ASSERT_EQ(1, ::send(p.fds[1], &payload, 1, 0));
    /// Bytes are waiting on fds[0]: the connection is alive, not closed, but not idle either -
    /// a connection pool must not reuse it.
    EXPECT_EQ(SocketState::DataPending, getSocketState(p.fds[0]));
    EXPECT_FALSE(isSocketPeerClosed(p.fds[0]));
}

TEST(SocketPeerClosed, PeerClosed)
{
    SocketPair p;
    ::close(p.fds[1]);
    p.fds[1] = -1;
    EXPECT_EQ(SocketState::Closed, getSocketState(p.fds[0]));
    EXPECT_TRUE(isSocketPeerClosed(p.fds[0]));
}

TEST(SocketPeerClosed, ClosedWithPendingDataThenDrain)
{
    SocketPair p;
    const char payload = 'x';
    ASSERT_EQ(1, ::send(p.fds[1], &payload, 1, 0));
    ::close(p.fds[1]);
    p.fds[1] = -1;
    /// Data is still buffered: a read would return it, not EOF, so the state is `DataPending`,
    /// which alone disqualifies the connection from reuse. The peer is gone nevertheless.
    EXPECT_EQ(SocketState::DataPending, getSocketState(p.fds[0]));
    EXPECT_TRUE(isSocketPeerClosed(p.fds[0]));
    char tmp = 0;
    ASSERT_EQ(1, ::recv(p.fds[0], &tmp, 1, 0));
    /// Buffer drained, only the FIN remains, so report closed.
    EXPECT_EQ(SocketState::Closed, getSocketState(p.fds[0]));
    EXPECT_TRUE(isSocketPeerClosed(p.fds[0]));
}

/// A peer that sends data and then shuts down only its sending side, like a native client that
/// sends the external tables of a query waiting for admission and then disconnects: the unread data
/// stays in front of the FIN, but the peer reads as closed.
TEST(SocketPeerClosed, HalfClosedWithPendingData)
{
    SocketPair p;
    const char payload = 'x';
    ASSERT_EQ(1, ::send(p.fds[1], &payload, 1, 0));
    EXPECT_FALSE(isSocketPeerClosed(p.fds[0]));
    /// `SHUT_WR` is a self-referential macro, see `SocketPair`.
    const int rc = ::shutdown(p.fds[1], SHUT_WR);
    ASSERT_EQ(0, rc);
    EXPECT_EQ(SocketState::DataPending, getSocketState(p.fds[0]));
    EXPECT_TRUE(isSocketPeerClosed(p.fds[0]));
    /// The pending data is not consumed by the check.
    char tmp = 0;
    ASSERT_EQ(1, ::recv(p.fds[0], &tmp, 1, 0));
    EXPECT_EQ('x', tmp);
}

TEST(SocketPeerClosed, InvalidFd)
{
    EXPECT_EQ(SocketState::Closed, getSocketState(-1));
    EXPECT_TRUE(isSocketPeerClosed(-1));
}


#if USE_SSL

using DB::getSSLSocketState;
using DB::isSSLPeerClosed;

namespace
{

void setNonBlocking(int fd)
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(0, ::fcntl(fd, F_SETFL, flags | O_NONBLOCK));
}

/// A TLS connection over a `socketpair`, driven directly through OpenSSL (no Poco), so that tests
/// have full control over what the "server" leaves unread on the "client" socket.
struct TlsPair
{
    int fds[2] = {-1, -1};   /// fds[0] = server side, fds[1] = client side.
    SSL_CTX * server_ctx = nullptr;
    SSL_CTX * client_ctx = nullptr;
    SSL * server = nullptr;
    SSL * client = nullptr;

    explicit TlsPair(const EphemeralCert & cert)
    {
        const int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
        EXPECT_EQ(0, rc);
        /// Non-blocking so the handshake pump and `SSL_peek` never block.
        setNonBlocking(fds[0]);
        setNonBlocking(fds[1]);

        server_ctx = SSL_CTX_new(TLS_server_method());
        client_ctx = SSL_CTX_new(TLS_client_method());
        EXPECT_TRUE(server_ctx);
        EXPECT_TRUE(client_ctx);
        EXPECT_EQ(1, SSL_CTX_use_certificate_file(server_ctx, cert.cert_path.c_str(), SSL_FILETYPE_PEM));
        EXPECT_EQ(1, SSL_CTX_use_PrivateKey_file(server_ctx, cert.key_path.c_str(), SSL_FILETYPE_PEM));
        SSL_CTX_set_verify(client_ctx, SSL_VERIFY_NONE, nullptr);

        server = SSL_new(server_ctx);
        client = SSL_new(client_ctx);
        EXPECT_TRUE(server);
        EXPECT_TRUE(client);
        SSL_set_fd(server, fds[0]);
        SSL_set_fd(client, fds[1]);
        SSL_set_accept_state(server);
        SSL_set_connect_state(client);

        handshake();
    }

    void handshake() const
    {
        for (int i = 0; i < 200; ++i)
        {
            SSL_do_handshake(client);
            SSL_do_handshake(server);
            if (SSL_is_init_finished(client) && SSL_is_init_finished(server))
                return;
        }
        FAIL() << "TLS handshake did not complete";
    }

    ~TlsPair()
    {
        if (server)
            SSL_free(server);
        if (client)
            SSL_free(client);
        if (server_ctx)
            SSL_CTX_free(server_ctx);
        if (client_ctx)
            SSL_CTX_free(client_ctx);
        for (int fd : fds)
            if (fd >= 0)
                ::close(fd);
    }
};

}

/// A live secure connection with nothing (or only post-handshake tickets) pending reads as idle.
TEST(SocketPeerClosed, SecureAliveIdle)
{
    EphemeralCert cert;
    TlsPair p(cert);
    EXPECT_EQ(SocketState::Idle, getSSLSocketState(p.client));
    EXPECT_FALSE(isSSLPeerClosed(p.client));
}

/// A post-handshake session ticket - the record that made the old `poll` + `available()` check
/// misfire - must be treated as a live *idle* connection: not closed, and not pending data either,
/// so a connection pool keeps reusing it.
TEST(SocketPeerClosed, SecureSessionTicketIsIdle)
{
    EphemeralCert cert;
    TlsPair p(cert);

    /// Force a fresh session ticket and flush it to the client's socket buffer.
    ASSERT_EQ(1, SSL_new_session_ticket(p.server));
    for (int i = 0; i < 200; ++i)
    {
        SSL_do_handshake(p.server);
        char c = 0;
        if (::recv(p.fds[1], &c, 1, MSG_PEEK | MSG_DONTWAIT) > 0)
            break;
    }

    /// There is an unread record on the wire, ...
    char c = 0;
    const ssize_t peeked = ::recv(p.fds[1], &c, 1, MSG_PEEK | MSG_DONTWAIT);
    ASSERT_GT(peeked, 0);
    /// ... but it is a session ticket, so the connection is alive and idle.
    EXPECT_EQ(SocketState::Idle, getSSLSocketState(p.client));
    EXPECT_FALSE(isSSLPeerClosed(p.client));
}

/// Pending application data reads as alive-but-not-idle and is not consumed by the check.
/// For a pooled connection this means "do not reuse": the unread bytes would be misparsed by the
/// next borrower as the response to its own request.
TEST(SocketPeerClosed, SecureAppDataIsPendingAndNotConsumed)
{
    EphemeralCert cert;
    TlsPair p(cert);

    const char payload = 'x';
    ASSERT_EQ(1, SSL_write(p.server, &payload, 1));

    EXPECT_EQ(SocketState::DataPending, getSSLSocketState(p.client));
    EXPECT_FALSE(isSSLPeerClosed(p.client));

    /// The application byte is still there to be read.
    char got = 0;
    ASSERT_EQ(1, SSL_read(p.client, &got, 1));
    EXPECT_EQ('x', got);
}

/// An orderly TLS shutdown (`close_notify`) reads as closed. This is the case a raw `MSG_PEEK`
/// gets wrong: it sees the encrypted alert bytes as "data pending" and reports the dead connection
/// as alive. `SSL_peek` decrypts the alert and correctly reports it as closed.
TEST(SocketPeerClosed, SecureCloseNotifyIsClosed)
{
    EphemeralCert cert;
    TlsPair p(cert);

    SSL_shutdown(p.server);   /// Sends `close_notify`.

    /// The raw file-descriptor check misfires here (the alert looks like unread data), ...
    EXPECT_EQ(SocketState::DataPending, getSocketState(p.fds[1]));
    EXPECT_FALSE(isSocketPeerClosed(p.fds[1]));
    /// ... while the TLS-aware check correctly reports the connection as closed.
    EXPECT_EQ(SocketState::Closed, getSSLSocketState(p.client));
    EXPECT_TRUE(isSSLPeerClosed(p.client));
}

/// Only part of a TLS record has arrived: `SSL_peek` cannot decrypt it yet and returns
/// `SSL_ERROR_WANT_READ`, exactly like it does for a truly idle connection. Treating that as idle
/// would let a later borrower read the rest of this same record - once it finishes arriving - as
/// the response to its own unrelated request.
TEST(SocketPeerClosed, SecurePartialRecordIsPendingNotIdle)
{
    EphemeralCert cert;
    TlsPair p(cert);

    /// Redirect the server's I/O to memory BIOs so the test can control, byte by byte, how much of
    /// the ciphertext record reaches the client's socket. This replaces both the read and write BIO
    /// in one call, so the shared socket BIO installed by `SSL_set_fd` is released cleanly.
    BIO * server_rbio = BIO_new(BIO_s_mem());
    BIO * server_wbio = BIO_new(BIO_s_mem());
    ASSERT_TRUE(server_rbio);
    ASSERT_TRUE(server_wbio);
    SSL_set_bio(p.server, server_rbio, server_wbio);

    const char payload = 'x';
    ASSERT_EQ(1, SSL_write(p.server, &payload, 1));
    char record[256];
    const int record_len = BIO_read(server_wbio, record, sizeof(record));
    ASSERT_GT(record_len, 1);

    /// Deliver only the first byte of the record onto the real socket.
    ASSERT_EQ(1, ::send(p.fds[0], record, 1, 0));
    EXPECT_EQ(SocketState::DataPending, getSSLSocketState(p.client));
    EXPECT_FALSE(isSSLPeerClosed(p.client));

    /// Deliver the rest: the record is now complete and decryptable.
    ASSERT_EQ(record_len - 1, ::send(p.fds[0], record + 1, static_cast<size_t>(record_len - 1), 0));
    EXPECT_EQ(SocketState::DataPending, getSSLSocketState(p.client));
    char got = 0;
    ASSERT_EQ(1, SSL_read(p.client, &got, 1));
    EXPECT_EQ('x', got);
}

/// Application data followed by an abrupt close: the data is still pending, but the peer reads as closed.
TEST(SocketPeerClosed, SecureCloseBehindPendingDataIsClosed)
{
    EphemeralCert cert;
    TlsPair p(cert);

    const char payload = 'x';
    ASSERT_EQ(1, SSL_write(p.server, &payload, 1));
    EXPECT_FALSE(isSSLPeerClosed(p.client));

    ::close(p.fds[0]);
    p.fds[0] = -1;

    EXPECT_EQ(SocketState::DataPending, getSSLSocketState(p.client));
    EXPECT_TRUE(isSSLPeerClosed(p.client));
}

/// An abrupt close (FIN without `close_notify`) reads as closed.
TEST(SocketPeerClosed, SecureAbruptCloseIsClosed)
{
    EphemeralCert cert;
    TlsPair p(cert);

    ::close(p.fds[0]);
    p.fds[0] = -1;

    EXPECT_EQ(SocketState::Closed, getSSLSocketState(p.client));
    EXPECT_TRUE(isSSLPeerClosed(p.client));
}

/// The probe of a Poco secure socket may run concurrently with the socket's own I/O, like the HTTP
/// progress callback that checks for a client disconnect on pipeline threads while the response is
/// written. It must hold the socket's SSL lock, so that neither the `SSL` object nor the blocking mode
/// is changed under a concurrent `sendBytes`, which would corrupt the stream or fail the write.
TEST(SocketPeerClosed, SecureProbeConcurrentWithWrites)
{
    EphemeralCert cert;
    auto server_context = cert.makeContext(Poco::Net::Context::SERVER_USE);
    auto client_context = cert.makeContext(Poco::Net::Context::CLIENT_USE);

    Poco::Net::SecureServerSocket server_socket(Poco::Net::SocketAddress("127.0.0.1", 0), 1, server_context);
    const Poco::Net::SocketAddress server_address("127.0.0.1", server_socket.address().port());
    const Poco::Timespan timeout(30, 0);

    constexpr size_t num_chunks = 2000;
    constexpr size_t chunk_size = 1024;
    std::string chunk(chunk_size, '\0');
    for (size_t i = 0; i < chunk_size; ++i)
        chunk[i] = static_cast<char>('a' + i % 26);

    std::atomic<bool> writing_done = false;
    /// The client must stay connected until every probe has stopped: its `close_notify` may arrive
    /// right after the last chunk, before the probes see `writing_done`, and they would report it.
    std::atomic<bool> probes_stopped = false;
    auto stop_probes = [&](std::vector<std::jthread> & probe_threads)
    {
        writing_done = true;
        probe_threads.clear();
        probes_stopped = true;
        probes_stopped.notify_all();
    };

    std::exception_ptr client_exception;
    size_t client_received = 0;
    bool client_data_intact = true;
    std::jthread client_thread([&]
    {
        try
        {
            Poco::Net::SecureStreamSocket client(server_address, client_context);
            client.setReceiveTimeout(timeout);
            client.setSendTimeout(timeout);
            std::string buf(chunk_size, '\0');
            while (client_received < num_chunks * chunk_size)
            {
                const int n = client.receiveBytes(buf.data(), static_cast<int>(buf.size()));
                if (n <= 0)
                    break;
                for (int i = 0; i < n; ++i)
                    if (buf[i] != chunk[(client_received + i) % chunk_size])
                        client_data_intact = false;
                client_received += n;
            }
            probes_stopped.wait(false);
            client.close();
        }
        catch (...)
        {
            client_exception = std::current_exception();
        }
    });
    /// Release the client before it is joined if an assertion below returns before the probes start.
    SCOPE_EXIT({
        probes_stopped = true;
        probes_stopped.notify_all();
    });

    Poco::Net::StreamSocket accepted = server_socket.acceptConnection();
    accepted.setSendTimeout(timeout);
    accepted.setReceiveTimeout(timeout);
    auto * accepted_impl = static_cast<Poco::Net::SecureStreamSocketImpl *>(accepted.impl());
    ASSERT_EQ(accepted_impl->completeHandshake(), 1);
    ASSERT_TRUE(accepted.getBlocking());

    std::atomic<size_t> num_closed = 0;
    std::vector<std::jthread> probes;
    /// Stop the probes and release the client even if an assertion below returns early.
    SCOPE_EXIT({ stop_probes(probes); });
    for (size_t i = 0; i < 4; ++i)
    {
        probes.emplace_back([&]
        {
            while (!writing_done.load())
                if (getSocketState(accepted) == SocketState::Closed)
                    ++num_closed;
        });
    }

    for (size_t i = 0; i < num_chunks; ++i)
        ASSERT_EQ(accepted.sendBytes(chunk.data(), static_cast<int>(chunk.size())), static_cast<int>(chunk.size()));

    stop_probes(probes);
    client_thread.join();
    if (client_exception)
        std::rethrow_exception(client_exception);

    EXPECT_EQ(num_closed.load(), 0);
    EXPECT_EQ(client_received, num_chunks * chunk_size);
    EXPECT_TRUE(client_data_intact);
    /// The probes restored the blocking mode.
    EXPECT_TRUE(accepted.getBlocking());

    /// The client has disconnected: once its `close_notify` arrives, the probe reports it.
    ASSERT_TRUE(accepted.poll(timeout, Poco::Net::Socket::SELECT_READ));
    EXPECT_EQ(SocketState::Closed, getSocketState(accepted));
}

#endif
