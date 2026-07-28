#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Client/Connection.h>
#include <Client/PacketReceiver.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <chrono>
#include <cerrno>
#include <poll.h>
#include <unistd.h>

using namespace DB;

/// Drives PacketReceiver::checkTimeout() directly against a real connected socket, without a
/// ClickHouse server: PacketReceiver only needs the socket's file descriptor, so the test
/// attaches an already-connected socket to an unconnected Connection.
struct PacketReceiverTestAccess
{
    Connection connection;
    std::unique_ptr<PacketReceiver> receiver;

    explicit PacketReceiverTestAccess(const Poco::Net::StreamSocket & socket)
        : connection(
              "127.0.0.1",
              /*port_=*/0,
              /*default_database_=*/"",
              /*user_=*/"default",
              /*password_=*/"",
              /*proto_send_chunked_=*/"notchunked",
              /*proto_recv_chunked_=*/"notchunked",
              SSHKey(),
              /*jwt_=*/"",
              /*quota_key_=*/"",
              /*cluster_=*/"",
              /*cluster_secret_=*/"",
              /*client_name_=*/"gtest",
              Protocol::Compression::Disable,
              Protocol::Secure::Disable,
              /*tls_sni_override_=*/"",
              /*bind_host_=*/"")
    {
        connection.socket = std::make_unique<Poco::Net::StreamSocket>(socket);
        receiver = std::make_unique<PacketReceiver>(&connection);
    }

    /// All const: these mutate the owned receiver through the pointer, not this struct.
    bool checkTimeout() const { return receiver->checkTimeout(); }
    bool isTimeoutExpired() const { return receiver->isTimeoutExpired(); }
    bool isPacketReady() const { return receiver->isPacketReady(); }
    void setTimeout(const Poco::Timespan & t) const { receiver->setTimeout(t); }

    /// The receive timer's descriptor, so a test can wait for it to actually fire instead of
    /// assuming a fixed sleep was long enough.
    int timerFd() const { return receiver->timeout_descriptor.getDescriptor(); }
};

namespace
{

/// Wait until the receive timer's descriptor is readable, so the following epoll wake is
/// guaranteed to report it. A fixed sleep can return early (EINTR, or an oversubscribed host),
/// and in a test where the socket is deliberately readable that would leave a socket-only wake,
/// which satisfies the same assertions for the wrong reason.
[[nodiscard]] bool waitTimerReady(int timer_fd, Poco::Timespan limit)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(limit.totalMicroseconds());
    while (true)
    {
        pollfd p{.fd = timer_fd, .events = POLLIN, .revents = 0};
        const int rc = ::poll(&p, 1, 50);
        if (rc == 1 && (p.revents & POLLIN))
            return true;
        if (rc == -1 && errno != EINTR)
            return false;
        if (std::chrono::steady_clock::now() >= deadline)
            return false;
    }
}

/// A connected TCP socket pair whose client end can be made readable on demand.
/// The kernel listen backlog completes the handshake, so no server thread is needed.
struct ConnectedPair
{
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0), 1};
    Poco::Net::StreamSocket client;
    Poco::Net::StreamSocket server;

    ConnectedPair()
    {
        client.connect(listener.address());
        server = listener.acceptConnection();
    }

    /// Make the client end readable, and wait until the kernel reports it as such,
    /// so that a subsequent epoll wake deterministically sees the socket ready.
    void makeClientReadable()
    {
        const char byte = 'x';
        server.sendBytes(&byte, 1);
        ASSERT_TRUE(client.poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));
    }

    void drainClient()
    {
        char buf[64];
        while (client.poll(Poco::Timespan(0, 0), Poco::Net::Socket::SELECT_READ))
        {
            if (client.receiveBytes(buf, sizeof(buf)) <= 0)
                break;
        }
    }
};

}

/// PacketReceiver::checkTimeout() observes the timer fd and the socket fd of one epoll wake.
/// Timer readiness alone means nothing: it is a receive timeout only if the socket was NOT
/// ready in the SAME wake. Historically the observation was stored in the `is_timeout_expired`
/// MEMBER before the conjunction was evaluated and nothing ever cleared it, so a single wake
/// carrying both a data packet and the timer expiry delivered its packet correctly and then
/// left the receiver permanently "timed out": isPacketReady() was stuck false and
/// HedgedConnections::resumePacketReceiver() threw a spurious SOCKET_TIMEOUT on every
/// subsequent packet.
TEST(PacketReceiverTimeoutLatch, SimultaneousReadinessLeavesNoTimeoutResidue)
{
    ConnectedPair pair;
    PacketReceiverTestAccess receiver(pair.client);

    /// Arm the timer and let it expire, then make the socket readable as well, so that the
    /// next epoll wake reports BOTH descriptors.
    receiver.setTimeout(Poco::Timespan(0, 10'000));
    pair.makeClientReadable();
    ASSERT_TRUE(waitTimerReady(receiver.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";

    /// Both ready => a packet is available => this is not a timeout.
    ASSERT_TRUE(receiver.checkTimeout());
    EXPECT_FALSE(receiver.isTimeoutExpired())
        << "a wake in which the socket was ready must not declare a receive timeout";
    EXPECT_TRUE(receiver.isPacketReady())
        << "the receiver must still be able to deliver packets after a simultaneous-readiness wake";

    /// And the receiver must remain usable: with the timer re-armed and unexpired, a further
    /// wake on a readable socket must again report "no timeout".
    receiver.setTimeout(Poco::Timespan(10, 0));
    ASSERT_TRUE(receiver.checkTimeout());
    EXPECT_FALSE(receiver.isTimeoutExpired());
    EXPECT_TRUE(receiver.isPacketReady());
}

/// The regression guard for the opposite direction: a genuine receive timeout (timer ready,
/// socket NOT ready) must still be declared, and must still be visible to
/// HedgedConnections::resumePacketReceiver() through isTimeoutExpired() after checkTimeout()
/// returned, i.e. the verdict has to persist across calls even though the raw observation
/// must not.
TEST(PacketReceiverTimeoutLatch, GenuineTimeoutIsStillDeclaredAndPersists)
{
    ConnectedPair pair;
    PacketReceiverTestAccess receiver(pair.client);

    pair.drainClient();
    receiver.setTimeout(Poco::Timespan(0, 10'000));
    ASSERT_TRUE(waitTimerReady(receiver.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";

    /// Timer ready, socket not ready => genuine receive timeout.
    ASSERT_FALSE(receiver.checkTimeout());
    EXPECT_TRUE(receiver.isTimeoutExpired());
    EXPECT_FALSE(receiver.isPacketReady());

    /// The verdict persists for the caller, which reads it only after checkTimeout() returned.
    EXPECT_TRUE(receiver.isTimeoutExpired());
}

/// HedgedConnections::resumePacketReceiver() re-arms the receive timeout after every received
/// packet (setTimeout()); that per-packet re-arm is the invariant of "receive_timeout is a
/// per-packet idle timeout, not a total-query timeout". A declared timeout must therefore be
/// cleared by the re-arm, or the re-arm would be unreachable for the rest of the connection.
TEST(PacketReceiverTimeoutLatch, SetTimeoutClearsADeclaredTimeout)
{
    ConnectedPair pair;
    PacketReceiverTestAccess receiver(pair.client);

    pair.drainClient();
    receiver.setTimeout(Poco::Timespan(0, 10'000));
    ASSERT_TRUE(waitTimerReady(receiver.timerFd(), Poco::Timespan(5, 0)))
        << "the receive timer never became ready, so this wake would not exercise the latch";
    ASSERT_FALSE(receiver.checkTimeout());
    ASSERT_TRUE(receiver.isTimeoutExpired());

    /// Re-arming starts a new receive interval.
    receiver.setTimeout(Poco::Timespan(10, 0));
    EXPECT_FALSE(receiver.isTimeoutExpired())
        << "re-arming the receive timeout must clear a previously declared timeout";
    EXPECT_TRUE(receiver.isPacketReady())
        << "after the per-packet re-arm the receiver must be usable again";
}

#endif
