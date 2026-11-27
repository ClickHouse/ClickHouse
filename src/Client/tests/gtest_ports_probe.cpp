#include <gtest/gtest.h>

#include <Client/PortsProbe.h>

#include <Poco/Exception.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <optional>

using namespace DB;

namespace
{

/// The probes run against loopback listeners, so generous timeouts never actually elapse
/// on the success paths; they only bound the failure paths.
const Poco::Timespan probe_timeout(2, 0);
const Poco::Timespan attempt_delay(0, 250000);

Poco::Net::ServerSocket listenOnLoopback()
{
    return Poco::Net::ServerSocket(Poco::Net::SocketAddress("127.0.0.1", 0));
}

/// Binds and immediately closes a listener, returning a port that (very likely) refuses connections.
UInt16 closedPort()
{
    auto socket = listenOnLoopback();
    UInt16 port = socket.address().port();
    socket.close();
    return port;
}

PortsProbeResult probe(UInt16 plain_port, UInt16 secure_port, const String & host = "127.0.0.1")
{
    return probePlainAndSecurePorts(host, "", plain_port, secure_port, probe_timeout, attempt_delay);
}

}

/// A server listening on both ports is connected to over either of them - both work, so the probe
/// does not wait to find out whether the other one answers too.
TEST(PortsProbe, EitherPortWhenBothListen)
{
    auto plain = listenOnLoopback();
    auto secure = listenOnLoopback();
    auto result = probe(plain.address().port(), secure.address().port());
    ASSERT_TRUE(result.endpoint.has_value());
    /// The address that answered is reported, so that the connection does not start over from the
    /// first resolved address of the host.
    const auto & answered = result.endpoint->secure ? secure : plain;
    EXPECT_EQ(result.endpoint->address.toString(), answered.address().toString());
}

/// A plain-only server (the most common setup) is chosen when the secure port refuses.
TEST(PortsProbe, PlainOnlyWhenSecureRefused)
{
    auto plain = listenOnLoopback();
    auto result = probe(plain.address().port(), closedPort());
    ASSERT_TRUE(result.endpoint.has_value());
    EXPECT_FALSE(result.endpoint->secure);
    EXPECT_EQ(result.endpoint->address.toString(), plain.address().toString());
}

/// When only the secure port answers (e.g. the plain port is closed or firewalled), TLS is chosen.
TEST(PortsProbe, SecureWhenPlainRefused)
{
    auto secure = listenOnLoopback();
    auto result = probe(closedPort(), secure.address().port());
    ASSERT_TRUE(result.endpoint.has_value());
    EXPECT_TRUE(result.endpoint->secure);
    EXPECT_EQ(result.endpoint->address.toString(), secure.address().toString());
}

/// A host can resolve to several addresses (`localhost` usually resolves to both `127.0.0.1` and `::1`),
/// and only some of them answer. The address that did is the one to connect to.
TEST(PortsProbe, ReportsTheAddressThatAnswered)
{
    auto plain = listenOnLoopback();
    auto result = probe(plain.address().port(), closedPort(), "localhost");
    ASSERT_TRUE(result.endpoint.has_value());
    EXPECT_FALSE(result.endpoint->secure);
    EXPECT_EQ(result.endpoint->address.toString(), plain.address().toString());
}

/// The connection the probe established is handed over to the caller, so that the real connection reuses
/// it instead of opening a second one and leaving a session that sends nothing on the server.
TEST(PortsProbe, HandsOverTheEstablishedConnection)
{
    auto plain = listenOnLoopback();
    auto result = probe(plain.address().port(), closedPort());
    ASSERT_TRUE(result.endpoint.has_value());

    auto accepted = plain.acceptConnection();
    /// Both ends of the same connection: what the probe connected with is what the server accepted.
    EXPECT_EQ(result.endpoint->socket.peerAddress().toString(), accepted.address().toString());
    EXPECT_EQ(result.endpoint->socket.address().toString(), accepted.peerAddress().toString());

    /// And it is a working connection, not a closed one.
    result.endpoint->socket.setBlocking(true);
    const char message[] = "probe";
    ASSERT_EQ(result.endpoint->socket.sendBytes(message, sizeof(message)), static_cast<int>(sizeof(message)));
    char received[sizeof(message)] = {};
    ASSERT_EQ(accepted.receiveBytes(received, sizeof(received)), static_cast<int>(sizeof(message)));
    EXPECT_STREQ(received, message);
}

/// The addresses of a port are attempted one at a time, so a host that resolves to several reachable
/// backends is connected to on one of them only: every other accepted connection would be a session that
/// sends nothing, which the server logs and counts against `max_connections`.
TEST(PortsProbe, AttemptsOneAddressAtATime)
{
    /// `localhost` resolves to both `127.0.0.1` and `::1` on the machines this runs on, so listening on
    /// the same port in both families gives a host with two reachable addresses. The port has to be free
    /// in both, which an ephemeral port of one family does not guarantee, hence the retries.
    std::optional<Poco::Net::ServerSocket> v4;
    std::optional<Poco::Net::ServerSocket> v6;
    for (size_t attempt = 0; attempt < 16 && !v6; ++attempt)
    {
        v4.emplace(Poco::Net::SocketAddress("127.0.0.1", 0));
        try
        {
            v6.emplace(Poco::Net::SocketAddress("::1", v4->address().port()));
        }
        catch (const Poco::Exception &)
        {
            v4.reset();
        }
    }
    if (!v6)
        GTEST_SKIP() << "Cannot listen on the same port in both address families";

    const UInt16 port = v4->address().port();
    auto result = probePlainAndSecurePorts("localhost", "", port, closedPort(), probe_timeout, attempt_delay);
    ASSERT_TRUE(result.endpoint.has_value());

    /// Exactly one of the two listeners has a connection queued: `poll` on a server socket reports
    /// readability precisely when there is one to accept. Which of them it is depends on the order the
    /// resolver returns, so it is read from the result rather than assumed.
    const bool answered_over_v4 = result.endpoint->address.family() == Poco::Net::AddressFamily::IPv4;
    auto & chosen = answered_over_v4 ? *v4 : *v6;
    auto & other = answered_over_v4 ? *v6 : *v4;
    EXPECT_TRUE(chosen.poll(Poco::Timespan(0, 0), Poco::Net::Socket::SELECT_READ));
    EXPECT_FALSE(other.poll(Poco::Timespan(0, 0), Poco::Net::Socket::SELECT_READ));
}

/// When nothing answers, the failure of every probed address is reported.
TEST(PortsProbe, NeitherWhenBothRefused)
{
    auto result = probe(closedPort(), closedPort());
    EXPECT_FALSE(result.endpoint.has_value());
    EXPECT_FALSE(result.timed_out);
    EXPECT_TRUE(result.failure_reason.contains("127.0.0.1"));
}
