#include <gtest/gtest.h>

#include <Client/PortsProbe.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>

using namespace DB;

namespace
{

/// The probes run against loopback listeners, so generous timeouts never actually elapse
/// on the success paths; they only bound the failure paths.
const Poco::Timespan probe_timeout(2, 0);
const Poco::Timespan preference_window(0, 100000);

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
    return probePlainAndSecurePorts(host, "", plain_port, secure_port, probe_timeout, preference_window);
}

}

/// TLS is preferred: a server listening on both ports is connected over the secure port.
TEST(PortsProbe, PreferSecureWhenBothListen)
{
    auto plain = listenOnLoopback();
    auto secure = listenOnLoopback();
    auto result = probe(plain.address().port(), secure.address().port());
    EXPECT_EQ(result.choice, PortsProbeResult::Choice::PreferSecure);
    /// The address that answered is reported, so that the connection does not start over from the
    /// first resolved address of the host.
    ASSERT_TRUE(result.address.has_value());
    EXPECT_EQ(result.address->toString(), secure.address().toString());
}

/// A plain-only server (the most common setup) is chosen when the secure port refuses,
/// without waiting for the preference window to elapse.
TEST(PortsProbe, PlainOnlyWhenSecureRefused)
{
    auto plain = listenOnLoopback();
    auto result = probe(plain.address().port(), closedPort());
    EXPECT_EQ(result.choice, PortsProbeResult::Choice::PlainOnly);
    ASSERT_TRUE(result.address.has_value());
    EXPECT_EQ(result.address->toString(), plain.address().toString());
}

/// When only the secure port answers (e.g. the plain port is closed or firewalled), TLS is chosen.
TEST(PortsProbe, PreferSecureWhenPlainRefused)
{
    auto secure = listenOnLoopback();
    auto result = probe(closedPort(), secure.address().port());
    EXPECT_EQ(result.choice, PortsProbeResult::Choice::PreferSecure);
    ASSERT_TRUE(result.address.has_value());
    EXPECT_EQ(result.address->toString(), secure.address().toString());
}

/// A host can resolve to several addresses (`localhost` usually resolves to both `127.0.0.1` and `::1`),
/// and only some of them answer. The address that did is the one to connect to.
TEST(PortsProbe, ReportsTheAddressThatAnswered)
{
    auto plain = listenOnLoopback();
    auto result = probe(plain.address().port(), closedPort(), "localhost");
    ASSERT_EQ(result.choice, PortsProbeResult::Choice::PlainOnly);
    ASSERT_TRUE(result.address.has_value());
    EXPECT_EQ(result.address->toString(), plain.address().toString());
}

/// When nothing answers, the failure of every probed address is reported.
TEST(PortsProbe, NeitherWhenBothRefused)
{
    auto result = probe(closedPort(), closedPort());
    EXPECT_EQ(result.choice, PortsProbeResult::Choice::Neither);
    EXPECT_FALSE(result.timed_out);
    EXPECT_TRUE(result.failure_reason.contains("127.0.0.1"));
}
