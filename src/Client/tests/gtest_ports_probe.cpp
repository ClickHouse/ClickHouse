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

PortsProbeResult probe(UInt16 plain_port, UInt16 secure_port)
{
    return probePlainAndSecurePorts("127.0.0.1", "", plain_port, secure_port, probe_timeout, preference_window);
}

}

/// A server listening on both ports keeps being connected over the plain port, as without the probing.
TEST(PortsProbe, PreferPlainWhenBothListen)
{
    auto plain = listenOnLoopback();
    auto secure = listenOnLoopback();
    EXPECT_EQ(probe(plain.address().port(), secure.address().port()).choice, PortsProbeResult::Choice::PreferPlain);
}

/// A plain-only server (the most common setup) is chosen even though the secure port refuses.
TEST(PortsProbe, PreferPlainWhenOnlyPlainListens)
{
    auto plain = listenOnLoopback();
    EXPECT_EQ(probe(plain.address().port(), closedPort()).choice, PortsProbeResult::Choice::PreferPlain);
}

/// When only the secure port answers (e.g. the plain port is closed or firewalled), TLS is chosen.
TEST(PortsProbe, SecureOnlyWhenPlainRefused)
{
    auto secure = listenOnLoopback();
    EXPECT_EQ(probe(closedPort(), secure.address().port()).choice, PortsProbeResult::Choice::SecureOnly);
}

/// When nothing answers, the failure of every probed address is reported.
TEST(PortsProbe, NeitherWhenBothRefused)
{
    auto result = probe(closedPort(), closedPort());
    EXPECT_EQ(result.choice, PortsProbeResult::Choice::Neither);
    EXPECT_FALSE(result.timed_out);
    EXPECT_TRUE(result.failure_reason.contains("127.0.0.1"));
}
