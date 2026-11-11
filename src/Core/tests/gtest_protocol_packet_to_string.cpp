#include <gtest/gtest.h>

#include <Core/Protocol.h>

#include <limits>

using namespace DB;

/// Out-of-range packet types come off the wire (stream desync, fuzzing). Stringifying
/// them must not load an out-of-range value into the unscoped enum (undefined behavior,
/// flagged by UBSan -fsanitize=enum). Out-of-range values render as their numeric value so
/// the "unexpected packet" error messages stay debuggeable.

TEST(ProtocolPacketToString, ServerValid)
{
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::Hello), "Hello");
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::Exception), "Exception");
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::SSHChallenge), "SSHChallenge");
}

TEST(ProtocolPacketToString, ServerOutOfRange)
{
    /// 138 is the exact value that triggered the UBSan report in arm_ubsan stress.
    EXPECT_EQ(Protocol::Server::toString(138), "138");
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::MAX + 1), std::to_string(Protocol::Server::MAX + 1));
    EXPECT_EQ(Protocol::Server::toString(255), "255");
    EXPECT_EQ(Protocol::Server::toString(std::numeric_limits<UInt64>::max()),
              std::to_string(std::numeric_limits<UInt64>::max()));
}

TEST(ProtocolPacketToString, ClientValid)
{
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::Hello), "Hello");
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::Query), "Query");
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::QueryPlan), "QueryPlan");
}

TEST(ProtocolPacketToString, ClientOutOfRange)
{
    EXPECT_EQ(Protocol::Client::toString(138), "138");
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::MAX + 1), std::to_string(Protocol::Client::MAX + 1));
    EXPECT_EQ(Protocol::Client::toString(255), "255");
    EXPECT_EQ(Protocol::Client::toString(std::numeric_limits<UInt64>::max()),
              std::to_string(std::numeric_limits<UInt64>::max()));
}
