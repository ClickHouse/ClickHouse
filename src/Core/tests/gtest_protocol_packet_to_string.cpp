#include <gtest/gtest.h>

#include <Core/Protocol.h>

#include <limits>

using namespace DB;

/// Out-of-range packet types come off the wire (stream desync, fuzzing). Stringifying
/// them must not load an out-of-range value into the unscoped enum (undefined behavior,
/// flagged by UBSan -fsanitize=enum). Out-of-range values map to an empty name.

TEST(ProtocolPacketToString, ServerValid)
{
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::Hello), "Hello");
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::Exception), "Exception");
    EXPECT_EQ(Protocol::Server::toString(Protocol::Server::SSHChallenge), "SSHChallenge");
}

TEST(ProtocolPacketToString, ServerOutOfRange)
{
    /// 138 is the exact value that triggered the UBSan report in arm_ubsan stress.
    EXPECT_TRUE(Protocol::Server::toString(138).empty());
    EXPECT_TRUE(Protocol::Server::toString(Protocol::Server::MAX + 1).empty());
    EXPECT_TRUE(Protocol::Server::toString(255).empty());
    EXPECT_TRUE(Protocol::Server::toString(std::numeric_limits<UInt64>::max()).empty());
}

TEST(ProtocolPacketToString, ClientValid)
{
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::Hello), "Hello");
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::Query), "Query");
    EXPECT_EQ(Protocol::Client::toString(Protocol::Client::QueryPlan), "QueryPlan");
}

TEST(ProtocolPacketToString, ClientOutOfRange)
{
    EXPECT_TRUE(Protocol::Client::toString(138).empty());
    EXPECT_TRUE(Protocol::Client::toString(Protocol::Client::MAX + 1).empty());
    EXPECT_TRUE(Protocol::Client::toString(255).empty());
    EXPECT_TRUE(Protocol::Client::toString(std::numeric_limits<UInt64>::max()).empty());
}
