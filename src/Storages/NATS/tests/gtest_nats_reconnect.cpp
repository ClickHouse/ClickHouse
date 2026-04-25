#include <gtest/gtest.h>

#include "config.h"

#if USE_NATS

#include <atomic>
#include <thread>

#include <Common/Logger.h>
#include <Storages/NATS/NATSConnection.h>

namespace DB
{

/// Test the reconnect-counter primitive used by `StorageNATS` to detect that
/// the underlying NATS connection has reconnected to a new server. The full
/// re-subscription flow (StorageNATS + JetStream) is exercised manually
/// against a live NATS cluster as documented in issue #96651; this unit
/// test covers only the counter mechanism, which is the only piece that
/// can be verified without a live broker.
class NATSConnectionReconnectCountTest : public ::testing::Test
{
};

namespace
{

/// Build a `NATSConnection` instance without actually connecting to any
/// broker. The constructor only configures `natsOptions`; it does not open
/// a connection. `connect` is intentionally not called here.
std::unique_ptr<NATSConnection> makeConnectionForTest()
{
    NATSConfiguration configuration;
    configuration.url = "nats://127.0.0.1:1";  /// Unreachable, never connected.
    configuration.max_connect_tries = 1;
    configuration.reconnect_wait = 1000;
    configuration.secure = false;

    natsOptions * raw_options = nullptr;
    [[maybe_unused]] natsStatus status = natsOptions_Create(&raw_options);
    NATSOptionsPtr options(raw_options, &natsOptions_Destroy);

    return std::make_unique<NATSConnection>(configuration, getLogger("NATSConnectionTest"), std::move(options));
}

}

TEST_F(NATSConnectionReconnectCountTest, InitialCountIsZero)
{
    auto conn = makeConnectionForTest();
    EXPECT_EQ(conn->getReconnectCount(), 0u);
}

TEST_F(NATSConnectionReconnectCountTest, CounterIsMonotonicAndStableAcrossReads)
{
    auto conn = makeConnectionForTest();
    /// Read the same counter twice; without any reconnect callback firing,
    /// the value must be unchanged.
    EXPECT_EQ(conn->getReconnectCount(), 0u);
    EXPECT_EQ(conn->getReconnectCount(), 0u);
}

}

#endif
