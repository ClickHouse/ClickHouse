#include "config.h"

#if USE_NURAFT

#include <gtest/gtest.h>

#include <Coordination/KeeperConnectionStats.h>

using namespace DB;

TEST(KeeperConnectionStats, MinLatency)
{
    KeeperConnectionStats stats;
    ASSERT_EQ(stats.getMinLatency(), 0);
    ASSERT_EQ(stats.getMaxLatency(), 0);
    ASSERT_EQ(stats.getAvgLatency(), 0);

    stats.updateLatency(50);
    stats.updateLatency(25);
    stats.updateLatency(120);
    EXPECT_EQ(stats.getMinLatency(), 25);
    EXPECT_EQ(stats.getMaxLatency(), 120);
    EXPECT_EQ(stats.getAvgLatency(), 65);
    EXPECT_EQ(stats.getLastLatency(), 120);

    /// A real 0 ms sample is reported as the minimum.
    stats.updateLatency(0);
    stats.updateLatency(40);
    EXPECT_EQ(stats.getMinLatency(), 0);
}

TEST(KeeperConnectionStats, MinLatencyAfterReset)
{
    KeeperConnectionStats stats;
    stats.updateLatency(10);
    stats.reset();
    EXPECT_EQ(stats.getMinLatency(), 0);
    EXPECT_EQ(stats.getMaxLatency(), 0);

    stats.updateLatency(30);
    stats.updateLatency(70);
    EXPECT_EQ(stats.getMinLatency(), 30);
    EXPECT_EQ(stats.getMaxLatency(), 70);
}

#endif
