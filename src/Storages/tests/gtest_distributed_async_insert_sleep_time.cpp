#include <gtest/gtest.h>

#include <Storages/Distributed/DistributedAsyncInsertDirectoryQueue.h>

using namespace DB;
using namespace std::chrono;

namespace
{
milliseconds calc(int64_t default_ms, int64_t max_ms, size_t error_count)
{
    return DistributedAsyncInsertDirectoryQueue::calculateSleepTime(
        milliseconds(default_ms), milliseconds(max_ms), error_count);
}
}

TEST(DistributedAsyncInsertSleepTime, GrowsExponentiallyWithErrors)
{
    /// error_count == 0 -> base delay; each error roughly doubles the delay until it saturates.
    EXPECT_EQ(calc(100, 30000, 0), milliseconds(100));
    EXPECT_EQ(calc(100, 30000, 1), milliseconds(200));
    EXPECT_EQ(calc(100, 30000, 2), milliseconds(400));
    EXPECT_EQ(calc(100, 30000, 3), milliseconds(800));
}

TEST(DistributedAsyncInsertSleepTime, SaturatesAtMax)
{
    /// 100 * 2^10 = 102400 > 30000, so it must clamp to max.
    EXPECT_EQ(calc(100, 30000, 10), milliseconds(30000));
    EXPECT_EQ(calc(100, 30000, 50), milliseconds(30000));
}

TEST(DistributedAsyncInsertSleepTime, DecreasesWhenErrorCountDecays)
{
    /// The delay must come back down as error_count decays, not stay pinned at the maximum.
    EXPECT_EQ(calc(100, 30000, 20), milliseconds(30000));
    EXPECT_EQ(calc(100, 30000, 8), milliseconds(25600));
    EXPECT_EQ(calc(100, 30000, 3), milliseconds(800));
    EXPECT_EQ(calc(100, 30000, 0), milliseconds(100));
}

TEST(DistributedAsyncInsertSleepTime, DoesNotOverflowForLargeErrorCount)
{
    /// Regression: with default 100ms, error_count == 62 makes 100 * 2^62 wrap to 0; must saturate to max.
    EXPECT_EQ(calc(100, 30000, 62), milliseconds(30000));
    EXPECT_EQ(calc(100, 30000, 63), milliseconds(30000));
    EXPECT_EQ(calc(100, 30000, 1000), milliseconds(30000));
}

TEST(DistributedAsyncInsertSleepTime, HandlesZeroBaseDelay)
{
    /// A zero base delay disables backoff and must not divide by zero.
    EXPECT_EQ(calc(0, 30000, 0), milliseconds(0));
    EXPECT_EQ(calc(0, 30000, 100), milliseconds(0));
}
