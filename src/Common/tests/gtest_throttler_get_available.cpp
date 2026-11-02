#include <gtest/gtest.h>

#include <Common/Throttler.h>

#include <limits>

using namespace DB;

/// `s3_max_get_rps` = 2^64 - 1 makes a throttler whose burst holds more tokens than `Int64` can represent.
/// `getAvailable` (read by asynchronous metrics) used to convert them to `Int64` with undefined behavior.
TEST(Throttler, GetAvailableWithHugeBurst)
{
    constexpr auto max_value = std::numeric_limits<UInt64>::max();
    Throttler throttler(max_value, max_value, /* parent_= */ nullptr);
    EXPECT_EQ(throttler.getAvailable(), std::numeric_limits<Int64>::max());
}

TEST(Throttler, GetAvailable)
{
    Throttler throttler(/* max_speed_= */ 1000, /* max_burst_= */ 100, /* parent_= */ nullptr);
    EXPECT_EQ(throttler.getAvailable(), 100);
}
