#include <Interpreters/AdaptiveAggregation/AdaptiveFreezeBounds.h>

#include <gtest/gtest.h>

#include <tuple>

using namespace DB;

/// Up to 64 producers every learning table takes the whole freeze threshold, and more producers share 64 thresholds:
/// with 16,384 keys, 65 producers take 16,384 * 64 / 65 = 16,131 keys each and 192 producers 5,461.
TEST(AdaptiveEarlyFreeze, ThresholdIsSharedAbove64Producers)
{
    for (const auto & [threshold, producers, expected] : {
             std::tuple<size_t, size_t, size_t>{16384, 1, 16384},
             {16384, 64, 16384},
             {16384, 65, 16131},
             {16384, 128, 8192},
             {16384, 192, 5461},
             {128, 192, 42},
             {0, 192, 0}})
        EXPECT_EQ(adaptiveEarlyFreezeThreshold(threshold, producers), expected) << threshold << " keys, " << producers << " producers";
}

/// With 192 producers a table freezes at 5,461 keys, the key count of an early freeze, while the freeze pays: its
/// producer expects 16 more rows for each of 2,048 partitions. It also does while the table is on track to the whole
/// threshold: 5,600 keys after 8,000 rows gain 0.7 keys per row, so 15,406 more rows reach 16,384 keys and 15,405 do
/// not. Otherwise the table freezes at the whole threshold.
TEST(AdaptiveFreezeBounds, EarlyFreezeAbove64Producers)
{
    const AdaptiveFreezeBounds pays(16384, 5461, /*expected_rows_=*/42768, /*received_rows_=*/10000, /*partitions_=*/2048);
    EXPECT_FALSE(pays.reached(5460, 9000));
    EXPECT_TRUE(pays.reached(5461, 9000));

    const AdaptiveFreezeBounds on_track(16384, 5461, /*expected_rows_=*/23406, /*received_rows_=*/8000, /*partitions_=*/2048);
    EXPECT_TRUE(on_track.reached(5600, 8000));

    const AdaptiveFreezeBounds neither(16384, 5461, /*expected_rows_=*/23405, /*received_rows_=*/8000, /*partitions_=*/2048);
    EXPECT_FALSE(neither.reached(5600, 8000));
    EXPECT_TRUE(neither.reached(16384, 20000));
}
