#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/ReplicationEligibility.h>

using namespace DB;

namespace
{

const ReplicationBudget bytes_only{.max_rows = std::nullopt, .max_bytes = 1000};
const ReplicationBudget rows_and_bytes{.max_rows = 100, .max_bytes = 1000};

}

TEST(ReplicationEligibility, TrustedEstimateAgainstTheByteBudget)
{
    /// 50 rows of 10 bytes fit a budget of 1000 bytes, 200 rows do not.
    auto fits = decideReplication({.estimated_rows = 50, .bytes_per_row = 10}, bytes_only);
    EXPECT_TRUE(fits.allowed);
    EXPECT_EQ(fits.counted_rows, 50);
    EXPECT_EQ(fits.modeled_bytes, 500);

    auto too_big = decideReplication({.estimated_rows = 200, .bytes_per_row = 10}, bytes_only);
    EXPECT_FALSE(too_big.allowed);
    EXPECT_EQ(too_big.modeled_bytes, 2000);

    /// A bound far above the estimate does not matter when the estimate is trusted.
    EXPECT_TRUE(decideReplication({.estimated_rows = 50, .max_rows = 1000000, .bytes_per_row = 10}, bytes_only).allowed);
}

TEST(ReplicationEligibility, GuessesNeedABound)
{
    /// An estimate that took defaults is a guess: the bound decides the bytes.
    EXPECT_TRUE(decideReplication({.estimated_rows = 50, .estimate_from_defaults = true, .max_rows = 90, .bytes_per_row = 10}, bytes_only).allowed);
    EXPECT_FALSE(decideReplication({.estimated_rows = 50, .estimate_from_defaults = true, .max_rows = 500, .bytes_per_row = 10}, bytes_only).allowed);
    EXPECT_FALSE(decideReplication({.estimated_rows = 50, .estimate_from_defaults = true, .bytes_per_row = 10}, bytes_only).allowed);

    /// No estimate at all: the bound alone decides, nothing without one.
    EXPECT_TRUE(decideReplication({.max_rows = 90, .bytes_per_row = 10}, bytes_only).allowed);
    EXPECT_FALSE(decideReplication({.max_rows = 500, .bytes_per_row = 10}, bytes_only).allowed);
    EXPECT_FALSE(decideReplication({.bytes_per_row = 10}, bytes_only).allowed);
}

TEST(ReplicationEligibility, RowLimit)
{
    /// The row limit applies to the estimate as the planners always did, and to the bound when
    /// there is no estimate.
    EXPECT_TRUE(decideReplication({.estimated_rows = 100, .bytes_per_row = 1}, rows_and_bytes).allowed);
    EXPECT_FALSE(decideReplication({.estimated_rows = 101, .bytes_per_row = 1}, rows_and_bytes).allowed);
    EXPECT_TRUE(decideReplication({.max_rows = 100, .bytes_per_row = 1}, rows_and_bytes).allowed);
    EXPECT_FALSE(decideReplication({.max_rows = 101, .bytes_per_row = 1}, rows_and_bytes).allowed);
    EXPECT_FALSE(decideReplication({.bytes_per_row = 1}, rows_and_bytes).allowed);

    /// A guess within the row limit still has to prove its bytes with the bound.
    EXPECT_FALSE(decideReplication({.estimated_rows = 50, .estimate_from_defaults = true, .max_rows = 5000, .bytes_per_row = 1}, rows_and_bytes).allowed);
    /// Both limits pass on the estimate and the bound, respectively.
    EXPECT_TRUE(decideReplication({.estimated_rows = 50, .estimate_from_defaults = true, .max_rows = 900, .bytes_per_row = 1}, rows_and_bytes).allowed);
}

TEST(ReplicationEligibility, ScanBytes)
{
    /// Ten output rows, but every node would read 5000 bytes to find them.
    EXPECT_FALSE(decideReplication({.estimated_rows = 10, .bytes_per_row = 10, .scan_bytes = 5000}, bytes_only).allowed);
    EXPECT_TRUE(decideReplication({.estimated_rows = 10, .bytes_per_row = 10, .scan_bytes = 500}, bytes_only).allowed);
}

TEST(ReplicationEligibility, NoLimits)
{
    const ReplicationBudget none{.max_rows = std::nullopt, .max_bytes = 0};
    EXPECT_TRUE(decideReplication({.bytes_per_row = 10}, none).allowed);
    EXPECT_TRUE(decideReplication({.estimated_rows = 1000000, .bytes_per_row = 1000}, none).allowed);

    /// A row limit without a byte budget behaves as the row limit alone.
    const ReplicationBudget rows_only{.max_rows = 100, .max_bytes = 0};
    EXPECT_TRUE(decideReplication({.estimated_rows = 100, .bytes_per_row = 1000000}, rows_only).allowed);
    EXPECT_FALSE(decideReplication({.estimated_rows = 101, .bytes_per_row = 1}, rows_only).allowed);
}
