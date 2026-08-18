#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Core/Joins.h>
#include <algorithm>

using namespace DB;

/// `clampJoinRowCount` adjusts an inner-join-style estimate to the semantics of the join kind and
/// strictness. `base` is the multiplicative inner estimate; `left`/`right` are the input counts.
TEST(CascadesJoinStats, ClampByKindAndStrictness)
{
    const Float64 left = 100;
    const Float64 right = 10;
    const Float64 base = 1000;  /// unclamped inner product estimate

    /// Inner/cross keep the multiplicative estimate; paste is position-wise.
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Inner, JoinStrictness::All, base, left, right), base);
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Cross, JoinStrictness::Unspecified, base, left, right), base);
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Paste, JoinStrictness::All, base, left, right), std::min(left, right));

    /// Outer joins keep every preserved-side row (a small inner estimate must not drop below it).
    EXPECT_GE(clampJoinRowCount(JoinKind::Left, JoinStrictness::All, 5.0, left, right), left);
    EXPECT_GE(clampJoinRowCount(JoinKind::Right, JoinStrictness::All, 2.0, left, right), right);
    EXPECT_GE(clampJoinRowCount(JoinKind::Full, JoinStrictness::All, 1.0, left, right), std::max(left, right));

    /// Full outer keeps unmatched rows from both sides regardless of strictness: ANY/RightAny must not
    /// clamp it below the larger side (trace: left=10, right=100, base=1000).
    EXPECT_GE(clampJoinRowCount(JoinKind::Full, JoinStrictness::RightAny, 1000, 10, 100), 100.0);
    EXPECT_GE(clampJoinRowCount(JoinKind::Full, JoinStrictness::Any, 1000, 10, 100), 100.0);

    /// Semi/anti filter the preserved side, so the output cannot exceed it.
    EXPECT_LE(clampJoinRowCount(JoinKind::Left, JoinStrictness::Semi, base, left, right), left);
    EXPECT_LE(clampJoinRowCount(JoinKind::Left, JoinStrictness::Anti, base, left, right), left);
    EXPECT_LE(clampJoinRowCount(JoinKind::Right, JoinStrictness::Semi, base, left, right), right);

    /// Inner ANY/RightAny dedup to matching keys (measured: 1000 rows of one key ANY-join 1 row -> 1),
    /// so both are bounded by min(left, right).
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Inner, JoinStrictness::Any, base, left, right), std::min(left, right));
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Inner, JoinStrictness::RightAny, base, left, right), std::min(left, right));
    /// Inner ASOF keeps one nearest match per left row, so it is bounded by the left side.
    EXPECT_DOUBLE_EQ(clampJoinRowCount(JoinKind::Inner, JoinStrictness::Asof, base, left, right), left);
}

/// `clampJoinMaxRowCount` must be a valid upper bound: never below a possible join output.
TEST(CascadesJoinStats, MaxRowCountUpperBound)
{
    /// Full join over disjoint single-row sides emits both rows (product would say 1).
    EXPECT_GE(clampJoinMaxRowCount(JoinKind::Full, JoinStrictness::All, /*product=*/1, /*left=*/1, /*right=*/1), 2.0);
    /// Anti join with an empty right side keeps the whole preserved side (product = 0).
    EXPECT_GE(clampJoinMaxRowCount(JoinKind::Left, JoinStrictness::Anti, /*product=*/0, /*left=*/100, /*right=*/0), 100.0);
    /// Semi is bounded by the preserved side.
    EXPECT_LE(clampJoinMaxRowCount(JoinKind::Left, JoinStrictness::Semi, /*product=*/1000, /*left=*/100, /*right=*/10), 100.0);
    /// Inner keeps the product as the upper bound.
    EXPECT_DOUBLE_EQ(clampJoinMaxRowCount(JoinKind::Inner, JoinStrictness::All, /*product=*/1000, /*left=*/100, /*right=*/10), 1000.0);
    /// Left outer never drops below the preserved side even when the right side is empty.
    EXPECT_GE(clampJoinMaxRowCount(JoinKind::Left, JoinStrictness::All, /*product=*/0, /*left=*/100, /*right=*/0), 100.0);
}
