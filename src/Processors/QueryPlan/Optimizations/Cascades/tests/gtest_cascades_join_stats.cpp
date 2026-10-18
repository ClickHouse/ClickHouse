#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Core/Joins.h>
#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>

using namespace DB;

TEST(CascadesJoinStats, JoinKeyNdvMinRespectsPreservedSide)
{
    const auto update_join_key_ndvs = [](JoinKind kind, JoinStrictness strictness, UInt64 left_ndv, UInt64 right_ndv)
    {
        ColumnStats left_stats{.num_distinct_values = left_ndv};
        ColumnStats right_stats{.num_distinct_values = right_ndv};
        QueryPlanOptimizations::updateJoinKeyDistinctCounts(left_stats, right_stats, kind, strictness);
        return std::pair{left_stats.num_distinct_values, right_stats.num_distinct_values};
    };

    EXPECT_EQ(update_join_key_ndvs(JoinKind::Inner, JoinStrictness::All, 100, 40), (std::pair<UInt64, UInt64>{40, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Left, JoinStrictness::All, 100, 40), (std::pair<UInt64, UInt64>{100, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Right, JoinStrictness::All, 40, 100), (std::pair<UInt64, UInt64>{40, 100}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Full, JoinStrictness::All, 100, 40), (std::pair<UInt64, UInt64>{100, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Left, JoinStrictness::Anti, 100, 40), (std::pair<UInt64, UInt64>{100, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Left, JoinStrictness::Semi, 100, 40), (std::pair<UInt64, UInt64>{40, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Right, JoinStrictness::Semi, 40, 100), (std::pair<UInt64, UInt64>{40, 40}));

    /// A zero NDV is unknown: the known side bounds it when the join filters its side, and it never
    /// narrows the known side.
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Inner, JoinStrictness::All, 0, 40), (std::pair<UInt64, UInt64>{40, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Inner, JoinStrictness::All, 100, 0), (std::pair<UInt64, UInt64>{100, 100}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Left, JoinStrictness::All, 0, 40), (std::pair<UInt64, UInt64>{0, 40}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Left, JoinStrictness::All, 100, 0), (std::pair<UInt64, UInt64>{100, 100}));
    EXPECT_EQ(update_join_key_ndvs(JoinKind::Full, JoinStrictness::All, 0, 40), (std::pair<UInt64, UInt64>{0, 40}));
}

TEST(CascadesJoinStats, JoinKeyNdvOverlapWithinDomain)
{
    /// Two sides filtered out of one domain of 1000 values share values in proportion: 800 and 500
    /// of 1000 overlap in 400, not in 500.
    ColumnStats left_stats{.num_distinct_values = 800, .domain_distinct_values = 1000};
    ColumnStats right_stats{.num_distinct_values = 500, .domain_distinct_values = 1000};
    QueryPlanOptimizations::updateJoinKeyDistinctCounts(left_stats, right_stats, JoinKind::Inner, JoinStrictness::All);
    EXPECT_EQ(left_stats.num_distinct_values, 400);
    EXPECT_EQ(right_stats.num_distinct_values, 400);

    /// Without domains the smaller side counts as contained in the larger one.
    ColumnStats left_no_domain{.num_distinct_values = 800};
    ColumnStats right_no_domain{.num_distinct_values = 500};
    QueryPlanOptimizations::updateJoinKeyDistinctCounts(left_no_domain, right_no_domain, JoinKind::Inner, JoinStrictness::All);
    EXPECT_EQ(left_no_domain.num_distinct_values, 500);

    /// A side without a domain takes the other side's.
    ColumnStats left_known{.num_distinct_values = 800, .domain_distinct_values = 1000};
    ColumnStats right_unknown{.num_distinct_values = 500};
    QueryPlanOptimizations::updateJoinKeyDistinctCounts(left_known, right_unknown, JoinKind::Left, JoinStrictness::All);
    EXPECT_EQ(left_known.num_distinct_values, 800);
    EXPECT_EQ(right_unknown.num_distinct_values, 400);
    EXPECT_EQ(right_unknown.domain_distinct_values, 1000);
}

TEST(CascadesJoinStats, ToUInt64Saturating)
{
    using QueryPlanOptimizations::toUInt64Saturating;
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    constexpr Float64 two_to_64 = 18446744073709551616.0;

    EXPECT_EQ(toUInt64Saturating(0.0), 0u);
    EXPECT_EQ(toUInt64Saturating(-5.0), 0u);
    EXPECT_EQ(toUInt64Saturating(std::numeric_limits<Float64>::quiet_NaN()), 0u);
    EXPECT_EQ(toUInt64Saturating(12345.9), 12345u);
    EXPECT_EQ(toUInt64Saturating(std::nextafter(two_to_64, 0.0)), max - 2047);
    EXPECT_EQ(toUInt64Saturating(two_to_64), max);
    EXPECT_EQ(toUInt64Saturating(1e22), max);
    EXPECT_EQ(toUInt64Saturating(std::numeric_limits<Float64>::infinity()), max);
}
