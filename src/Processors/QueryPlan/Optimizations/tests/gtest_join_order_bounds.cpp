#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(JoinOrderBounds, RowsUpperBoundByJoinKind)
{
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    const auto bound = [](std::optional<UInt64> left, std::optional<UInt64> right, JoinKind kind, JoinStrictness strictness = JoinStrictness::All)
    {
        return estimateJoinRowsUpperBound(left, right, kind, strictness);
    };

    EXPECT_EQ(bound(10, 20, JoinKind::Inner), 200);
    EXPECT_EQ(bound(10, 20, JoinKind::Cross), 200);
    /// A preserved row stays even without a match, so an empty other side does not zero the bound.
    EXPECT_EQ(bound(10, 20, JoinKind::Left), 200);
    EXPECT_EQ(bound(10, 0, JoinKind::Left), 10);
    EXPECT_EQ(bound(0, 20, JoinKind::Right), 20);
    EXPECT_EQ(bound(10, 20, JoinKind::Full), 220);
    EXPECT_EQ(bound(10, 0, JoinKind::Full), 10);
    /// Semi and anti joins keep at most the preserved input, whatever the other side.
    EXPECT_EQ(bound(10, 20, JoinKind::Left, JoinStrictness::Semi), 10);
    EXPECT_EQ(bound(10, 20, JoinKind::Left, JoinStrictness::Anti), 10);
    EXPECT_EQ(bound(10, 20, JoinKind::Right, JoinStrictness::Semi), 20);
    EXPECT_EQ(bound(10, std::nullopt, JoinKind::Left, JoinStrictness::Semi), 10);
    /// An unknown input bound makes the result unknown.
    EXPECT_EQ(bound(std::nullopt, 20, JoinKind::Inner), std::nullopt);
    EXPECT_EQ(bound(10, std::nullopt, JoinKind::Left), std::nullopt);
    /// Saturation instead of wrap-around.
    EXPECT_EQ(bound(max, 2, JoinKind::Inner), max);
    EXPECT_EQ(bound(max, 2, JoinKind::Full), max);
}

TEST(JoinOrderBounds, SemiAntiRowsFromKeyContainment)
{
    /// Without the containment the other side's rows per preserved key stand in: 100000 rows over
    /// 10000 keys give one match per preserved row, so nothing is dropped.
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Semi), 100000);
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Anti), 1);
    /// With it, a tenth of the preserved key values has a match whatever the other side's rows.
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Semi, 0.1), 10000);
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Anti, 0.1), 90000);
    EXPECT_EQ(estimateJoinCardinality(100000, 1000, 1.0 / 10000, JoinKind::Right, JoinStrictness::Semi, 1.0), 1000);
    /// A key estimate picks the preserved side's fraction by the join kind.
    JoinKeyEstimate keys{.selectivity = 1.0 / 10000, .left_match_fraction = 0.1, .right_match_fraction = 1.0};
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, keys, JoinKind::Left, JoinStrictness::Anti), 90000);
    EXPECT_EQ(estimateJoinCardinality(100000, 1000, keys, JoinKind::Right, JoinStrictness::Anti), 1);
}

TEST(JoinOrderBounds, KeyContainment)
{
    /// Fewer values on the other side lie within this side's values.
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(10000, 1000), 0.1);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(10000, 1000, 150000), 0.1);
    /// At least as many values on the other side: all matched without a domain, their share of
    /// the domain with one (1363 filtered customers against the 100000 customers with orders, of
    /// 150000).
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(1000, 10000), 1.0);
    EXPECT_NEAR(*QueryPlanOptimizations::keyContainment(1363, 100000, 150000), 0.6667, 0.001);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(1363, 100000, 50000), 1.0);
    EXPECT_EQ(QueryPlanOptimizations::keyContainment(0, 1000), std::nullopt);
    EXPECT_EQ(QueryPlanOptimizations::keyContainment(1000, 0), std::nullopt);
    /// Two sides with equally many values out of a larger domain overlap in proportion, as the
    /// NDV update models it, instead of one side counting as contained in the other.
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(500, 500, 1000), 0.5);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(500, 500), 1.0);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(500, 501, 1000), 0.501);
}

TEST(JoinOrderBounds, DistinctValuesAfterFilter)
{
    /// Four rows per value and a third of the rows kept: a value survives unless all four of its
    /// rows are dropped, (1 - (2/3)^4) = 80 percent of the values.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 13333), 8025);
    /// Unique values go with their rows.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(40000, 40000, 13333), 13333);
    /// Nothing filtered, unknown NDV, and the bound by the rows kept.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 40000), 10000);
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(0, 40000, 13333), 0);
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 5), 5);
}

TEST(JoinOrderBounds, EmptySideMatrix)
{
    using enum JoinKind;
    const auto rows = [](std::optional<UInt64> left, std::optional<UInt64> right, JoinKind kind, JoinStrictness strictness)
    {
        return estimateJoinCardinality(left, right, 0.5, kind, strictness);
    };
    const auto bound = [](std::optional<UInt64> left, std::optional<UInt64> right, JoinKind kind, JoinStrictness strictness)
    {
        return estimateJoinRowsUpperBound(left, right, kind, strictness);
    };
    constexpr auto semi = JoinStrictness::Semi;
    constexpr auto anti = JoinStrictness::Anti;
    constexpr auto all = JoinStrictness::All;

    /// A semi join over an empty side is empty, whichever side and even when the other is unknown.
    EXPECT_EQ(rows(100, 0, Left, semi), 0);
    EXPECT_EQ(rows(0, 100, Right, semi), 0);
    EXPECT_EQ(rows(std::nullopt, 0, Left, semi), 0);
    EXPECT_EQ(rows(0, std::nullopt, Right, semi), 0);
    EXPECT_EQ(bound(100, 0, Left, semi), 0);
    EXPECT_EQ(bound(std::nullopt, 0, Left, semi), 0);
    /// An anti join keeps the whole preserved side when the other side is empty, nothing when it is empty itself.
    EXPECT_EQ(rows(100, 0, Left, anti), 100);
    EXPECT_EQ(rows(0, 100, Left, anti), 0);
    EXPECT_EQ(rows(0, 100, Right, anti), 100);
    EXPECT_EQ(bound(100, 0, Left, anti), 100);
    EXPECT_EQ(bound(std::nullopt, 0, Left, anti), std::nullopt);
    /// An inner join over an empty side is empty, with an unknown other side too.
    EXPECT_EQ(rows(100, 0, Inner, all), 0);
    EXPECT_EQ(rows(std::nullopt, 0, Inner, all), 0);
    EXPECT_EQ(bound(std::nullopt, 0, Inner, all), 0);
    EXPECT_EQ(bound(0, std::nullopt, Cross, all), 0);
    /// An outer join keeps its preserved side; an unknown preserved side stays unknown.
    EXPECT_EQ(rows(100, 0, Left, all), 100);
    EXPECT_EQ(rows(0, 100, Right, all), 100);
    EXPECT_EQ(rows(0, 100, Full, all), 100);
    EXPECT_EQ(rows(std::nullopt, 0, Left, all), std::nullopt);
    EXPECT_EQ(rows(0, std::nullopt, Left, all), 0);
}

TEST(JoinOrderBounds, PasteAnyAsof)
{
    using enum JoinKind;
    constexpr auto all = JoinStrictness::All;
    constexpr auto any = JoinStrictness::Any;
    constexpr auto right_any = JoinStrictness::RightAny;
    constexpr auto asof = JoinStrictness::Asof;

    /// A paste join pairs rows by position: the shorter side, with any selectivity.
    EXPECT_EQ(estimateJoinCardinality(100, 10, 1.0, Paste, all), 10);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 10, Paste, all), 10);
    EXPECT_EQ(estimateJoinRowsUpperBound(std::nullopt, 10, Paste, all), 10);

    /// An outer any or asof join emits every row of the side that takes one match per row.
    EXPECT_EQ(estimateJoinCardinality(100, 100, 1.0, Left, any), 100);
    EXPECT_EQ(estimateJoinCardinality(100, 30, 1.0, Right, any), 30);
    EXPECT_EQ(estimateJoinCardinality(100, 30, 1.0, Right, right_any), 30);
    EXPECT_EQ(estimateJoinCardinality(100, 1000, 1.0, Left, asof), 100);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 1000, Left, asof), 100);
    /// An inner one emits at most that side, and only the matched rows.
    EXPECT_EQ(estimateJoinCardinality(100, 100, 1.0, Inner, any), 100);
    EXPECT_EQ(estimateJoinCardinality(100, 100, 0.001, Inner, any), 10);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 1000, Inner, any), 100);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 1000, Inner, right_any), 1000);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 0, Inner, any), 0);
    EXPECT_EQ(estimateJoinRowsUpperBound(100, 0, Left, any), 100);
}

TEST(JoinOrderBounds, SaturatingConversions)
{
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    /// The upper half of the range stays itself; a signed rounding would fold it into 2^63.
    EXPECT_EQ(roundToRowCount(Float64(UInt64(3) << 62)), UInt64(3) << 62);
    EXPECT_EQ(roundToRowCount(Float64(max)), max);
    EXPECT_EQ(roundToRowCount(1e30), max);
    EXPECT_EQ(roundToRowCount(-5.0), 0);
    EXPECT_EQ(roundToRowCount(std::nan("")), 0);
    EXPECT_EQ(roundToRowCount(2.5), 3);
    EXPECT_EQ(ceilToRowCount(2.1), 3);
    EXPECT_EQ(ceilToRowCount(Float64(UInt64(3) << 62)), UInt64(3) << 62);

    /// A kept fraction below the double precision still leaves the values its rows keep; nothing
    /// survives when no row does.
    EXPECT_EQ(distinctValuesAfterFilter(1000000000000ULL, 1000000000000000000ULL, 1), 1);
    EXPECT_EQ(distinctValuesAfterFilter(5, 0, 0), 0);
    EXPECT_EQ(distinctValuesAfterFilter(5, 100, 0), 0);
}
