#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(KeySelectivity, MostSelectiveKeyDecides)
{
    /// No predicates: everything matches, no side has a known match fraction. Several keys: the
    /// most selective one decides, and the smallest containment of each side.
    JoinKeyEstimate none;
    EXPECT_DOUBLE_EQ(none.selectivity, 1.0);
    EXPECT_FALSE(none.has_equality);
    EXPECT_EQ(none.left_match_fraction, std::nullopt);

    JoinKeyEstimate keys;
    keys.add({.from_statistics = 10}, {.from_statistics = 10}, 0);
    keys.add({.from_statistics = 1000}, {.from_statistics = 100}, 0);
    keys.add({}, {}, 0);  /// Both NDVs unknown: still an equality, but no key to size it by.
    EXPECT_DOUBLE_EQ(keys.selectivity, 0.001);
    EXPECT_DOUBLE_EQ(keys.statistics_selectivity, 0.001);
    EXPECT_TRUE(keys.has_equality);
    EXPECT_TRUE(keys.from_statistics);
    /// 100 right values lie within the 1000 left values: a tenth of the left side matches.
    EXPECT_DOUBLE_EQ(*keys.left_match_fraction, 0.1);
    EXPECT_DOUBLE_EQ(*keys.right_match_fraction, 1.0);
}

TEST(KeySelectivity, RowsStandInForMissingDistinctValues)
{
    /// A key without an NDV counts its relation's rows in the reported selectivity, not in the
    /// one from statistics, and neither side gets a match fraction: 1000 rows say nothing about
    /// how many of the 100 values they hold.
    JoinKeyEstimate keys;
    keys.add({.rows = 1000}, {.from_statistics = 100, .rows = 100}, 0);
    EXPECT_DOUBLE_EQ(keys.selectivity, 0.001);
    EXPECT_DOUBLE_EQ(keys.statistics_selectivity, 0.01);
    EXPECT_TRUE(keys.from_statistics);
    EXPECT_EQ(keys.left_match_fraction, std::nullopt);
    EXPECT_EQ(keys.right_match_fraction, std::nullopt);

    /// A probe side built from the preserved side's keys: a semi join keeps the share of preserved
    /// keys the probe side has, with or without NDVs; an anti join keeps the domain-based fraction.
    JoinKeyEstimate within;
    within.probe_keys_within_preserved_side = true;
    within.add({.from_statistics = 1000}, {.rows = 100}, 10000);
    EXPECT_DOUBLE_EQ(*within.left_within_fraction, 0.1);
    EXPECT_DOUBLE_EQ(*within.right_within_fraction, 1.0);
    EXPECT_EQ(within.left_match_fraction, std::nullopt);
    EXPECT_EQ(estimateJoinCardinality(1000, 100, within, JoinKind::Left, JoinStrictness::Semi), 100);
    EXPECT_EQ(estimateJoinCardinality(1000, 100, within, JoinKind::Left, JoinStrictness::Anti), 900);

    /// Equal key counts from statistics: the semi join keeps every preserved row, the anti join
    /// still keeps the share the domain gives, not one row.
    JoinKeyEstimate equal_keys;
    equal_keys.probe_keys_within_preserved_side = true;
    equal_keys.add({.from_statistics = 1000}, {.from_statistics = 1000}, 10000);
    EXPECT_EQ(estimateJoinCardinality(1000, 1000, equal_keys, JoinKind::Left, JoinStrictness::Semi), 1000);
    EXPECT_EQ(estimateJoinCardinality(1000, 1000, equal_keys, JoinKind::Left, JoinStrictness::Anti), 900);

    /// Nothing from statistics: the join is sized as a foreign key meeting a primary key.
    JoinKeyEstimate rows_only;
    rows_only.add({.rows = 1000}, {.rows = 100}, 0);
    EXPECT_DOUBLE_EQ(rows_only.selectivity, 0.001);
    EXPECT_FALSE(rows_only.from_statistics);
}

TEST(KeySelectivity, BoundStandsInForUnknownRows)
{
    /// A relation whose rows are unknown gives the rows it cannot exceed, so an equality between two
    /// such relations still has a selectivity instead of pricing the join as a cross product.
    RelationStats bounded;
    bounded.max_rows = 1000;
    bounded.column_stats["k"] = ColumnStats{};
    const auto key = keyDistinctValues(bounded, "k");
    EXPECT_EQ(key.from_statistics, 0u);
    EXPECT_EQ(key.rows, 1000u);

    JoinKeyEstimate keys;
    keys.add(key, key, 0);
    EXPECT_DOUBLE_EQ(keys.selectivity, 0.001);
    EXPECT_FALSE(keys.from_statistics);

    RelationStats nothing;
    EXPECT_EQ(keyDistinctValues(nothing, "k").rows, 0u);
}

TEST(KeySelectivity, JoinedRowsByWhatIsKnown)
{
    /// A key NDV from statistics sizes the join as the product over it, whichever side it is on.
    JoinKeyEstimate from_statistics;
    from_statistics.add({.rows = 1000}, {.from_statistics = 100, .rows = 100}, 0);
    EXPECT_DOUBLE_EQ(estimateJoinedRows(from_statistics, 1000, 100), 1000);
    EXPECT_DOUBLE_EQ(estimateJoinedRows(from_statistics, 50, 100), 50);

    /// An equality without any NDV keeps the larger side: a foreign key meeting a primary key.
    JoinKeyEstimate rows_only;
    rows_only.add({.rows = 1000}, {.rows = 100}, 0);
    EXPECT_DOUBLE_EQ(estimateJoinedRows(rows_only, 1000, 100), 1000);
    EXPECT_EQ(estimateJoinCardinality(1000, 100, rows_only, JoinKind::Inner), 1000);
    /// A semi join against it keeps the rows per key of the larger side: 100 of 1000.
    EXPECT_EQ(estimateJoinCardinality(1000, 100, rows_only, JoinKind::Left, JoinStrictness::Semi), 100);
    /// With the key NDV known on the other side every preserved row refers to one of its keys.
    EXPECT_EQ(estimateJoinCardinality(1000, 100, from_statistics, JoinKind::Left, JoinStrictness::Semi), 1000);

    /// No equality: the product.
    JoinKeyEstimate none;
    EXPECT_DOUBLE_EQ(estimateJoinedRows(none, 1000, 100), 100000);
}
