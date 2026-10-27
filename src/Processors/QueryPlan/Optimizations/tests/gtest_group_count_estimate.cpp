#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(GroupCountEstimate, SharedFormula)
{
    /// No keys: one group.
    auto global = estimateGroupCount(std::vector<UInt64>{}, 1000, 1000);
    EXPECT_EQ(global.estimated_rows, 1);
    EXPECT_EQ(global.max_rows, 1);

    /// The largest key NDV is the estimate; the bound stays the input's, the NDVs are estimates.
    auto two_keys = estimateGroupCount({100, 50}, 100000, 100000);
    EXPECT_EQ(two_keys.estimated_rows, 100);
    EXPECT_EQ(two_keys.max_rows, 100000);

    /// Both are capped by the rows.
    auto few_rows = estimateGroupCount({100, 50}, 60, 60);
    EXPECT_EQ(few_rows.estimated_rows, 60);
    EXPECT_EQ(few_rows.max_rows, 60);

    /// A key without an NDV leaves the estimate to the other keys and the bound to the input.
    auto one_unknown = estimateGroupCount({0, 50}, 100000, 100000);
    EXPECT_EQ(one_unknown.estimated_rows, 50);
    EXPECT_EQ(one_unknown.max_rows, 100000);

    /// No key NDV: unknown, bounded by the input.
    auto all_unknown = estimateGroupCount({0, 0}, 100000, 200000);
    EXPECT_EQ(all_unknown.estimated_rows, std::nullopt);
    EXPECT_EQ(all_unknown.max_rows, 200000);

    /// Unknown input rows leave the estimate uncapped and the bound unknown: an NDV undercount
    /// must not become a proven cap.
    auto unknown_rows = estimateGroupCount({100}, std::nullopt, std::nullopt);
    EXPECT_EQ(unknown_rows.estimated_rows, 100);
    EXPECT_EQ(unknown_rows.max_rows, std::nullopt);

}

TEST(GroupCountEstimate, MultiColumnDistinctModels)
{
    /// Three keys of 335, 5163 and 5000 values over 53M rows, the shape of TPC-DS Q38.
    const std::vector<double> distinct_values = {335, 5163, 5000};
    const double rows = 53'000'000;
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations(distinct_values, rows, MultiColumnDistinctModel::Largest), 5163);
    /// 5163 * 5000 * 335 is above the rows, so the product is the rows.
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations(distinct_values, rows, MultiColumnDistinctModel::Product), rows);
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations(distinct_values, rows, MultiColumnDistinctModel::GeometricMean), std::sqrt(5163 * rows));
    /// 5163 * 5000^(1/2) * 335^(1/4).
    EXPECT_NEAR(estimateDistinctCombinations(distinct_values, rows, MultiColumnDistinctModel::Backoff), 1'561'882, 1);

    /// Every model is at most the rows, and one key is its own NDV.
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations({100, 100}, 50, MultiColumnDistinctModel::Backoff), 50);
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations({100}, 1000, MultiColumnDistinctModel::Backoff), 100);
    EXPECT_DOUBLE_EQ(estimateDistinctCombinations({}, 1000, MultiColumnDistinctModel::Product), 1);
}
