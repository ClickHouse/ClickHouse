#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/RuntimeFilterOrder.h>
#include <base/unit.h>
#include <Common/tests/gtest_global_register.h>

#include <memory>
#include <optional>
#include <vector>

using namespace DB;

namespace
{

/// A step that has a measured selectivity; `probe_nanoseconds_per_row` is set only if it has run on large blocks.
RuntimeFilterStepMeasurement measured(
    Float64 selectivity,
    std::optional<Float64> probe_nanoseconds_per_row = {},
    bool cheap_to_probe = true,
    Float64 rows_in_rejected_blocks_ratio = 0)
{
    RuntimeFilterStepMeasurement step;
    step.selectivity = selectivity;
    step.probe_nanoseconds_per_row = probe_nanoseconds_per_row;
    step.cheap_to_probe = cheap_to_probe;
    step.rows_in_rejected_blocks_ratio = rows_in_rejected_blocks_ratio;
    return step;
}

/// An exact set of the keys `0 .. keys - 1`; with `exact_values_limit` below `keys` its key set is dropped.
std::shared_ptr<RuntimeFilter> makeExactSetFilter(UInt64 keys, UInt64 exact_values_limit, RuntimeFilterConfig config = {})
{
    tryRegisterFunctions();
    auto filter = std::make_shared<RuntimeFilter>(
        /*filters_to_merge_=*/0, config, RuntimeFilter::ExactContains(std::make_shared<DataTypeUInt64>(), 1_MiB, exact_values_limit));
    auto values = ColumnUInt64::create();
    for (UInt64 key = 0; key < keys; ++key)
        values->insertValue(key);
    filter->insert(std::move(values));
    filter->finishInsert();
    return filter;
}

/// Probes a block of `rows` rows with the values `first_value + row % 4`.
void probe(const RuntimeFilter & filter, size_t rows, UInt64 first_value = 0)
{
    auto values = ColumnUInt64::create();
    for (size_t row = 0; row < rows; ++row)
        values->insertValue(first_value + row % 4);
    filter.find({std::move(values), std::make_shared<DataTypeUInt64>(), "key"});
}

}

TEST(RuntimeFilterOrder, UnknownSelectivityKeepsPlannedPlace)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.9), RuntimeFilterStepMeasurement{}}), (PrewhereStepOrder{0, 1}));
    EXPECT_EQ(orderRuntimeFilterSteps({RuntimeFilterStepMeasurement{}, measured(0.01)}), (PrewhereStepOrder{0, 1}));
}

TEST(RuntimeFilterOrder, BothProbeTimesKnownMovesOnLowerProbeTimePerRejectedRow)
{
    /// 5 ns / 0.5 against 10 ns / 0.5.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 10), measured(0.5, 5)}), (PrewhereStepOrder{1, 0}));
    /// Rows of wholly rejected large blocks count twice: 10 ns / (0.5 + 0.5) against 6 ns / 0.5.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 6), measured(0.5, 10, true, 0.5)}), (PrewhereStepOrder{1, 0}));
}

TEST(RuntimeFilterOrder, BothProbeTimesKnownWithinMarginStays)
{
    /// 9.6 ns / 0.5 is 4% lower than 10 ns / 0.5, less than the margin.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 10), measured(0.5, 9.6)}), (PrewhereStepOrder{0, 1}));
}

TEST(RuntimeFilterOrder, NeitherProbeTimeKnownMovesWhenRejectingMore)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5), measured(0.1)}), (PrewhereStepOrder{1, 0}));
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.1), measured(0.5)}), (PrewhereStepOrder{0, 1}));
    /// 0.5 rejected + 0.3 in rejected blocks against 0.7 rejected.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.3), measured(0.5, {}, true, 0.3)}), (PrewhereStepOrder{1, 0}));
}

TEST(RuntimeFilterOrder, OnlyAheadMeasuredMovesCheapMover)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 1), measured(0.1, {}, /*cheap_to_probe=*/ true)}), (PrewhereStepOrder{1, 0}));
    /// A filter that ran first and passed every row it checked goes last.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(1, 2), measured(0.1), measured(0.5)}), (PrewhereStepOrder{1, 2, 0}));
}

TEST(RuntimeFilterOrder, OnlyAheadMeasuredKeepsExpensiveMoverBehindCheapOne)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 1), measured(0.1, {}, /*cheap_to_probe=*/ false)}), (PrewhereStepOrder{0, 1}));
    /// Behind a filter that is not cheap either, it moves.
    EXPECT_EQ(
        orderRuntimeFilterSteps({measured(0.5, 10, /*cheap_to_probe=*/ false), measured(0.1, {}, /*cheap_to_probe=*/ false)}),
        (PrewhereStepOrder{1, 0}));
}

TEST(RuntimeFilterOrder, OnlyAheadMeasuredCountsItsRejectedBlocks)
{
    /// 0.8 rejected against 0.5 rejected + 0.4 in wholly rejected large blocks.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 1, true, 0.4), measured(0.2)}), (PrewhereStepOrder{0, 1}));
}

TEST(RuntimeFilterOrder, OnlyMoverMeasured)
{
    /// 0.4 rejected + 0.2 in wholly rejected large blocks against 0.5 rejected.
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5), measured(0.6, 1, true, 0.2)}), (PrewhereStepOrder{1, 0}));
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5), measured(0.6, 1)}), (PrewhereStepOrder{0, 1}));
}

TEST(RuntimeFilterOrder, ThreeStepsBubble)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.9), measured(0.5), measured(0.1)}), (PrewhereStepOrder{2, 1, 0}));
}

TEST(RuntimeFilterOrder, NearEqualRejectionStays)
{
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5), measured(0.47)}), (PrewhereStepOrder{0, 1}));
    EXPECT_EQ(orderRuntimeFilterSteps({measured(0.5, 1), measured(0.47)}), (PrewhereStepOrder{0, 1}));
}

TEST(RuntimeFilterOrder, SelectivityNeedsEnoughCheckedRows)
{
    const auto filter = makeExactSetFilter(1, 100);
    probe(*filter, 30000);
    ASSERT_EQ(filter->getStats().rows_checked.load(), 30000);

    EXPECT_FALSE(measureRuntimeFilterStep({filter}).selectivity.has_value());
}

TEST(RuntimeFilterOrder, SelectivityComesFromCheckedRows)
{
    /// Passes the key 0 of `row % 4`, more than the threshold, so it skips the blocks after the first.
    const auto filter
        = makeExactSetFilter(1, 100, RuntimeFilterConfig{.pass_ratio_threshold_for_disabling = 0.1, .blocks_to_skip_before_reenabling = 30});
    probe(*filter, 40000);
    for (size_t block = 0; block < 2; ++block)
        probe(*filter, 10000);
    ASSERT_EQ(filter->getStats().rows_checked.load(), 40000);
    ASSERT_EQ(filter->getStats().rows_skipped.load(), 20000);

    const auto step = measureRuntimeFilterStep({filter});
    ASSERT_TRUE(step.selectivity.has_value());
    EXPECT_DOUBLE_EQ(*step.selectivity, 0.25);
}

TEST(RuntimeFilterOrder, DroppedKeySetPassesEveryRow)
{
    const auto filter = makeExactSetFilter(3, 2);
    ASSERT_TRUE(filter->isKeySetDropped());
    probe(*filter, 100);
    ASSERT_EQ(filter->getStats().rows_checked.load(), 0);

    const auto step = measureRuntimeFilterStep({filter});
    ASSERT_TRUE(step.selectivity.has_value());
    EXPECT_DOUBLE_EQ(*step.selectivity, 1);
}

TEST(RuntimeFilterOrder, StepOfSeveralFiltersCombinesTheirMeasurements)
{
    /// Too many keys to be cheap. Passes 1/2 of two blocks and rejects a third whole: selectivity 1/3, 1/3 in rejected blocks.
    const auto expensive = makeExactSetFilter(5000, 10000);
    probe(*expensive, 65536, 4998);
    probe(*expensive, 65536, 4998);
    probe(*expensive, 65536, 5000);
    /// Passes 1/4 of three blocks and rejects a fourth whole: selectivity 3/16, 1/4 in rejected blocks.
    const auto cheap = makeExactSetFilter(1, 100);
    for (size_t block = 0; block < 3; ++block)
        probe(*cheap, 65536);
    probe(*cheap, 65536, 4);

    const auto expensive_step = measureRuntimeFilterStep({expensive});
    const auto cheap_step = measureRuntimeFilterStep({cheap});
    ASSERT_TRUE(expensive_step.probe_nanoseconds_per_row.has_value());
    ASSERT_TRUE(cheap_step.probe_nanoseconds_per_row.has_value());
    ASSERT_FALSE(expensive_step.cheap_to_probe);
    ASSERT_TRUE(cheap_step.cheap_to_probe);

    const auto step = measureRuntimeFilterStep({expensive, cheap});
    ASSERT_TRUE(step.selectivity.has_value());
    EXPECT_DOUBLE_EQ(*step.selectivity, 1.0 / 16);
    ASSERT_TRUE(step.probe_nanoseconds_per_row.has_value());
    EXPECT_DOUBLE_EQ(*step.probe_nanoseconds_per_row, *expensive_step.probe_nanoseconds_per_row + *cheap_step.probe_nanoseconds_per_row);
    EXPECT_DOUBLE_EQ(step.rows_in_rejected_blocks_ratio, 1.0 / 3);
    EXPECT_FALSE(step.cheap_to_probe);

    /// One filter not registered yet leaves the step unmeasured.
    EXPECT_FALSE(measureRuntimeFilterStep({cheap, nullptr}).selectivity.has_value());
}
