#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// A `groupArray(UInt64)` column of the shape `AggregatingInOrderTransform` produces for a skewed ordered
/// aggregation: every group's state is empty (one varint on the wire) except one, which holds
/// `elements_in_giant_state` distinct values. The distinct values keep the giant state incompressible, so
/// missing it in the sample understates the compressed figure as well as the uncompressed one.
ColumnAggregateFunction::MutablePtr
createSkewedGroupArrayColumn(size_t rows, size_t giant_state_row, size_t elements_in_giant_state, AggregateFunctionPtr & function)
{
    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(
        "groupArray", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);

    auto column = ColumnAggregateFunction::create(function);

    auto values = ColumnUInt64::create();
    for (size_t i = 0; i < elements_in_giant_state; ++i)
        values->insert(UInt64(i) * 0x9E3779B97F4A7C15ULL);
    const IColumn * arguments[1] = {values.get()};

    for (size_t row = 0; row < rows; ++row)
    {
        column->insertDefault();
        if (row == giant_state_row)
            for (size_t i = 0; i < elements_in_giant_state; ++i)
                function->add(column->getData()[row], arguments, i, &column->createOrGetArena());
    }
    return column;
}

}

/// The in-order producer of the `AggregationState` statistic must measure a block of a few hundred states
/// exactly, like `Aggregator::estimateSizeOfCompressedState` measures a single-level hash table of that
/// size: with a sample of only 100 states, one giant state among a few hundred tiny ones swings the
/// extrapolation several-fold depending on whether it lands on a sampled position (512 rows gave period 6,
/// and a giant state on an off-grid row was missed entirely), which can flip the automatic parallel
/// replicas decision on skewed ordered aggregations.
TEST(RuntimeDataflowStatisticsStateSampling, SkewedInOrderStatesAreMeasuredExactly)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    /// Not a multiple of the pre-fix sampling period (ceil(512 / 100) = 6), so a 100-state sample misses it.
    constexpr size_t giant_state_row = 511;
    constexpr size_t elements_in_giant_state = 1000;

    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(rows, giant_state_row, elements_in_giant_state, function);

    /// The exact figures: with the limit not smaller than the column's size, every state is serialized,
    /// so `bytes == sample_bytes` and the expected estimate is `bytes / (sample_bytes / compressed_bytes)`
    /// = `compressed_bytes`.
    const auto exact = column->sampledStateSizes(rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    /// The giant state dominates the column and does not compress, so an estimate that misses it is off
    /// by far more than the 2x margin asserted below.
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    const size_t cache_key = 0x111985;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{
            nullptr, std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}), "state"});

        Chunk chunk(Columns{std::move(column)}, rows);
        updater.recordAggregationStateColumnSizes(chunk, /*keys_positions=*/{}, header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}
