#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// A `groupArray(UInt64)` column whose serialized state size is skewed along the column: the first
/// `rows / 2` states are empty (one varint on the wire) and the rest hold `elements_in_big_state` values
/// each. This is the shape `AggregatingInOrderTransform` produces for an ordered key where the group size
/// correlates with the key, and the shape on which a prefix sample misestimates the column.
ColumnAggregateFunction::MutablePtr createSkewedGroupArrayColumn(size_t rows, size_t elements_in_big_state)
{
    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get(
        "groupArray", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);

    auto column = ColumnAggregateFunction::create(function);

    auto values = ColumnUInt64::create();
    for (size_t i = 0; i < elements_in_big_state; ++i)
        values->insert(UInt64(42));
    const IColumn * arguments[1] = {values.get()};

    for (size_t row = 0; row < rows; ++row)
    {
        column->insertDefault();
        if (row >= rows / 2)
            for (size_t i = 0; i < elements_in_big_state; ++i)
                function->add(column->getData()[row], arguments, i, &column->createOrGetArena());
    }
    return column;
}

size_t serializedStateBytes(const ColumnAggregateFunction & column, size_t from, size_t to)
{
    size_t total = 0;
    for (size_t row = from; row < to; ++row)
    {
        WriteBufferFromOwnString buf;
        column.getAggregateFunction()->serialize(column.getData()[row], buf, std::nullopt);
        buf.finalize();
        total += buf.str().size();
    }
    return total;
}

}

/// `sampledStateSizes` must sample the whole column periodically, not a prefix: on a column where the
/// state size grows along the column, a prefix sample sees only the small states and the extrapolated
/// total collapses, while a periodic sample lands on both populations.
TEST(ColumnAggregateFunctionSampledStateSizes, PeriodicSampleCoversSkewedColumn)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 20000;
    auto column = createSkewedGroupArrayColumn(rows, /*elements_in_big_state=*/100);

    const size_t exact_bytes = serializedStateBytes(*column, 0, rows);

    /// What a prefix sampler measures on this column: the first `min(8192, rows)` states are all empty,
    /// so the extrapolated total misses the big half entirely.
    const size_t prefix_extrapolated = serializedStateBytes(*column, 0, 8192) * rows / 8192;
    ASSERT_LT(prefix_extrapolated * 10, exact_bytes);

    const auto sizes = column->sampledStateSizes(/*max_states_to_serialize=*/100);

    /// The periodic sample interleaves both populations, so the extrapolated total is close to the truth.
    EXPECT_GE(sizes.bytes * 2, exact_bytes);
    EXPECT_LE(sizes.bytes, exact_bytes * 2);

    /// The uncompressed figure is the one `sampledSerializedStateBytes` returns for the same limit - the
    /// two producers of the estimate must stay in sync.
    EXPECT_EQ(sizes.bytes, column->sampledSerializedStateBytes(100));

    /// The sample honors the cap: about a hundred states, not the whole column.
    EXPECT_LT(sizes.sample_bytes * 10, exact_bytes);
    EXPECT_GT(sizes.sample_bytes, 0u);

    /// The big states are a hundred repetitions of one value each, so the sample must compress.
    EXPECT_LT(sizes.compressed_bytes, sizes.sample_bytes);
    EXPECT_GT(sizes.compressed_bytes, 0u);
}

/// A sample of a few tiny states is smaller than the compressed format's per-block framing (checksum plus
/// header), so the raw compressed size exceeds the sample. Such a sample must be reported as
/// incompressible, not as expanding - a compression ratio below one would inflate the estimate above the
/// uncompressed size.
TEST(ColumnAggregateFunctionSampledStateSizes, TinySampleIsClampedToIncompressible)
{
    tryRegisterAggregateFunctions();

    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get(
        "count", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);

    auto column = ColumnAggregateFunction::create(function);
    auto values = ColumnUInt64::create();
    values->insert(UInt64(1));
    const IColumn * arguments[1] = {values.get()};
    for (size_t row = 0; row < 8; ++row)
    {
        column->insertDefault();
        function->add(column->getData()[row], arguments, 0, &column->createOrGetArena());
    }

    const auto sizes = column->sampledStateSizes(/*max_states_to_serialize=*/100);

    /// Eight `count` states are one varint each - well under the framing.
    EXPECT_EQ(sizes.compressed_bytes, sizes.sample_bytes);
    EXPECT_EQ(sizes.bytes, sizes.sample_bytes);
    EXPECT_GT(sizes.sample_bytes, 0u);
}

TEST(ColumnAggregateFunctionSampledStateSizes, EmptyColumn)
{
    tryRegisterAggregateFunctions();

    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get(
        "count", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);
    auto column = ColumnAggregateFunction::create(function);

    const auto sizes = column->sampledStateSizes(/*max_states_to_serialize=*/100);
    EXPECT_EQ(sizes.bytes, 0u);
    EXPECT_EQ(sizes.sample_bytes, 0u);
    EXPECT_EQ(sizes.compressed_bytes, 0u);
}
