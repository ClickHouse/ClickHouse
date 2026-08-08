#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// `Aggregator::estimateSizeOfCompressedState` must serialize the sampled states with the version of the
/// output header's `DataTypeAggregateFunction` - the version `NativeWriter` uses when the states are
/// actually sent - and not with the function's default version. The difference is observable on the merge
/// path, where `aggregate_state_types` preserves the input header's type: a `AggregateFunction(0, sumMap,
/// Array(UInt8), Array(Decimal(9, 2)))` column keeps version 0 on the wire (4-byte `Decimal32` values),
/// while the default version 1 promotes every value to a 16-byte `Decimal128`.

Aggregator::Params makeMergeParams(const Names & keys, const AggregateDescriptions & aggregates)
{
    Aggregator::Params params(
        keys,
        aggregates,
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr,
        /*max_threads_=*/1,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/false,
        /*only_merge_=*/true,
        /*optimize_group_by_constant_keys_=*/true,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/false);
    return params;
}

size_t serializedStateSize(const IAggregateFunction & function, ConstAggregateDataPtr place, std::optional<size_t> version)
{
    WriteBufferFromOwnString buf;
    function.serialize(place, buf, version);
    buf.finalize();
    return buf.str().size();
}

struct VersionedStatesFixture
{
    AggregateFunctionPtr function;
    DataTypePtr state_type_v0;
    /// One state per row; each state's map holds several `Decimal32` values.
    MutableColumnPtr states;
    size_t rows;

    explicit VersionedStatesFixture(size_t rows_) : rows(rows_)
    {
        DataTypes argument_types
            = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()),
               std::make_shared<DataTypeArray>(std::make_shared<DataTypeDecimal<Decimal32>>(9, 2))};
        AggregateFunctionProperties properties;
        function = AggregateFunctionFactory::instance().get("sumMap", NullsAction::EMPTY, argument_types, {}, properties);
        state_type_v0 = std::make_shared<DataTypeAggregateFunction>(function, argument_types, Array{}, /*version=*/0);

        states = state_type_v0->createColumn();
        auto & column = assert_cast<ColumnAggregateFunction &>(*states);

        auto map_keys = argument_types[0]->createColumn();
        auto map_values = argument_types[1]->createColumn();
        for (size_t row = 0; row < rows; ++row)
        {
            Array keys_array;
            Array values_array;
            for (size_t entry = 0; entry < 5; ++entry)
            {
                keys_array.emplace_back(UInt64(row * 5 + entry));
                values_array.emplace_back(DecimalField<Decimal32>(Decimal32(static_cast<Int32>(1000 + row * 100 + entry)), 2));
            }
            map_keys->insert(keys_array);
            map_values->insert(values_array);
        }

        const IColumn * arguments[2] = {map_keys.get(), map_values.get()};
        for (size_t row = 0; row < rows; ++row)
        {
            column.insertDefault();
            function->add(column.getData()[row], arguments, row, &column.createOrGetArena());
        }
    }

    size_t totalSerializedSize(std::optional<size_t> version) const
    {
        const auto & column = assert_cast<const ColumnAggregateFunction &>(*states);
        size_t total = 0;
        for (size_t row = 0; row < rows; ++row)
            total += serializedStateSize(*function, column.getData()[row], version);
        return total;
    }
};

}

TEST(AggregatorStateSizeEstimate, MergePathUsesExplicitStateVersion)
{
    tryRegisterAggregateFunctions();

    VersionedStatesFixture fixture(/*rows_=*/3);

    const size_t expected_v0_bytes = fixture.totalSerializedSize(0);
    const size_t default_version_bytes = fixture.totalSerializedSize(std::nullopt);
    /// The regression is only observable while the versions serialize differently.
    ASSERT_GT(default_version_bytes, expected_v0_bytes);

    AggregateDescription description;
    description.function = fixture.function;
    description.column_name = "st";

    Block header;
    header.insert({std::make_shared<DataTypeUInt8>()->createColumn(), std::make_shared<DataTypeUInt8>(), "k"});
    header.insert({fixture.state_type_v0->createColumn(), fixture.state_type_v0, "st"});

    Aggregator aggregator(header, makeMergeParams({"k"}, {description}));

    auto key_column = ColumnUInt8::create();
    for (size_t row = 0; row < fixture.rows; ++row)
        key_column->insert(UInt64(row));

    auto variants = std::make_shared<AggregatedDataVariants>();
    bool no_more_keys = false;
    std::atomic<bool> is_cancelled{false};
    Columns columns = {std::move(key_column), fixture.states->getPtr()};
    aggregator.mergeOnBlock(columns, fixture.rows, /*is_overflows=*/false, *variants, no_more_keys, is_cancelled);

    /// Every key is distinct, so each merged state equals its input state, and with three states the
    /// sampling period is 1: the sample is exactly the version-0 serialization of all the states.
    const auto estimate = aggregator.estimateSizeOfCompressedState(*variants, /*bucket=*/-1);
    EXPECT_EQ(estimate.sample_bytes, expected_v0_bytes);
    EXPECT_EQ(estimate.bytes, expected_v0_bytes);
}

TEST(AggregatorStateSizeEstimate, SampleSpansTheWholeHashTable)
{
    tryRegisterAggregateFunctions();

    /// `FixedHashTable` iteration goes in key order, so with a `UInt8` key the states are visited from key 0
    /// upward. Give the low keys tiny `groupArray` states and the high keys large ones: a sample truncated to
    /// a prefix of the table would see only tiny states and extrapolate an estimate that misses almost all of
    /// the data, while a periodic sample of the whole table lands within a small factor of the real size.
    constexpr size_t keys = 256;
    constexpr size_t small_keys = 200;
    constexpr size_t large_state_values = 1000;

    DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function
        = AggregateFunctionFactory::instance().get("groupArray", NullsAction::EMPTY, argument_types, {}, properties);
    auto state_type = std::make_shared<DataTypeAggregateFunction>(function, argument_types, Array{});

    auto values = ColumnUInt64::create();
    for (size_t i = 0; i < large_state_values; ++i)
        values->insert(UInt64(i));
    const IColumn * arguments[1] = {values.get()};

    auto states = state_type->createColumn();
    auto & state_column = assert_cast<ColumnAggregateFunction &>(*states);
    for (size_t key = 0; key < keys; ++key)
    {
        state_column.insertDefault();
        const size_t state_values = key < small_keys ? 1 : large_state_values;
        for (size_t i = 0; i < state_values; ++i)
            function->add(state_column.getData()[key], arguments, i, &state_column.createOrGetArena());
    }

    size_t total_serialized_bytes = 0;
    for (size_t key = 0; key < keys; ++key)
        total_serialized_bytes += serializedStateSize(*function, state_column.getData()[key], std::nullopt);

    AggregateDescription description;
    description.function = function;
    description.column_name = "st";

    Block header;
    header.insert({std::make_shared<DataTypeUInt8>()->createColumn(), std::make_shared<DataTypeUInt8>(), "k"});
    header.insert({state_type->createColumn(), state_type, "st"});

    Aggregator aggregator(header, makeMergeParams({"k"}, {description}));

    auto key_column = ColumnUInt8::create();
    for (size_t key = 0; key < keys; ++key)
        key_column->insert(UInt64(key));

    auto variants = std::make_shared<AggregatedDataVariants>();
    bool no_more_keys = false;
    std::atomic<bool> is_cancelled{false};
    Columns columns = {std::move(key_column), states->getPtr()};
    aggregator.mergeOnBlock(columns, keys, /*is_overflows=*/false, *variants, no_more_keys, is_cancelled);

    const auto estimate = aggregator.estimateSizeOfCompressedState(*variants, /*bucket=*/-1);
    /// A periodic sample of the whole table sees the large states in their real proportion (with 256
    /// states the single-level sampling period is 1, so the sample is in fact exact), far from the
    /// >100x underestimation of a prefix-only sample.
    EXPECT_GE(estimate.bytes * 2, total_serialized_bytes);
    EXPECT_LE(estimate.bytes, total_serialized_bytes * 2);
}

TEST(AggregatorStateSizeEstimate, SkewedStatesAreMeasuredExactly)
{
    tryRegisterAggregateFunctions();

    /// A distribution where one giant state holds almost all of the bytes - the shape of e.g.
    /// `uniqExact(UserID) GROUP BY MobilePhoneModel`, where the top group dominates. With a sparse
    /// periodic sample the extrapolation from the sample mean would swing several-fold on whether the
    /// giant lands on a sampled position (and the position depends on the hash table layout, i.e. on
    /// the insertion order): a period of 6 would report either ~6x the real size or a few percent of
    /// it. The single-level sampler aims at ~1000 samples, so a table of 512 states is measured exactly.
    constexpr size_t keys = 512;
    constexpr size_t large_state_values = 100000;

    DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function
        = AggregateFunctionFactory::instance().get("groupArray", NullsAction::EMPTY, argument_types, {}, properties);
    auto state_type = std::make_shared<DataTypeAggregateFunction>(function, argument_types, Array{});

    auto values = ColumnUInt64::create();
    for (size_t i = 0; i < large_state_values; ++i)
        values->insert(UInt64(i));
    const IColumn * arguments[1] = {values.get()};

    auto states = state_type->createColumn();
    auto & state_column = assert_cast<ColumnAggregateFunction &>(*states);
    for (size_t key = 0; key < keys; ++key)
    {
        state_column.insertDefault();
        const size_t state_values = key == 0 ? large_state_values : 1;
        for (size_t i = 0; i < state_values; ++i)
            function->add(state_column.getData()[key], arguments, i, &state_column.createOrGetArena());
    }

    size_t total_serialized_bytes = 0;
    for (size_t key = 0; key < keys; ++key)
        total_serialized_bytes += serializedStateSize(*function, state_column.getData()[key], std::nullopt);

    AggregateDescription description;
    description.function = function;
    description.column_name = "st";

    Block header;
    header.insert({std::make_shared<DataTypeUInt64>()->createColumn(), std::make_shared<DataTypeUInt64>(), "k"});
    header.insert({state_type->createColumn(), state_type, "st"});

    Aggregator aggregator(header, makeMergeParams({"k"}, {description}));

    auto key_column = ColumnUInt64::create();
    for (size_t key = 0; key < keys; ++key)
        key_column->insert(UInt64(key));

    auto variants = std::make_shared<AggregatedDataVariants>();
    bool no_more_keys = false;
    std::atomic<bool> is_cancelled{false};
    Columns columns = {std::move(key_column), states->getPtr()};
    aggregator.mergeOnBlock(columns, keys, /*is_overflows=*/false, *variants, no_more_keys, is_cancelled);

    const auto estimate = aggregator.estimateSizeOfCompressedState(*variants, /*bucket=*/-1);
    EXPECT_EQ(estimate.bytes, total_serialized_bytes);
    EXPECT_EQ(estimate.sample_bytes, total_serialized_bytes);
}

TEST(AggregatorStateSizeEstimate, WithoutKeyUsesExplicitStateVersion)
{
    tryRegisterAggregateFunctions();

    VersionedStatesFixture fixture(/*rows_=*/2);

    AggregateDescription description;
    description.function = fixture.function;
    description.column_name = "st";

    Block header;
    header.insert({fixture.state_type_v0->createColumn(), fixture.state_type_v0, "st"});

    Aggregator aggregator(header, makeMergeParams({}, {description}));

    auto variants = std::make_shared<AggregatedDataVariants>();
    bool no_more_keys = false;
    std::atomic<bool> is_cancelled{false};
    Columns columns = {fixture.states->getPtr()};
    aggregator.mergeOnBlock(columns, fixture.rows, /*is_overflows=*/false, *variants, no_more_keys, is_cancelled);

    ASSERT_NE(variants->without_key, nullptr);
    /// The single aggregate function's state offset is 0.
    const size_t expected_v0_bytes = serializedStateSize(*fixture.function, variants->without_key, 0);
    const size_t default_version_bytes = serializedStateSize(*fixture.function, variants->without_key, std::nullopt);
    ASSERT_GT(default_version_bytes, expected_v0_bytes);

    const auto estimate = aggregator.estimateSizeOfCompressedState(*variants, /*bucket=*/-1);
    EXPECT_EQ(estimate.sample_bytes, expected_v0_bytes);
    EXPECT_EQ(estimate.bytes, expected_v0_bytes);
}
