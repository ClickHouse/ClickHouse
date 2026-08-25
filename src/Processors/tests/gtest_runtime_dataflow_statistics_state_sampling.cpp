#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeWriter.h>
#include <IO/NullWriteBuffer.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Common/Arena.h>
#include <Common/tests/gtest_global_register.h>

#include <fmt/format.h>

using namespace DB;

namespace
{

/// The compressed size of a whole column as `NativeWriter` puts it on the wire - the ground truth the
/// estimate approximates for a materialized column.
size_t compressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    NativeWriter::writeData(
        *serialization, column_to_write, compressed_buf, std::nullopt, 0, column_to_write->size(), DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    return null_buf.count();
}

/// A `groupArray(UInt64)` column of the shape `AggregatingInOrderTransform` produces for a skewed ordered
/// aggregation: every group's state is empty (one varint on the wire) except one, which holds
/// `elements_in_giant_state` distinct values. The distinct values keep the giant state incompressible, so
/// missing it in the sample understates the compressed figure as well as the uncompressed one.
ColumnAggregateFunction::MutablePtr createSkewedGroupArrayColumn(
    size_t rows, size_t giant_state_row, size_t elements_in_giant_state, AggregateFunctionPtr & function, Arena * states_arena = nullptr)
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
                function->add(column->getData()[row], arguments, i, states_arena ? states_arena : &column->createOrGetArena());
    }
    return column;
}

/// A `groupArray(String)` column whose states really live in the arena the column owns: unlike the numeric
/// `groupArray`, the generic implementation allocates its nodes in an arena, so `byteSize` counts the whole
/// state payload. Every row is an empty state except `filled_state_row`, which holds `elements_in_state`
/// short strings. Short values make the arena bookkeeping per element - a node header and a pointer -
/// several times the element's wire size, which is what the carrier accounting must not count twice.
ColumnAggregateFunction::MutablePtr
createGroupArrayStringColumn(size_t rows, size_t filled_state_row, size_t elements_in_state, AggregateFunctionPtr & function)
{
    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(
        "groupArray", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeString>()}, Array{}, properties);

    auto column = ColumnAggregateFunction::create(function);

    auto values = ColumnString::create();
    for (size_t i = 0; i < elements_in_state; ++i)
        values->insert(fmt::format("{:02x}", (i * 0x9E) & 0xFF));
    const IColumn * arguments[1] = {values.get()};

    for (size_t row = 0; row < rows; ++row)
    {
        column->insertDefault();
        if (row == filled_state_row)
            for (size_t i = 0; i < elements_in_state; ++i)
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

/// Final `-State` results can leave the query wrapped in carrier columns: `prepareOutputBlockColumns`
/// attaches the shared arenas to the nested `ColumnAggregateFunction` leaves of `isState()` results, so
/// e.g. `SELECT tuple(groupArrayState(x))` emits a `ColumnTuple` around an aggregate-state leaf whose
/// states live in arenas the leaf does not own. `recordOutputChunk` must size such leaves from their
/// serialized states like top-level ones: the wrappers' `byteSize` just sums the nested `byteSize`, which
/// counts one pointer per row and drops the shared-arena payload, so the `OutputChunk` figure
/// under-measures exactly the large-state outputs whose plan choice the statistic is supposed to steer.
TEST(RuntimeDataflowStatisticsStateSampling, WrappedStatesAreSizedFromSerializedStates)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    constexpr size_t giant_state_row = 511;
    /// Large enough that the serialized states dwarf `byteSize` of a leaf that does not own the arena
    /// (one pointer per row plus the empty states) by far more than the 2x margin asserted below.
    constexpr size_t elements_in_giant_state = 100000;

    /// The states' payload lives in a shared arena, like after `addArenasToAggregateColumns` /
    /// `prepareOutputBlockColumns`, so the leaf's `byteSize` drops it.
    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(rows, giant_state_row, elements_in_giant_state, function, states_arena.get());
    column->addArena(states_arena);

    const auto exact = column->sampledStateSizes(rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    const size_t cache_key = 0x111985 + 1;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, std::make_shared<DataTypeTuple>(DataTypes{state_type}), "wrapped_state"});

        Columns tuple_elements;
        tuple_elements.emplace_back(std::move(column));
        Chunk chunk(Columns{ColumnTuple::create(std::move(tuple_elements))}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}

/// A state-bearing carrier can also hold non-state payload - e.g. `SELECT tuple(groupArrayState(x), s)`
/// emits a `ColumnTuple` around a state leaf and a plain sibling string. The leaves' serialized-state
/// sample says nothing about the sibling's compressibility, so the sibling needs a compression sample of
/// its own: with only the (incompressible) states sampled, a highly compressible sibling that dominates
/// the column would be estimated at the leaves' ratio of ~1, overstating `output_bytes` by the sibling's
/// real compression ratio.
TEST(RuntimeDataflowStatisticsStateSampling, MixedWrapperSamplesNonStatePayloadCompression)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    constexpr size_t giant_state_row = 511;
    constexpr size_t elements_in_giant_state = 100000;
    /// The sibling string column dwarfs the states uncompressed and vanishes compressed.
    constexpr size_t string_size = 10240;

    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(rows, giant_state_row, elements_in_giant_state, function, states_arena.get());
    column->addArena(states_arena);

    const auto exact = column->sampledStateSizes(rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    auto string_column = ColumnString::create();
    const std::string value(string_size, 'a');
    for (size_t row = 0; row < rows; ++row)
        string_column->insertData(value.data(), value.size());
    /// Uncompressed the sibling dominates the states by far more than the 2x margin asserted below, so an
    /// estimate that applies the states' ratio of ~1 to it lands several-fold above the upper bound.
    ASSERT_GT(string_column->byteSize(), exact.compressed_bytes * 4);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    const size_t cache_key = 0x111985 + 2;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{
            nullptr,
            std::make_shared<DataTypeTuple>(DataTypes{state_type, std::make_shared<DataTypeString>()}),
            "wrapped_state_and_string"});

        Columns tuple_elements;
        tuple_elements.emplace_back(std::move(column));
        tuple_elements.emplace_back(std::move(string_column));
        Chunk chunk(Columns{ColumnTuple::create(std::move(tuple_elements))}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    /// The sibling compresses to almost nothing, so the whole column's compressed size is the states'.
    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}

/// A state leaf can also sit inside a `ColumnVariant` - `Variant` allows `AggregateFunction` alternatives,
/// so e.g. `if(cond, groupArrayState(x), CAST(s, 'Variant(...)'))` emits a `ColumnVariant` whose
/// alternatives are a state leaf and a plain string. The walk that samples the non-state payload of a
/// state-bearing carrier must recurse through the variant like it does through tuples/arrays/maps: falling
/// back to "count the carrier's own payload as incompressible" applies a compression ratio of 1 to the
/// discriminators, offsets and the string alternative, so a compressible alternative that dominates the
/// column uncompressed overstates `output_bytes` by its real compression ratio.
TEST(RuntimeDataflowStatisticsStateSampling, VariantWithStateAlternativeSamplesNonStatePayloadCompression)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    constexpr size_t state_rows = 256;
    constexpr size_t giant_state_row = 255;
    constexpr size_t elements_in_giant_state = 50000;
    /// The string alternative's rows dwarf the states uncompressed and vanish compressed.
    constexpr size_t string_size = 10240;

    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(state_rows, giant_state_row, elements_in_giant_state, function, states_arena.get());
    column->addArena(states_arena);

    const auto exact = column->sampledStateSizes(state_rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    auto string_column = ColumnString::create();
    const std::string value(string_size, 'a');
    for (size_t row = 0; row < rows - state_rows; ++row)
        string_column->insertData(value.data(), value.size());
    /// Uncompressed the string alternative dominates the states by far more than the 2x margin asserted
    /// below, so an estimate that counts it as incompressible lands several-fold above the upper bound.
    ASSERT_GT(string_column->byteSize(), exact.compressed_bytes * 4);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    /// The first `state_rows` rows hold the states, the rest the strings.
    auto discriminators = ColumnVariant::ColumnDiscriminators::create();
    auto offsets = ColumnVariant::ColumnOffsets::create();
    for (size_t row = 0; row < rows; ++row)
    {
        discriminators->insertValue(row < state_rows ? 0 : 1);
        offsets->insertValue(row < state_rows ? row : row - state_rows);
    }

    const size_t cache_key = 0x111985 + 4;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{
            nullptr,
            std::make_shared<DataTypeVariant>(DataTypes{state_type, std::make_shared<DataTypeString>()}),
            "variant_state_or_string"});

        Columns variant_alternatives;
        variant_alternatives.emplace_back(std::move(column));
        variant_alternatives.emplace_back(std::move(string_column));
        Chunk chunk(Columns{ColumnVariant::create(std::move(discriminators), std::move(offsets), variant_alternatives)}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    /// The string alternative compresses to almost nothing, so the whole column's compressed size is the
    /// states'.
    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}

/// A state alternative can be absent from a sampled `Variant` block. Its aggregate-state leaf then has
/// zero rows, even though the outer block is non-empty. This must not establish a zero-byte sample for
/// later blocks: the Native stream contains no state values in the first block, while following blocks can
/// contain large state payloads. Keep sampling until at least one state value was serialized.
TEST(RuntimeDataflowStatisticsStateSampling, VariantWithoutSampledStateValuesDoesNotExtrapolateFromZero)
{
    tryRegisterAggregateFunctions();

    constexpr size_t string_rows = 100;
    constexpr size_t state_rows = 200;
    constexpr size_t state_blocks = 4;
    constexpr size_t elements_in_state = 4000;

    AggregateFunctionPtr function;
    auto source_state = createSkewedGroupArrayColumn(/*rows=*/1, /*giant_state_row=*/0, elements_in_state, function);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});
    const auto variant_type = std::make_shared<DataTypeVariant>(DataTypes{state_type, std::make_shared<DataTypeString>()});
    ASSERT_EQ(variant_type->getVariants()[0]->getName(), state_type->getName());

    const auto make_string_variant = [&]
    {
        auto empty_states = ColumnAggregateFunction::create(function);
        auto strings = ColumnString::create();
        auto discriminators = ColumnVariant::ColumnDiscriminators::create();
        auto offsets = ColumnVariant::ColumnOffsets::create();
        for (size_t row = 0; row < string_rows; ++row)
        {
            strings->insertData("sample", 6);
            discriminators->insertValue(1);
            offsets->insertValue(row);
        }
        Columns alternatives;
        alternatives.emplace_back(std::move(empty_states));
        alternatives.emplace_back(std::move(strings));
        return ColumnVariant::create(std::move(discriminators), std::move(offsets), alternatives);
    };
    const auto make_state_variant = [&]
    {
        auto states = ColumnAggregateFunction::create(function);
        auto empty_strings = ColumnString::create();
        auto discriminators = ColumnVariant::ColumnDiscriminators::create();
        auto offsets = ColumnVariant::ColumnOffsets::create();
        for (size_t row = 0; row < state_rows; ++row)
        {
            states->insertFrom(*source_state, 0);
            discriminators->insertValue(0);
            offsets->insertValue(row);
        }
        Columns alternatives;
        alternatives.emplace_back(std::move(states));
        alternatives.emplace_back(std::move(empty_strings));
        return ColumnVariant::create(std::move(discriminators), std::move(offsets), alternatives);
    };

    const size_t cache_key = 0x111985 + 8;
    size_t exact_compressed_bytes = 0;
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, string_rows + state_rows * state_blocks);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, variant_type, "variant_state_or_string"});

        auto sample = make_string_variant();
        exact_compressed_bytes += compressedColumnSize({sample, variant_type, "variant_state_or_string"});
        updater.recordOutputChunk(Chunk(Columns{std::move(sample)}, string_rows), header);

        for (size_t block = 0; block < state_blocks; ++block)
        {
            auto states = make_state_variant();
            exact_compressed_bytes += compressedColumnSize({states, variant_type, "variant_state_or_string"});
            updater.recordOutputChunk(Chunk(Columns{std::move(states)}, state_rows), header);
        }
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

/// Blocks racing with the first sampled one serialize their states before a per-state-value figure exists
/// (`serialize_states` without `sample_block`), and their states enter `sample_bytes`/`compressed_bytes`
/// while their wrapper payload enters the byte total. Such a block must sample its carriers' non-state
/// payload too, or the compression ratio is derived from a different population of bytes than the total it
/// divides: a compressible string alternative dominating the forced block keeps the states' ratio of ~1 and
/// overstates `output_bytes` several-fold. The first, sampled block below carries no state values - the
/// deterministic equivalent of the concurrent startup race - so the second, unsampled block with an
/// incompressible state and dominant compressible strings takes exactly the forced path.
TEST(RuntimeDataflowStatisticsStateSampling, ForcedStateSerializationSamplesNonStatePayloadCompression)
{
    tryRegisterAggregateFunctions();

    constexpr size_t first_block_rows = 16;
    constexpr size_t state_rows = 256;
    constexpr size_t giant_state_row = 255;
    constexpr size_t elements_in_giant_state = 50000;
    /// Uncompressed the strings of the second block dwarf its states by far more than the 2x margin
    /// asserted below, and compressed they vanish, so counting them as incompressible lands several-fold
    /// above the upper bound.
    constexpr size_t string_rows = 256;
    constexpr size_t string_size = 10240;

    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto states = createSkewedGroupArrayColumn(state_rows, giant_state_row, elements_in_giant_state, function, states_arena.get());
    states->addArena(states_arena);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});
    const auto variant_type = std::make_shared<DataTypeVariant>(DataTypes{state_type, std::make_shared<DataTypeString>()});
    ASSERT_EQ(variant_type->getVariants()[0]->getName(), state_type->getName());

    /// The state alternative is empty, so this block establishes no per-state-value figure.
    const auto make_first_block = [&]
    {
        auto empty_states = ColumnAggregateFunction::create(function);
        auto strings = ColumnString::create();
        auto discriminators = ColumnVariant::ColumnDiscriminators::create();
        auto offsets = ColumnVariant::ColumnOffsets::create();
        for (size_t row = 0; row < first_block_rows; ++row)
        {
            strings->insertData("sample", 6);
            discriminators->insertValue(1);
            offsets->insertValue(row);
        }
        Columns alternatives;
        alternatives.emplace_back(std::move(empty_states));
        alternatives.emplace_back(std::move(strings));
        return ColumnVariant::create(std::move(discriminators), std::move(offsets), alternatives);
    };
    /// The first `state_rows` rows hold the states, the rest the compressible strings.
    const auto make_forced_block = [&]
    {
        auto strings = ColumnString::create();
        const std::string value(string_size, 'a');
        for (size_t row = 0; row < string_rows; ++row)
            strings->insertData(value.data(), value.size());
        auto discriminators = ColumnVariant::ColumnDiscriminators::create();
        auto offsets = ColumnVariant::ColumnOffsets::create();
        for (size_t row = 0; row < state_rows + string_rows; ++row)
        {
            discriminators->insertValue(row < state_rows ? 0 : 1);
            offsets->insertValue(row < state_rows ? row : row - state_rows);
        }
        Columns alternatives;
        alternatives.emplace_back(std::move(states));
        alternatives.emplace_back(std::move(strings));
        return ColumnVariant::create(std::move(discriminators), std::move(offsets), alternatives);
    };

    const size_t cache_key = 0x111985 + 14;
    size_t exact_compressed_bytes = 0;
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, first_block_rows + state_rows + string_rows);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, variant_type, "variant_state_or_string"});

        auto first_block = make_first_block();
        exact_compressed_bytes += compressedColumnSize({first_block, variant_type, "variant_state_or_string"});
        updater.recordOutputChunk(Chunk(Columns{std::move(first_block)}, first_block_rows), header);

        auto forced_block = make_forced_block();
        exact_compressed_bytes += compressedColumnSize({forced_block, variant_type, "variant_state_or_string"});
        updater.recordOutputChunk(Chunk(Columns{std::move(forced_block)}, state_rows + string_rows), header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

/// Materializing a constant sparse state column repeats its non-default values, not the implicit default
/// that `ColumnSparse` retains at `values[0]`. The repeated aggregate-state sample must use the same
/// skipped-row offset as the one-copy sample, or it measures the default state instead of the payload.
TEST(RuntimeDataflowStatisticsStateSampling, ConstantSparseStateSamplesRepeatedPayload)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 200;
    constexpr size_t elements_in_state = 4000;

    AggregateFunctionPtr function;
    auto source_state = createSkewedGroupArrayColumn(/*rows=*/1, /*giant_state_row=*/0, elements_in_state, function);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    MutableColumnPtr values = ColumnAggregateFunction::create(function);
    values->insertDefault();
    values->insertFrom(*source_state, 0);
    auto offsets = ColumnUInt64::create();
    offsets->insert(0);
    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), /*size_=*/1);
    ColumnPtr constant = ColumnConst::create(std::move(sparse), rows);

    /// `NativeWriter` cannot put a `ColumnConst` over a `ColumnSparse` on the wire at all - materializing
    /// the constant keeps the sparse layout, which the state's serialization does not accept - so the wire
    /// ground truth is the equivalent materialized column: the constant's single non-default state, once
    /// per row.
    auto materialized = ColumnAggregateFunction::create(function);
    for (size_t row = 0; row < rows; ++row)
        materialized->insertFrom(*source_state, 0);

    const size_t cache_key = 0x111985 + 10;
    const auto exact_compressed_bytes
        = compressedColumnSize({std::move(materialized), state_type, "constant_sparse_state"});
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, state_type, "constant_sparse_state"});
        updater.recordOutputChunk(Chunk(Columns{std::move(constant)}, rows), header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

/// The state leaf of a `ColumnSparse` is a `cut` view of its `values`, which shares the states instead of
/// owning them, so the view's `byteSize` is one pointer per row while the sparse carrier's `byteSize` still
/// counts the original `values` column together with the arena it owns. Sizing the carrier's own payload as
/// `byteSize` minus the leaf must subtract the original column's bytes, or the whole state payload stays in
/// the plain bytes and is counted a second time by the serialized-state measurement.
TEST(RuntimeDataflowStatisticsStateSampling, SparseStateArenaIsNotCountedAsPlainPayload)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 200;
    /// Small enough that a copy of the serialized state fits the codec's match window, so the repeated
    /// payload compresses well and the arena bytes the pre-fix accounting left in `plain_bytes` dominate.
    constexpr size_t elements_in_state = 4000;

    AggregateFunctionPtr function;
    /// Row zero is the sparse implicit default, row one the stored value; unlike a column assembled with
    /// `insertFrom`, this one builds its states in its own arena, which `byteSize` counts.
    auto values = createGroupArrayStringColumn(/*rows=*/2, /*filled_state_row=*/1, elements_in_state, function);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeString>()}, Array{});
    ASSERT_GT(values->byteSize(), elements_in_state * 8);

    auto offsets = ColumnUInt64::create();
    offsets->insert(0);
    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), /*size_=*/1);
    ColumnPtr constant = ColumnConst::create(std::move(sparse), rows);

    /// As in `ConstantSparseStateSamplesRepeatedPayload`, the wire ground truth is the equivalent
    /// materialized column: `NativeWriter` cannot serialize a constant over a sparse column.
    auto source_state = createGroupArrayStringColumn(/*rows=*/1, /*filled_state_row=*/0, elements_in_state, function);
    auto materialized = ColumnAggregateFunction::create(function);
    for (size_t row = 0; row < rows; ++row)
        materialized->insertFrom(*source_state, 0);

    const size_t cache_key = 0x111985 + 13;
    const auto exact_compressed_bytes = compressedColumnSize({std::move(materialized), state_type, "sparse_state_arena"});
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, state_type, "sparse_state_arena"});
        updater.recordOutputChunk(Chunk(Columns{std::move(constant)}, rows), header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

/// The implicit default at `ColumnSparse::values[0]` is one outer `Array` row, but that row contains no
/// nested aggregate states. The state leaf in the following real row must therefore not be skipped while
/// sampling. This is the same row-expanding layout `Map` uses for its nested key/value arrays.
TEST(RuntimeDataflowStatisticsStateSampling, SparseArrayStateDoesNotSkipFirstNestedValue)
{
    tryRegisterAggregateFunctions();

    constexpr size_t elements_in_state = 100000;

    AggregateFunctionPtr function;
    auto source_state = createSkewedGroupArrayColumn(/*rows=*/1, /*giant_state_row=*/0, elements_in_state, function);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});
    const auto array_type = std::make_shared<DataTypeArray>(state_type);

    auto nested_states = ColumnAggregateFunction::create(function);
    nested_states->insertFrom(*source_state, 0);
    auto array_offsets = ColumnArray::ColumnOffsets::create();
    array_offsets->insert(0); /// The sparse default array row has no nested elements.
    array_offsets->insert(1);
    auto values = ColumnArray::create(std::move(nested_states), std::move(array_offsets));

    auto sparse_offsets = ColumnUInt64::create();
    sparse_offsets->insert(0);
    auto sparse = ColumnSparse::create(std::move(values), std::move(sparse_offsets), /*size_=*/1);

    const auto exact_compressed_bytes
        = compressedColumnSize({sparse->convertToFullColumnIfSparse(), array_type, "sparse_array_state"});
    ASSERT_GT(exact_compressed_bytes, elements_in_state * sizeof(UInt64) / 2);

    const size_t cache_key = 0x111985 + 11;
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, /*total_rows_to_read_=*/1);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, array_type, "sparse_array_state"});
        updater.recordOutputChunk(Chunk(Columns{std::move(sparse)}, /*num_rows=*/1), header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

TEST(RuntimeDataflowStatisticsStateSampling, SparseMapStateDoesNotSkipFirstNestedValue)
{
    tryRegisterAggregateFunctions();

    constexpr size_t elements_in_state = 100000;

    AggregateFunctionPtr function;
    auto source_state = createSkewedGroupArrayColumn(/*rows=*/1, /*giant_state_row=*/0, elements_in_state, function);
    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), state_type);

    auto keys = ColumnString::create();
    keys->insertData("key", 3);
    auto nested_states = ColumnAggregateFunction::create(function);
    nested_states->insertFrom(*source_state, 0);
    Columns tuple_elements;
    tuple_elements.emplace_back(std::move(keys));
    tuple_elements.emplace_back(std::move(nested_states));
    auto array_offsets = ColumnArray::ColumnOffsets::create();
    array_offsets->insert(0); /// The sparse default map row has no nested entries.
    array_offsets->insert(1);
    auto values = ColumnMap::create(ColumnArray::create(ColumnTuple::create(std::move(tuple_elements)), std::move(array_offsets)));

    auto sparse_offsets = ColumnUInt64::create();
    sparse_offsets->insert(0);
    auto sparse = ColumnSparse::create(std::move(values), std::move(sparse_offsets), /*size_=*/1);

    const auto exact_compressed_bytes
        = compressedColumnSize({sparse->convertToFullColumnIfSparse(), map_type, "sparse_map_state"});
    ASSERT_GT(exact_compressed_bytes, elements_in_state * sizeof(UInt64) / 2);

    const size_t cache_key = 0x111985 + 12;
    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, /*total_rows_to_read_=*/1);
        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, map_type, "sparse_map_state"});
        updater.recordOutputChunk(Chunk(Columns{std::move(sparse)}, /*num_rows=*/1), header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

/// A state leaf can also sit inside a `ColumnDynamic` - `Dynamic` accepts `AggregateFunction` values, so
/// e.g. `if(cond, CAST(groupArrayState(x), 'Dynamic'), CAST(s, 'Dynamic'))` emits a `ColumnDynamic` whose
/// nested `ColumnVariant` holds a state leaf next to a plain string alternative. The walk that samples the
/// non-state payload of a state-bearing carrier must recurse through the dynamic column's variant like it
/// does through a `ColumnVariant` itself: falling back to "count the carrier's own payload as
/// incompressible" applies a compression ratio of 1 to the discriminators, offsets and the string
/// alternative, so a compressible alternative that dominates the column uncompressed overstates
/// `output_bytes` by its real compression ratio.
TEST(RuntimeDataflowStatisticsStateSampling, DynamicWithStateAlternativeSamplesNonStatePayloadCompression)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    constexpr size_t state_rows = 256;
    constexpr size_t giant_state_row = 255;
    constexpr size_t elements_in_giant_state = 50000;
    /// The string alternative's rows dwarf the states uncompressed and vanish compressed.
    constexpr size_t string_size = 10240;

    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(state_rows, giant_state_row, elements_in_giant_state, function, states_arena.get());
    column->addArena(states_arena);

    const auto exact = column->sampledStateSizes(state_rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    auto string_column = ColumnString::create();
    const std::string value(string_size, 'a');
    for (size_t row = 0; row < rows - state_rows; ++row)
        string_column->insertData(value.data(), value.size());
    /// Uncompressed the string alternative dominates the states by far more than the 2x margin asserted
    /// below, so an estimate that counts it as incompressible lands several-fold above the upper bound.
    ASSERT_GT(string_column->byteSize(), exact.compressed_bytes * 4);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    /// The dynamic column's variant always includes the shared variant. `DataTypeVariant` sorts the
    /// alternatives by name, so the global discriminators are 0 = the state ("AggregateFunction(...)"),
    /// 1 = "SharedVariant" (empty here), 2 = "String".
    const auto variant_type = std::make_shared<DataTypeVariant>(
        DataTypes{state_type, ColumnDynamic::getSharedVariantDataType(), std::make_shared<DataTypeString>()});
    ASSERT_EQ(variant_type->getVariants()[0]->getName(), state_type->getName());

    /// The first `state_rows` rows hold the states, the rest the strings.
    auto discriminators = ColumnVariant::ColumnDiscriminators::create();
    auto offsets = ColumnVariant::ColumnOffsets::create();
    for (size_t row = 0; row < rows; ++row)
    {
        discriminators->insertValue(row < state_rows ? 0 : 2);
        offsets->insertValue(row < state_rows ? row : row - state_rows);
    }

    const size_t cache_key = 0x111985 + 5;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, std::make_shared<DataTypeDynamic>(), "dynamic_state_or_string"});

        Columns variant_alternatives;
        variant_alternatives.emplace_back(std::move(column));
        variant_alternatives.emplace_back(ColumnDynamic::getSharedVariantDataType()->createColumn());
        variant_alternatives.emplace_back(std::move(string_column));
        auto variant_column = ColumnVariant::create(std::move(discriminators), std::move(offsets), variant_alternatives);
        Chunk chunk(
            Columns{ColumnDynamic::create(
                variant_column->assumeMutable(), variant_type, /*max_dynamic_types_=*/2, /*global_max_dynamic_types_=*/2)},
            rows);
        updater.recordOutputChunk(chunk, header);
    }

    /// The string alternative compresses to almost nothing, so the whole column's compressed size is the
    /// states'.
    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}

/// A state-bearing carrier can also be a constant - e.g. a scalar subquery returning a `-State` value, which
/// reaches the output as a `ColumnConst` around the state (or around a tuple holding one). `ColumnConst`
/// stores its single row once, but the data type cannot serialize constants, so `NativeWriter::writeData`
/// materializes them and the wire carries that row once per row of the block. Both the uncompressed figure
/// and the compression sample of everything below such a carrier must therefore count the whole block, not
/// the one stored row, or the statistic under-measures the output by the block's row count.
TEST(RuntimeDataflowStatisticsStateSampling, ConstantCarrierCountsEveryRowOfTheBlock)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 512;
    constexpr size_t elements_in_giant_state = 100000;
    constexpr size_t string_size = 10240;

    /// One row: a giant incompressible state in a foreign arena, and a large constant sibling string.
    auto states_arena = std::make_shared<Arena>();
    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(
        /*rows=*/1, /*giant_state_row=*/0, elements_in_giant_state, function, states_arena.get());
    column->addArena(states_arena);

    const auto exact = column->sampledStateSizes(1);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.compressed_bytes, elements_in_giant_state * sizeof(UInt64) / 2);

    auto string_column = ColumnString::create();
    const std::string value(string_size, 'a');
    string_column->insertData(value.data(), value.size());

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    const size_t cache_key = 0x111985 + 3;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{
            nullptr,
            std::make_shared<DataTypeTuple>(DataTypes{state_type, std::make_shared<DataTypeString>()}),
            "constant_wrapped_state"});

        Columns tuple_elements;
        tuple_elements.emplace_back(std::move(column));
        tuple_elements.emplace_back(std::move(string_column));
        Chunk chunk(Columns{ColumnConst::create(ColumnTuple::create(std::move(tuple_elements)), rows)}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    /// The state repeats `rows` times on the wire and does not compress; the constant string does, so the
    /// whole column's compressed size is the repeated states'. Sizing the carrier from its single stored
    /// row instead lands a factor of `rows` below the lower bound.
    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, rows * exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, rows * exact.compressed_bytes * 2);
}

/// The repetitions of a constant state must also be *compressed* like the materialized column the wire
/// carries, not scaled from one copy: `NativeWriter::writeData` sends `rows` identical serialized states in
/// one compressed stream, which compress to almost nothing while a copy fits the codec's match window.
/// Multiplying the one-copy `sample_bytes` and `compressed_bytes` by the row count keeps the one-copy
/// ratio, so a constant state that is incompressible on its own - or tiny, like a repeated `countState()` -
/// would overstate `output_bytes` by the repeated payload's real compression ratio and could disable
/// automatic parallel replicas on constant `-State` outputs.
TEST(RuntimeDataflowStatisticsStateSampling, ConstantStateCompressesAcrossRepetitions)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 48;
    /// One copy of the state is ~16 KiB of distinct values: incompressible on its own, well inside LZ4's
    /// 64 KiB match window when repeated back to back.
    constexpr size_t elements_in_state = 2000;

    AggregateFunctionPtr function;
    auto column = createSkewedGroupArrayColumn(/*rows=*/1, /*giant_state_row=*/0, elements_in_state, function);

    /// The ground truth: the materialized column the wire carries, i.e. the same state `rows` times,
    /// measured exactly by the one-copy primitive (the limit covers every state, so nothing is scaled).
    auto materialized = ColumnAggregateFunction::create(function);
    for (size_t row = 0; row < rows; ++row)
        materialized->insertFrom(*column, 0);
    const auto exact = materialized->sampledStateSizes(rows);
    ASSERT_EQ(exact.bytes, exact.sample_bytes);
    ASSERT_GT(exact.sample_bytes, rows * elements_in_state * sizeof(UInt64) / 2);
    /// The identical copies compress: an estimate that keeps the one-copy ratio of ~1 lands close to
    /// `sample_bytes`, far more than the 2x margin asserted below allows.
    ASSERT_LT(exact.compressed_bytes * 8, exact.sample_bytes);

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});

    const size_t cache_key = 0x111985 + 6;

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, state_type, "constant_state"});

        Chunk chunk(Columns{ColumnConst::create(std::move(column), rows)}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact.compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact.compressed_bytes * 2);
}

/// The same holds for the non-state payload *below* the stored row of a constant state-bearing carrier - a
/// constant `Array(Tuple(groupArrayState(x), String))`, say, whose nested strings have more than one row and
/// therefore cannot be put back into a constant of the block's row count. `NativeWriter::writeData`
/// materializes the constant all the same, so the wire carries the whole nested payload once per row of the
/// block: its copies compress against each other even when one copy is incompressible on its own, and
/// scaling one copy's `sample_bytes` and `compressed_bytes` by the row count keeps the one-copy ratio,
/// overstating `output_bytes` by the repeated payload's real compression ratio.
static void checkConstantNestedPayloadCompression(size_t rows, size_t cache_key)
{
    tryRegisterAggregateFunctions();

    /// The stored row is an array of this many `(state, string)` tuples. The states stay empty - one varint
    /// each on the wire - so the strings dominate the carrier.
    constexpr size_t elements_in_array = 4;
    /// Distinct bytes, so one copy of the strings is incompressible; all four together are ~16 KiB, well
    /// inside LZ4's 64 KiB match window when the copies follow each other.
    constexpr size_t string_size = 4096;

    AggregateFunctionPtr function;
    auto state_column = createSkewedGroupArrayColumn(
        /*rows=*/elements_in_array,
        /*giant_state_row=*/elements_in_array,
        /*elements_in_giant_state=*/0,
        function);

    auto string_column = ColumnString::create();
    for (size_t element = 0; element < elements_in_array; ++element)
    {
        std::string value(string_size, ' ');
        for (size_t i = 0; i < string_size; ++i)
            value[i] = static_cast<char>((i * 0x9E3779B9ULL + element * 0x85EBCA6BULL) >> 13);
        string_column->insertData(value.data(), value.size());
    }

    const auto state_type = std::make_shared<DataTypeAggregateFunction>(function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{});
    const auto element_type = std::make_shared<DataTypeTuple>(DataTypes{state_type, std::make_shared<DataTypeString>()});
    const auto array_type = std::make_shared<DataTypeArray>(element_type);

    Columns tuple_elements;
    tuple_elements.emplace_back(std::move(state_column));
    tuple_elements.emplace_back(std::move(string_column));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(elements_in_array);
    const ColumnPtr array_column = ColumnArray::create(ColumnTuple::create(std::move(tuple_elements)), std::move(offsets));

    /// The ground truth: the materialized column the wire carries, i.e. the same array `rows` times.
    auto materialized = array_column->cloneEmpty();
    for (size_t row = 0; row < rows; ++row)
        materialized->insertFrom(*array_column, 0);
    const size_t exact_compressed_bytes = compressedColumnSize({std::move(materialized), array_type, "materialized"});
    /// The identical copies compress: an estimate that keeps the one-copy ratio of ~1 lands at the whole
    /// uncompressed payload, far above the 2x margin asserted below.
    ASSERT_LT(exact_compressed_bytes * 8, rows * elements_in_array * string_size);

    {
        RuntimeDataflowStatisticsCacheUpdater updater(cache_key, rows);

        Block header;
        header.insert(ColumnWithTypeAndName{nullptr, array_type, "constant_array_of_states"});

        Chunk chunk(Columns{ColumnConst::create(array_column, rows)}, rows);
        updater.recordOutputChunk(chunk, header);
    }

    const auto stats = getRuntimeDataflowStatisticsCache().getStats(cache_key);
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->output_bytes, exact_compressed_bytes / 2);
    EXPECT_LE(stats->output_bytes, exact_compressed_bytes * 2);
}

TEST(RuntimeDataflowStatisticsStateSampling, ConstantNestedPayloadCompressesAcrossRepetitions)
{
    checkConstantNestedPayloadCompression(/*rows=*/32, /*cache_key=*/0x111985 + 7);
}

/// With enough repetitions the copies outgrow the measurement budget, which is also the size of one
/// compressed block. Every further block of the wire repeats the measured block's shape - the compressed
/// stream cannot match across a block boundary - so the measured figure scales with the uncompressed size.
TEST(RuntimeDataflowStatisticsStateSampling, ManyConstantNestedRepetitionsScaleByCompressedBlock)
{
    /// 16 KiB of strings per copy, so the copies pass the 1 MiB measurement budget well before the last one.
    checkConstantNestedPayloadCompression(/*rows=*/256, /*cache_key=*/0x111985 + 12);
}
