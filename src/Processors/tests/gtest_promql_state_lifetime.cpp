#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionForEach.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <Processors/Transforms/PromQLPartialGroupMergeTransform.h>
#include <Processors/Transforms/PromQLRangeSumByTransform.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/memory.h>
#include <Common/tests/gtest_global_register.h>
#include <base/arithmeticOverflow.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

/// Test-only scalar snapshots and non-owning Arena observers. No chunk, aggregate state, Arena owner,
/// or mutable reference escapes this class.
class PromQLStateLifetimeTestAccess
{
public:
    struct StateColumnCensus
    {
        bool has_chunk = false;
        bool has_state_column = false;
        size_t state_count = 0;
        size_t dynamic_array_size = 0;
        size_t nested_stride_bytes = 0;
        size_t foreach_cells_span_bytes = 0;
        size_t reachable_span_bytes = 0;
        size_t column_byte_size = 0;
        size_t column_allocated_bytes = 0;
        size_t row_pointer_bytes = 0;
        size_t pointer_capacity_bytes = 0;
        std::vector<std::uintptr_t> state_addresses;
    };

    struct TransformCensus
    {
        size_t group_count = 0;
        std::vector<std::uintptr_t> group_state_addresses;
        size_t group_foreach_cells_span_bytes = 0;
        size_t group_reachable_span_bytes = 0;
        bool group_arena_present = false;
        size_t group_arena_used_bytes = 0;
        size_t group_arena_allocated_bytes = 0;
        bool has_current_series = false;
        bool rate_state_created = false;
        size_t current_series_samples = 0;
        StateColumnCensus current_input;
        StateColumnCensus pending_output;
    };

    static TransformCensus inspect(const PromQLRangeSumByTransform & transform)
    {
        TransformCensus census;
        census.group_count = transform.group_states.size();
        inspectGroupStates(transform.group_states, transform.sum_function, census);
        census.has_current_series = transform.has_current_series;
        census.rate_state_created = transform.rate_state_created;
        census.current_series_samples = transform.current_series_samples;
        census.group_arena_present = static_cast<bool>(transform.group_arena);
        if (transform.group_arena)
        {
            census.group_arena_used_bytes = transform.group_arena->usedBytes();
            census.group_arena_allocated_bytes = transform.group_arena->allocatedBytes();
        }
        census.current_input = inspectOwnedStateColumn(transform.current_input_chunk, transform.sum_function);
        census.pending_output = inspectOwnedStateColumn(transform.current_output_chunk, transform.sum_function);
        return census;
    }

    static TransformCensus inspect(const PromQLPartialGroupMergeTransform & transform)
    {
        TransformCensus census;
        census.group_count = transform.group_states.size();
        inspectGroupStates(transform.group_states, transform.sum_function, census);
        census.group_arena_present = static_cast<bool>(transform.group_arena);
        if (transform.group_arena)
        {
            census.group_arena_used_bytes = transform.group_arena->usedBytes();
            census.group_arena_allocated_bytes = transform.group_arena->allocatedBytes();
        }
        census.current_input = inspectOwnedStateColumn(transform.current_input_chunk, transform.sum_function);
        census.pending_output = inspectOwnedStateColumn(transform.current_output_chunk, transform.sum_function);
        return census;
    }

    static std::weak_ptr<Arena> observeGroupArena(const PromQLRangeSumByTransform & transform)
    {
        return transform.group_arena;
    }

    /// Complete the last rate series only to snapshot all original group states before generate.
    static void finishCurrentSeries(PromQLRangeSumByTransform & transform)
    {
        transform.finishSeries();
    }

private:
    static size_t nestedStride(const AggregateFunctionPtr & function)
    {
        const auto nested_function = function->getNestedFunction();
        if (!nested_function)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL lifetime census expected an aggregate combinator");
        return ::Memory::alignUp(nested_function->sizeOfData(), nested_function->alignOfData());
    }

    static void addStateSpan(const AggregateFunctionPtr & function, size_t nested_stride_bytes, ConstAggregateDataPtr state_ptr, StateColumnCensus & census)
    {
        if (!state_ptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL lifetime census found a null aggregate state");

        const auto & state = *reinterpret_cast<const AggregateFunctionForEachData *>(state_ptr);
        size_t cells_span_bytes = 0;
        if (common::mulOverflow(state.dynamic_array_size, nested_stride_bytes, cells_span_bytes))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL lifetime census ForEach span overflows size_t");

        size_t state_span_bytes = function->sizeOfData();
        if (common::addOverflow(state_span_bytes, cells_span_bytes, state_span_bytes)
            || common::addOverflow(census.foreach_cells_span_bytes, cells_span_bytes, census.foreach_cells_span_bytes)
            || common::addOverflow(census.reachable_span_bytes, state_span_bytes, census.reachable_span_bytes))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL lifetime census total state span overflows size_t");

        census.dynamic_array_size = std::max(census.dynamic_array_size, state.dynamic_array_size);
    }

    template <typename GroupStates>
    static void inspectGroupStates(const GroupStates & states, const AggregateFunctionPtr & function, TransformCensus & census)
    {
        census.group_state_addresses.reserve(states.size());
        const size_t stride_bytes = nestedStride(function);
        StateColumnCensus state_census;
        state_census.nested_stride_bytes = stride_bytes;
        for (const auto & entry : states)
        {
            const auto state_ptr = entry.getMapped();
            census.group_state_addresses.push_back(reinterpret_cast<std::uintptr_t>(state_ptr));
            addStateSpan(function, stride_bytes, state_ptr, state_census);
        }
        std::sort(census.group_state_addresses.begin(), census.group_state_addresses.end());
        census.group_foreach_cells_span_bytes = state_census.foreach_cells_span_bytes;
        census.group_reachable_span_bytes = state_census.reachable_span_bytes;
    }

    /// Call sites inspect live test-owned copies or states transferred with their shared Arena,
    /// including chunks moved through the real ports.
    static StateColumnCensus inspectOwnedStateColumn(const Chunk & chunk, const AggregateFunctionPtr & function)
    {
        StateColumnCensus census;
        census.has_chunk = static_cast<bool>(chunk);
        if (!chunk || chunk.getColumns().size() < 2)
            return census;

        const auto * column = typeid_cast<const ColumnAggregateFunction *>(chunk.getColumns()[1].get());
        if (!column)
            return census;

        census.has_state_column = true;
        census.state_count = column->size();
        census.nested_stride_bytes = nestedStride(function);
        census.state_addresses.reserve(census.state_count);
        for (auto * const state_ptr : column->getData())
        {
            census.state_addresses.push_back(reinterpret_cast<std::uintptr_t>(state_ptr));
            addStateSpan(function, census.nested_stride_bytes, state_ptr, census);
        }

        /// Preserve raw counters. Owned-copy tests assert subtraction preconditions; foreign Arenas
        /// are absent from byteSize/allocatedBytes, so these counters are not overall state memory.
        if (common::mulOverflow(census.state_count, sizeof(AggregateDataPtr), census.row_pointer_bytes))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL lifetime census row pointer size overflows size_t");
        census.column_byte_size = column->byteSize();
        census.column_allocated_bytes = column->allocatedBytes();
        census.pointer_capacity_bytes = column->getData().allocated_bytes();
        return census;
    }
};

namespace
{

using Collector = ContextTimeSeriesTagsCollector;
using Tags = Collector::TagNamesAndValues;
using TagsPtr = Collector::TagNamesAndValuesPtr;

class LifetimeChunksSource final : public ISource
{
public:
    LifetimeChunksSource(SharedHeader header_, Chunks chunks_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , chunks(std::move(chunks_))
    {
    }

    String getName() const override
    {
        return "PromQLStateLifetimeChunksSource";
    }

protected:
    Chunk generate() override
    {
        if (position == chunks.size())
            return {};
        return std::move(chunks[position++]);
    }

private:
    Chunks chunks;
    size_t position = 0;
};

class LifetimeCollectingSink final : public ISink
{
public:
    explicit LifetimeCollectingSink(SharedHeader header_)
        : ISink(std::move(header_))
    {
    }

    String getName() const override
    {
        return "PromQLStateLifetimeCollectingSink";
    }

    const std::vector<Chunk> & getChunks() const
    {
        return chunks;
    }

    Chunk takeChunk(size_t index)
    {
        return std::move(chunks.at(index));
    }

protected:
    void consume(Chunk chunk) override
    {
        chunks.emplace_back(std::move(chunk));
    }

private:
    std::vector<Chunk> chunks;
};

DataTypePtr makeSamplesType()
{
    return std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()}));
}

SharedHeader makeInputHeader(const DataTypePtr & samples_type)
{
    auto id_type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(id_type->createColumn(), id_type, TimeSeriesColumnNames::ID),
        ColumnWithTypeAndName(samples_type->createColumn(), samples_type, TimeSeriesColumnNames::TimeSeries),
    });
}

std::shared_ptr<Collector> makeCollector(size_t group_count = 4)
{
    auto collector = std::make_shared<Collector>();
    auto ids = ColumnUInt64::create();
    VectorWithMemoryTracking<TagsPtr> tags;
    for (UInt64 id = 1; id <= group_count; ++id)
    {
        ids->insertValue(id);
        auto row_tags = std::make_shared<Tags>();
        row_tags->emplace_back("__name__", "requests_total");
        row_tags->emplace_back("series", "series" + std::to_string(id));
        tags.emplace_back(std::move(row_tags));
    }
    collector->storeTags(std::move(ids), tags);
    return collector;
}

AggregateFunctionPtr makeRateFunction(const DataTypePtr & samples_type, UInt32 grid_points = 256)
{
    const UInt32 last_evaluation = 10 + grid_points - 1;
    Array parameters{UInt64{10}, UInt64{last_evaluation}, UInt64{1}, UInt64{5}};
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{samples_type}, parameters, UInt32{10}, last_evaluation, Int32{1}, Int32{5}, 0, 0);
}

AggregateFunctionPtr makeSumFunction(const AggregateFunctionPtr & rate_function, const char * name)
{
    tryRegisterAggregateFunctions();
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(
        name, NullsAction::EMPTY, DataTypes{rate_function->getResultType()}, {}, properties);
}

Chunk makeInputChunk(const DataTypePtr & samples_type, size_t group_count = 4, UInt32 grid_points = 256)
{
    auto ids = ColumnUInt64::create();
    auto samples_column = samples_type->createColumn();
    const UInt32 last_sample = 10 + grid_points - 1 + 5;
    for (UInt64 id = 1; id <= group_count; ++id)
    {
        ids->insertValue(id);
        Array values;
        values.reserve(last_sample + 1);
        for (UInt32 timestamp = 0; timestamp <= last_sample; ++timestamp)
            values.emplace_back(Tuple{timestamp, static_cast<Float64>(id * timestamp)});
        samples_column->insert(values);
    }
    return Chunk(Columns{std::move(ids), std::move(samples_column)}, group_count);
}

ColumnPtr makeExpectedConstantRate(const DataTypePtr & result_type, Float64 slope, size_t grid_points = 256)
{
    auto result = result_type->createColumn();
    Array expected_values;
    expected_values.reserve(grid_points);
    for (size_t point = 0; point < grid_points; ++point)
        expected_values.emplace_back(slope);
    result->insert(expected_values);
    return result;
}

void consumeLaneInput(LifetimeChunksSource & source, PromQLRangeSumByTransform & lane, LifetimeCollectingSink & sink)
{
    ASSERT_EQ(sink.prepare(), IProcessor::Status::Ready);
    sink.work();
    ASSERT_EQ(sink.prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(lane.prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source.prepare(), IProcessor::Status::Ready);
    source.work();
    ASSERT_EQ(source.prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(lane.getInputPort().hasData());
    ASSERT_EQ(lane.prepare(), IProcessor::Status::Ready);
    lane.work();
}

void generateFirstLaneOutput(LifetimeChunksSource & source, PromQLRangeSumByTransform & lane)
{
    ASSERT_EQ(lane.prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source.prepare(), IProcessor::Status::Ready);
    source.work();
    ASSERT_EQ(source.prepare(), IProcessor::Status::Finished);
    ASSERT_EQ(lane.prepare(), IProcessor::Status::Ready);
    lane.work();
}

void assertRetainedConstantRates(
    const ColumnAggregateFunction & states,
    const std::vector<UInt64> & groups,
    const AggregateFunctionPtr & sum_function,
    const DataTypePtr & rate_result_type,
    const Collector & collector,
    size_t group_count,
    size_t grid_points)
{
    ASSERT_FALSE(groups.empty());
    ASSERT_EQ(states.size(), groups.size());
    auto values = sum_function->getResultType()->createColumn();
    for (auto * const state_ptr : states.getData())
        sum_function->insertResultInto(state_ptr, *values, nullptr);
    ASSERT_EQ(values->size(), groups.size());

    std::set<UInt64> unique_groups;
    std::set<UInt64> observed_slopes;
    for (size_t row = 0; row < groups.size(); ++row)
    {
        ASSERT_TRUE(unique_groups.insert(groups[row]).second);
        const auto series_tag = collector.extractTag(groups[row], "series");
        bool matches_known_tag = false;
        for (UInt64 slope = 1; slope <= group_count; ++slope)
        {
            if (series_tag == "series" + std::to_string(slope))
            {
                matches_known_tag = true;
                const auto expected_rate = makeExpectedConstantRate(rate_result_type, static_cast<Float64>(slope), grid_points);
                ASSERT_EQ(values->compareAt(row, 0, *expected_rate, 0), 0);
                observed_slopes.insert(slope);
            }
        }
        ASSERT_TRUE(matches_known_tag);
    }
    ASSERT_EQ(observed_slopes.size(), groups.size());
}

Chunk makeOwnedPartialStatesChunk(const AggregateFunctionPtr & sum_function)
{
    auto group_column = ColumnUInt64::create();
    auto states_column = ColumnAggregateFunction::create(sum_function);
    for (UInt64 group : {11, 22})
    {
        group_column->insertValue(group);
        auto values_column = sum_function->getArgumentTypes().front()->createColumn();
        values_column->insert(Array{1.0, 2.0});
        const IColumn * arguments[] = {values_column.get()};
        Arena source_arena;
        AlignedBuffer place(sum_function->sizeOfData(), sum_function->alignOfData());
        sum_function->create(place.data());
        SCOPE_EXIT({ sum_function->destroy(place.data()); });
        sum_function->add(place.data(), arguments, 0, &source_arena);
        /// This creates an owned copy whose arena lifetime is independent of the stack state above.
        states_column->insertFrom(place.data());
    }
    return Chunk(Columns{std::move(group_column), std::move(states_column)}, 2);
}

void recordNumber(const std::string & name, size_t value)
{
    ::testing::Test::RecordProperty(name, std::to_string(value));
}

bool recordOwnedColumnAccounting(const std::string & prefix, const PromQLStateLifetimeTestAccess::StateColumnCensus & census)
{
    if (!census.has_state_column)
        return false;

    const bool pointer_rows_fit = census.column_byte_size >= census.row_pointer_bytes;
    const bool pointer_capacity_fits = census.column_allocated_bytes >= census.pointer_capacity_bytes;
    EXPECT_TRUE(pointer_rows_fit);
    EXPECT_TRUE(pointer_capacity_fits);
    if (!pointer_rows_fit || !pointer_capacity_fits)
        return false;

    const size_t own_arena_used_bytes = census.column_byte_size - census.row_pointer_bytes;
    const size_t own_arena_allocated_bytes = census.column_allocated_bytes - census.pointer_capacity_bytes;
    const bool allocated_covers_used = own_arena_allocated_bytes >= own_arena_used_bytes;
    const bool used_covers_reachable = own_arena_used_bytes >= census.reachable_span_bytes;
    EXPECT_TRUE(allocated_covers_used);
    EXPECT_TRUE(used_covers_reachable);
    if (!allocated_covers_used || !used_covers_reachable)
        return false;

    recordNumber(prefix + "_state_count", census.state_count);
    recordNumber(prefix + "_dynamic_array_size", census.dynamic_array_size);
    recordNumber(prefix + "_nested_stride_bytes", census.nested_stride_bytes);
    recordNumber(prefix + "_foreach_cells_span_bytes", census.foreach_cells_span_bytes);
    recordNumber(prefix + "_reachable_span_bytes", census.reachable_span_bytes);
    recordNumber(prefix + "_own_arena_used_bytes", own_arena_used_bytes);
    recordNumber(prefix + "_own_arena_allocated_bytes", own_arena_allocated_bytes);
    recordNumber(prefix + "_retained_requested_bytes", own_arena_used_bytes - census.reachable_span_bytes);
    recordNumber(prefix + "_capacity_slack_bytes", own_arena_allocated_bytes - own_arena_used_bytes);
    return true;
}

bool recordGroupArenaAccounting(const std::string & prefix, const PromQLStateLifetimeTestAccess::TransformCensus & census)
{
    if (!census.group_arena_present)
        return false;

    const bool used_covers_reachable = census.group_arena_used_bytes >= census.group_reachable_span_bytes;
    const bool allocated_covers_used = census.group_arena_allocated_bytes >= census.group_arena_used_bytes;
    EXPECT_TRUE(used_covers_reachable);
    EXPECT_TRUE(allocated_covers_used);
    if (!used_covers_reachable || !allocated_covers_used)
        return false;

    recordNumber(prefix + "_group_count", census.group_count);
    recordNumber(prefix + "_foreach_cells_span_bytes", census.group_foreach_cells_span_bytes);
    recordNumber(prefix + "_reachable_span_bytes", census.group_reachable_span_bytes);
    recordNumber(prefix + "_arena_used_bytes", census.group_arena_used_bytes);
    recordNumber(prefix + "_arena_allocated_bytes", census.group_arena_allocated_bytes);
    recordNumber(prefix + "_retained_requested_bytes", census.group_arena_used_bytes - census.group_reachable_span_bytes);
    recordNumber(prefix + "_capacity_slack_bytes", census.group_arena_allocated_bytes - census.group_arena_used_bytes);
    return true;
}

class PromQLStateLifetimeTest : public ::testing::TestWithParam<const char *>
{
};

}

TEST_P(PromQLStateLifetimeTest, KeepsOriginalQueuedPendingAndDestinationStatesDistinct)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t grid_points = 256;

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, 512, 4, 1, nullptr, true);
    auto merger = std::make_shared<PromQLPartialGroupMergeTransform>(
        lane->getOutputPort().getSharedHeader(), sum_function, 4, 4);
    auto sink = std::make_shared<LifetimeCollectingSink>(merger->getOutputPort().getSharedHeader());

    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), merger->getInputPort());
    connect(merger->getOutputPort(), sink->getPort());

    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(merger->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(lane->prepare(), IProcessor::Status::NeedData);

    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    source->work();
    ASSERT_EQ(source->prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(lane->getInputPort().hasData());
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    lane->work();

    ASSERT_EQ(lane->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    source->work();
    ASSERT_EQ(source->prepare(), IProcessor::Status::Finished);
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    lane->work();

    const auto before_queue = PromQLStateLifetimeTestAccess::inspect(*lane);
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_FALSE(original_group_arena.expired());
    ASSERT_EQ(before_queue.group_count, 4);
    ASSERT_EQ(before_queue.group_state_addresses.size(), 4);
    ASSERT_TRUE(recordGroupArenaAccounting("original_groups", before_queue));
    ASSERT_TRUE(before_queue.pending_output.has_state_column);
    ASSERT_EQ(before_queue.pending_output.state_count, 1);
    ASSERT_EQ(before_queue.pending_output.dynamic_array_size, grid_points);
    ASSERT_TRUE(recordOwnedColumnAccounting("first_queued_copy", before_queue.pending_output));
    const auto first_queued_address = before_queue.pending_output.state_addresses.front();
    const void * connection_id = lane->getOutputPort().getConnectionId();

    /// The pending first chunk is pushed through the real connection. prepare() then returns Ready
    /// from finished_input without rechecking the now-full output; work() creates the second pending chunk.
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    ASSERT_TRUE(lane->getOutputPort().hasData());
    ASSERT_TRUE(merger->getInputPort().hasData());
    ASSERT_EQ(connection_id, merger->getInputPort().getConnectionId());
    lane->work();

    const auto with_pending = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(with_pending.pending_output.state_count, 1);
    ASSERT_EQ(with_pending.pending_output.dynamic_array_size, grid_points);
    ASSERT_TRUE(recordOwnedColumnAccounting("second_pending_copy", with_pending.pending_output));
    const auto second_pending_address = with_pending.pending_output.state_addresses.front();
    ASSERT_NE(first_queued_address, second_pending_address);

    ASSERT_EQ(merger->prepare(), IProcessor::Status::Ready);
    const auto while_input_held = PromQLStateLifetimeTestAccess::inspect(*merger);
    ASSERT_TRUE(while_input_held.current_input.has_state_column);
    ASSERT_EQ(while_input_held.current_input.state_count, 1);
    ASSERT_EQ(while_input_held.current_input.dynamic_array_size, grid_points);
    ASSERT_EQ(while_input_held.current_input.state_addresses.front(), first_queued_address);
    ASSERT_TRUE(recordOwnedColumnAccounting("held_input_copy", while_input_held.current_input));
    ASSERT_EQ(while_input_held.group_count, 0);

    std::set<std::uintptr_t> live_addresses(
        before_queue.group_state_addresses.begin(), before_queue.group_state_addresses.end());
    live_addresses.insert(first_queued_address);
    live_addresses.insert(second_pending_address);
    ASSERT_EQ(live_addresses.size(), before_queue.group_count + 2);
    recordNumber("distinct_live_states_before_merge", live_addresses.size());
    recordNumber("unique_owned_copy_states_before_merge", live_addresses.size() - before_queue.group_count);

    merger->work();
    const auto after_consume = PromQLStateLifetimeTestAccess::inspect(*merger);
    ASSERT_EQ(after_consume.group_count, 1);
    ASSERT_FALSE(after_consume.current_input.has_state_column);
    ASSERT_TRUE(after_consume.group_arena_present);
    ASSERT_GT(after_consume.group_arena_used_bytes, 0);
    const auto destination_address = after_consume.group_state_addresses.front();
    ASSERT_EQ(PromQLStateLifetimeTestAccess::inspect(*lane).group_state_addresses, before_queue.group_state_addresses);
    ASSERT_NE(destination_address, first_queued_address);
    ASSERT_NE(destination_address, second_pending_address);
    for (const auto original_address : before_queue.group_state_addresses)
        ASSERT_NE(destination_address, original_address);
    std::set<std::uintptr_t> post_merge_live_addresses(
        before_queue.group_state_addresses.begin(), before_queue.group_state_addresses.end());
    post_merge_live_addresses.insert(second_pending_address);
    post_merge_live_addresses.insert(destination_address);
    ASSERT_EQ(post_merge_live_addresses.size(), before_queue.group_count + 2);
    recordNumber("distinct_live_states_after_merge", post_merge_live_addresses.size());
    recordNumber("original_group_states_retained_after_merge", before_queue.group_count);
    recordNumber("destination_group_states_after_merge", after_consume.group_count);

    recordNumber("grid_points", grid_points);
    recordNumber("input_samples_per_series", 271);
    recordNumber("evaluation_grid_start", 10);
    recordNumber("evaluation_grid_end", 265);
    recordNumber("evaluation_grid_step", 1);
    recordNumber("rate_window_seconds", 5);
    ASSERT_TRUE(recordGroupArenaAccounting("destination_after_consume", after_consume));
    ::testing::Test::RecordProperty("sum_function", GetParam());

    bool drained = false;
    bool lane_last_group_emitted = false;
    for (size_t iteration = 0; iteration < 128; ++iteration)
    {
        const auto sink_status = sink->prepare();
        if (sink_status == IProcessor::Status::Finished)
        {
            drained = true;
            break;
        }
        if (sink_status == IProcessor::Status::Ready)
            sink->work();

        if (merger->prepare() == IProcessor::Status::Ready)
            merger->work();
        if (lane->prepare() == IProcessor::Status::Ready)
        {
            const auto groups_before_work = PromQLStateLifetimeTestAccess::inspect(*lane).group_count;
            lane->work();
            const auto lane_state = PromQLStateLifetimeTestAccess::inspect(*lane);
            if (groups_before_work > 0 && lane_state.group_count == 0)
            {
                ASSERT_FALSE(lane_last_group_emitted);
                ASSERT_TRUE(lane_state.pending_output.has_state_column);
                ASSERT_EQ(lane_state.pending_output.state_count, 1);
                /// The last owned copy is still pending, but it must not retain the lane's Arena.
                ASSERT_TRUE(original_group_arena.expired());
                lane_last_group_emitted = true;
            }
        }
        if (source->prepare() == IProcessor::Status::Ready)
            source->work();
    }
    ASSERT_TRUE(drained);
    ASSERT_TRUE(lane_last_group_emitted);

    const auto lane_final = PromQLStateLifetimeTestAccess::inspect(*lane);
    const auto merger_final = PromQLStateLifetimeTestAccess::inspect(*merger);
    ASSERT_EQ(lane_final.group_count, 0);
    ASSERT_TRUE(lane_final.group_arena_present);
    ASSERT_EQ(lane_final.group_arena_used_bytes, 0);
    ASSERT_FALSE(lane_final.current_input.has_state_column);
    ASSERT_FALSE(lane_final.pending_output.has_state_column);
    ASSERT_EQ(merger_final.group_count, 0);
    ASSERT_TRUE(merger_final.group_arena_present);
    ASSERT_EQ(merger_final.group_arena_used_bytes, 0);
    ASSERT_FALSE(merger_final.current_input.has_state_column);
    ASSERT_FALSE(merger_final.pending_output.has_state_column);
    ASSERT_TRUE(recordGroupArenaAccounting("lane_final", lane_final));
    ASSERT_TRUE(recordGroupArenaAccounting("merger_final", merger_final));
    recordNumber("lane_final_group_count", lane_final.group_count);
    recordNumber("lane_fresh_arena_used_bytes", lane_final.group_arena_used_bytes);
    recordNumber("merger_final_group_count", merger_final.group_count);
    recordNumber("merger_fresh_arena_used_bytes", merger_final.group_arena_used_bytes);

    ASSERT_EQ(sink->getChunks().size(), 1);
    const auto & output = sink->getChunks().front();
    ASSERT_EQ(output.getNumRows(), 4);
    const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns()[0]);
    const auto & values = *output.getColumns()[1];
    std::vector<std::pair<UInt64, ColumnPtr>> expected_rates;
    for (UInt64 slope = 1; slope <= 4; ++slope)
        expected_rates.emplace_back(slope, makeExpectedConstantRate(rate_function->getResultType(), static_cast<Float64>(slope)));
    std::set<UInt64> unique_groups;
    std::set<UInt64> observed_slopes;
    for (size_t row = 0; row < output.getNumRows(); ++row)
    {
        unique_groups.insert(groups.getElement(row));
        bool matches_known_slope = false;
        for (const auto & [slope, expected_rate] : expected_rates)
        {
            if (values.compareAt(row, 0, *expected_rate, 0) == 0)
            {
                matches_known_slope = true;
                ASSERT_EQ(collector->extractTag(groups.getElement(row), "series"), "series" + std::to_string(slope));
                observed_slopes.insert(slope);
            }
        }
        EXPECT_TRUE(matches_known_slope);
    }
    EXPECT_EQ(unique_groups.size(), 4);
    EXPECT_EQ(observed_slopes.size(), 4);
}

TEST_P(PromQLStateLifetimeTest, TransfersSingleBlockStatesAndArenaToRetainedView)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;
    constexpr size_t grid_points = 256;

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, group_count, nullptr, true);
    auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), sink->getPort());

    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(lane->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    source->work();
    ASSERT_EQ(source->prepare(), IProcessor::Status::PortFull);
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    lane->work();

    const auto after_last_consume = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(after_last_consume.group_count, group_count - 1);
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_FALSE(original_group_arena.expired());

    /// The fourth rate series is still unfinished after consume. Complete it for this pre-generate
    /// snapshot; the subsequent generate still runs through the actual ports and finishes no series twice.
    PromQLStateLifetimeTestAccess::finishCurrentSeries(*lane);
    const auto originals = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(originals.group_count, group_count);
    ASSERT_EQ(originals.group_state_addresses.size(), group_count);
    ASSERT_TRUE(std::includes(
        originals.group_state_addresses.begin(), originals.group_state_addresses.end(),
        after_last_consume.group_state_addresses.begin(), after_last_consume.group_state_addresses.end()));
    ASSERT_TRUE(recordGroupArenaAccounting("single_block_original_groups", originals));

    ASSERT_EQ(lane->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    source->work();
    ASSERT_EQ(source->prepare(), IProcessor::Status::Finished);
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    lane->work();

    const auto after_generate = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(after_generate.group_count, 0);
    ASSERT_TRUE(after_generate.group_arena_present);
    ASSERT_EQ(after_generate.group_arena_used_bytes, 0);
    ASSERT_FALSE(after_generate.current_input.has_chunk);
    ASSERT_TRUE(after_generate.pending_output.has_state_column);
    ASSERT_EQ(after_generate.pending_output.state_count, group_count);
    ASSERT_EQ(after_generate.pending_output.dynamic_array_size, grid_points);
    auto transferred_addresses = after_generate.pending_output.state_addresses;
    std::sort(transferred_addresses.begin(), transferred_addresses.end());
    ASSERT_EQ(transferred_addresses, originals.group_state_addresses);
    /// A transferred foreign Arena is deliberately excluded from both column counters. Equality
    /// proves no ownArena allocation here, not a reduction in whole-query or retained-Arena memory.
    ASSERT_EQ(after_generate.pending_output.column_byte_size, after_generate.pending_output.row_pointer_bytes);
    ASSERT_EQ(after_generate.pending_output.column_allocated_bytes, after_generate.pending_output.pointer_capacity_bytes);
    const auto fresh_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_FALSE(fresh_group_arena.expired());
    ASSERT_TRUE(original_group_arena.owner_before(fresh_group_arena) || fresh_group_arena.owner_before(original_group_arena));
    ASSERT_FALSE(original_group_arena.expired());

    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    ASSERT_TRUE(sink->getPort().hasData());
    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->getChunks().size(), 1);
    Chunk output = sink->takeChunk(0);
    ASSERT_EQ(output.getNumRows(), group_count);
    /// Destroy the lane before its final empty generate/output-finish cycle. Only the retained
    /// actual output column owns the transferred states and Arena after lane/source/sink release.
    lane.reset();
    source.reset();
    sink.reset();
    ASSERT_TRUE(fresh_group_arena.expired());
    ASSERT_FALSE(original_group_arena.expired());

    std::vector<UInt64> retained_groups;
    ColumnPtr retained_view;
    {
        const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns()[0]);
        const auto & states = assert_cast<const ColumnAggregateFunction &>(*output.getColumns()[1]);
        for (size_t row = 0; row < group_count; ++row)
            retained_groups.push_back(groups.getElement(row));
        /// filter creates a COW view with the original column as its source owner, not new states.
        retained_view = states.filter(IColumn::Filter(group_count, 1), group_count);
        ASSERT_NE(retained_view.get(), &states);
        const auto & view_states = assert_cast<const ColumnAggregateFunction &>(*retained_view);
        ASSERT_EQ(view_states.size(), group_count);
        for (size_t row = 0; row < group_count; ++row)
            ASSERT_EQ(view_states.getData()[row], states.getData()[row]);
    }
    output.clear();
    ASSERT_FALSE(original_group_arena.expired());

    {
        const auto & states = assert_cast<const ColumnAggregateFunction &>(*retained_view);
        auto values = sum_function->getResultType()->createColumn();
        for (auto * const state_ptr : states.getData())
            sum_function->insertResultInto(state_ptr, *values, nullptr);
        ASSERT_EQ(values->size(), group_count);
        std::set<UInt64> observed_slopes;
        for (size_t row = 0; row < group_count; ++row)
        {
            const auto series_tag = collector->extractTag(retained_groups[row], "series");
            bool matches_known_tag = false;
            for (UInt64 slope = 1; slope <= group_count; ++slope)
            {
                if (series_tag == "series" + std::to_string(slope))
                {
                    matches_known_tag = true;
                    const auto expected_rate = makeExpectedConstantRate(rate_function->getResultType(), static_cast<Float64>(slope));
                    ASSERT_EQ(values->compareAt(row, 0, *expected_rate, 0), 0);
                    observed_slopes.insert(slope);
                }
            }
            ASSERT_TRUE(matches_known_tag);
        }
        ASSERT_EQ(observed_slopes.size(), group_count);
    }
    retained_view.reset();
    ASSERT_TRUE(original_group_arena.expired());
    recordNumber("single_block_transferred_states", group_count);
    recordNumber("grid_points", grid_points);
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

TEST_P(PromQLStateLifetimeTest, SingleBlockRetainedViewRestoresRequestedByteAccounting)
{
    /// Factory registration and shared immutable metadata outlive the measured scope.
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;
    constexpr Int64 control_bytes = 4096;

    /// This is not total_memory_tracker: MemoryWorker never corrects this signed counter.
    /// Global also avoids User/Process saturation and query-end drift settlement.
    MemoryTracker measured_tracker(&total_memory_tracker, VariableContext::Global, false);
    struct Observations
    {
        Int64 baseline = 0;
        Int64 with_original_owners = 0;
        Int64 with_retained_view_only = 0;
        Int64 after_body = 0;
        Int64 extra_free = 0;
        Int64 free_reversed = 0;
        Int64 extra_charge = 0;
        Int64 charge_reversed = 0;
        bool transferred = false;
        bool view_shares_states = false;
        bool arena_retained = false;
        bool arena_released = false;
        bool completed = false;
    } observed;
    std::exception_ptr failure;

    std::thread([&]
    {
        try
        {
            ThreadStatus thread_status;
            thread_status.flushUntrackedMemory();
            auto * previous_parent = thread_status.memory_tracker.getParent();
            const auto previous_limit = thread_status.untracked_memory_limit;
            thread_status.memory_tracker.setParent(&measured_tracker);
            thread_status.untracked_memory_limit = 0;
            SCOPE_EXIT({
                thread_status.flushUntrackedMemory();
                thread_status.memory_tracker.setParent(previous_parent);
                thread_status.untracked_memory_limit = previous_limit;
            });
            observed.baseline = measured_tracker.get();

            /// Keep gtest assertions/reporting outside the attached scope. Every mutable
            /// collector, pipeline, Chunk, census and view below is destroyed before after_body.
            [&]
            {
                const auto collector = makeCollector(group_count);
                Chunks input_chunks;
                input_chunks.emplace_back(makeInputChunk(samples_type, group_count));
                auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
                auto lane = std::make_shared<PromQLRangeSumByTransform>(
                    input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, group_count, nullptr, true);
                auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
                connect(source->getPort(), lane->getInputPort());
                connect(lane->getOutputPort(), sink->getPort());

                if (sink->prepare() != IProcessor::Status::Ready)
                    return;
                sink->work();
                if (sink->prepare() != IProcessor::Status::NeedData
                    || lane->prepare() != IProcessor::Status::NeedData
                    || source->prepare() != IProcessor::Status::Ready)
                    return;
                source->work();
                if (source->prepare() != IProcessor::Status::PortFull || lane->prepare() != IProcessor::Status::Ready)
                    return;
                lane->work();
                PromQLStateLifetimeTestAccess::finishCurrentSeries(*lane);
                const auto originals = PromQLStateLifetimeTestAccess::inspect(*lane);
                const auto original_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);

                if (lane->prepare() != IProcessor::Status::NeedData || source->prepare() != IProcessor::Status::Ready)
                    return;
                source->work();
                if (source->prepare() != IProcessor::Status::Finished || lane->prepare() != IProcessor::Status::Ready)
                    return;
                lane->work();
                const auto generated = PromQLStateLifetimeTestAccess::inspect(*lane);
                auto transferred_addresses = generated.pending_output.state_addresses;
                std::sort(transferred_addresses.begin(), transferred_addresses.end());
                observed.transferred = originals.group_count == group_count && originals.group_arena_allocated_bytes > 0
                    && generated.group_count == 0 && generated.group_arena_used_bytes == 0
                    && generated.pending_output.has_state_column && generated.pending_output.state_count == group_count
                    && transferred_addresses == originals.group_state_addresses;
                if (!observed.transferred || lane->prepare() != IProcessor::Status::Ready
                    || !sink->getPort().hasData() || sink->prepare() != IProcessor::Status::Ready)
                    return;
                sink->work();
                if (sink->getChunks().size() != 1)
                    return;
                Chunk output = sink->takeChunk(0);
                if (output.getNumRows() != group_count || output.getColumns().size() != 2)
                    return;

                ColumnPtr retained_view;
                {
                    const auto & states = assert_cast<const ColumnAggregateFunction &>(*output.getColumns()[1]);
                    retained_view = states.filter(IColumn::Filter(group_count, 1), group_count);
                    const auto & view_states = assert_cast<const ColumnAggregateFunction &>(*retained_view);
                    observed.view_shares_states = retained_view.get() != &states && view_states.size() == group_count;
                    for (size_t row = 0; observed.view_shares_states && row < group_count; ++row)
                        observed.view_shares_states = view_states.getData()[row] == states.getData()[row];
                }
                thread_status.flushUntrackedMemory();
                observed.with_original_owners = measured_tracker.get();

                output.clear();
                lane.reset();
                source.reset();
                sink.reset();
                observed.arena_retained = !original_arena.expired();
                thread_status.flushUntrackedMemory();
                observed.with_retained_view_only = measured_tracker.get();
                retained_view.reset();
                observed.arena_released = original_arena.expired();
                observed.completed = true;
            }();

            thread_status.flushUntrackedMemory();
            observed.after_body = measured_tracker.get();

            /// Accounting-only negative controls: no allocator/free is called. Reverse both
            /// perturbations, and take every reading before detach, reset or tracker destruction.
            Int64 control_delta = 0;
            SCOPE_EXIT({ thread_status.memory_tracker.adjustWithUntrackedMemory(-control_delta); });
            std::ignore = CurrentMemoryTracker::free(control_bytes);
            control_delta -= control_bytes;
            observed.extra_free = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::allocNoThrow(control_bytes);
            control_delta += control_bytes;
            observed.free_reversed = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::allocNoThrow(control_bytes);
            control_delta += control_bytes;
            observed.extra_charge = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::free(control_bytes);
            control_delta -= control_bytes;
            observed.charge_reversed = measured_tracker.get();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
    }).join();

    if (failure)
        std::rethrow_exception(failure);
    ::testing::Test::RecordProperty("baseline", std::to_string(observed.baseline));
    ::testing::Test::RecordProperty("with_original_owners", std::to_string(observed.with_original_owners));
    ::testing::Test::RecordProperty("with_retained_view_only", std::to_string(observed.with_retained_view_only));
    ::testing::Test::RecordProperty("after_body", std::to_string(observed.after_body));
    ::testing::Test::RecordProperty("extra_free", std::to_string(observed.extra_free));
    ::testing::Test::RecordProperty("free_reversed", std::to_string(observed.free_reversed));
    ::testing::Test::RecordProperty("extra_charge", std::to_string(observed.extra_charge));
    ::testing::Test::RecordProperty("charge_reversed", std::to_string(observed.charge_reversed));
    ASSERT_TRUE(observed.completed);
    EXPECT_TRUE(observed.transferred);
    EXPECT_TRUE(observed.view_shares_states);
    EXPECT_TRUE(observed.arena_retained);
    EXPECT_TRUE(observed.arena_released);
    ASSERT_EQ(observed.baseline, 0);
    EXPECT_GT(observed.with_original_owners, observed.baseline);
    EXPECT_GT(observed.with_retained_view_only, observed.baseline);
    EXPECT_EQ(observed.after_body, observed.baseline);
    EXPECT_EQ(observed.extra_free, observed.baseline - control_bytes);
    EXPECT_EQ(observed.free_reversed, observed.baseline);
    EXPECT_EQ(observed.extra_charge, observed.baseline + control_bytes);
    EXPECT_EQ(observed.charge_reversed, observed.baseline);
}

TEST_P(PromQLStateLifetimeTest, MultiBlockQueuedCopiesAndMergeRestoreRequestedByteAccounting)
{
    /// Immutable metadata and factory registration stay outside the signed ledger.
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;
    constexpr size_t grid_points = 256;
    constexpr Int64 control_bytes = 4096;

    MemoryTracker measured_tracker(&total_memory_tracker, VariableContext::Global, false);
    struct Observations
    {
        Int64 baseline = 0;
        Int64 with_originals_queued_pending = 0;
        Int64 with_destination = 0;
        Int64 after_body = 0;
        Int64 extra_free = 0;
        Int64 free_reversed = 0;
        Int64 extra_charge = 0;
        Int64 charge_reversed = 0;
        bool originals_queued_pending_distinct = false;
        bool destination_distinct = false;
        bool drained = false;
        bool lane_originals_released = false;
        bool final_states_empty = false;
        bool exact_results = false;
        bool completed = false;
    } observed;
    std::exception_ptr failure;

    std::thread([&]
    {
        try
        {
            ThreadStatus thread_status;
            thread_status.flushUntrackedMemory();
            auto * previous_parent = thread_status.memory_tracker.getParent();
            const auto previous_limit = thread_status.untracked_memory_limit;
            thread_status.memory_tracker.setParent(&measured_tracker);
            thread_status.untracked_memory_limit = 0;
            SCOPE_EXIT({
                thread_status.flushUntrackedMemory();
                thread_status.memory_tracker.setParent(previous_parent);
                thread_status.untracked_memory_limit = previous_limit;
            });
            observed.baseline = measured_tracker.get();

            /// One lane emits each group once: no shared-key or concurrent merge is exercised.
            /// All mutable owners and census/result allocations end before the final reading.
            [&]
            {
                const auto collector = makeCollector(group_count);
                Chunks input_chunks;
                input_chunks.emplace_back(makeInputChunk(samples_type, group_count));
                auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
                auto lane = std::make_shared<PromQLRangeSumByTransform>(
                    input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, 1, nullptr, true);
                auto merger = std::make_shared<PromQLPartialGroupMergeTransform>(
                    lane->getOutputPort().getSharedHeader(), sum_function, group_count, group_count);
                auto sink = std::make_shared<LifetimeCollectingSink>(merger->getOutputPort().getSharedHeader());
                connect(source->getPort(), lane->getInputPort());
                connect(lane->getOutputPort(), merger->getInputPort());
                connect(merger->getOutputPort(), sink->getPort());

                if (sink->prepare() != IProcessor::Status::Ready)
                    return;
                sink->work();
                if (sink->prepare() != IProcessor::Status::NeedData
                    || merger->prepare() != IProcessor::Status::NeedData
                    || lane->prepare() != IProcessor::Status::NeedData
                    || source->prepare() != IProcessor::Status::Ready)
                    return;
                source->work();
                if (source->prepare() != IProcessor::Status::PortFull
                    || !lane->getInputPort().hasData() || lane->prepare() != IProcessor::Status::Ready)
                    return;
                lane->work();
                if (lane->prepare() != IProcessor::Status::NeedData || source->prepare() != IProcessor::Status::Ready)
                    return;
                source->work();
                if (source->prepare() != IProcessor::Status::Finished || lane->prepare() != IProcessor::Status::Ready)
                    return;
                lane->work();

                const auto before_queue = PromQLStateLifetimeTestAccess::inspect(*lane);
                const auto original_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
                if (before_queue.group_count != group_count || before_queue.group_state_addresses.size() != group_count
                    || before_queue.group_arena_allocated_bytes == 0 || original_arena.expired()
                    || !before_queue.pending_output.has_state_column || before_queue.pending_output.state_count != 1
                    || before_queue.pending_output.state_addresses.size() != 1
                    || before_queue.pending_output.dynamic_array_size != grid_points)
                    return;
                const auto first_queued_address = before_queue.pending_output.state_addresses.front();
                const void * connection_id = lane->getOutputPort().getConnectionId();
                if (lane->prepare() != IProcessor::Status::Ready || !lane->getOutputPort().hasData()
                    || !merger->getInputPort().hasData() || connection_id != merger->getInputPort().getConnectionId())
                    return;
                lane->work();
                const auto with_pending = PromQLStateLifetimeTestAccess::inspect(*lane);
                if (!with_pending.pending_output.has_state_column || with_pending.pending_output.state_count != 1
                    || with_pending.pending_output.state_addresses.size() != 1
                    || with_pending.pending_output.dynamic_array_size != grid_points
                    || with_pending.group_state_addresses != before_queue.group_state_addresses)
                    return;
                const auto second_pending_address = with_pending.pending_output.state_addresses.front();
                std::set<std::uintptr_t> live_addresses(
                    before_queue.group_state_addresses.begin(), before_queue.group_state_addresses.end());
                live_addresses.insert(first_queued_address);
                live_addresses.insert(second_pending_address);
                observed.originals_queued_pending_distinct = live_addresses.size() == group_count + 2;
                if (!observed.originals_queued_pending_distinct)
                    return;
                thread_status.flushUntrackedMemory();
                observed.with_originals_queued_pending = measured_tracker.get();

                if (merger->prepare() != IProcessor::Status::Ready)
                    return;
                const auto while_input_held = PromQLStateLifetimeTestAccess::inspect(*merger);
                if (!while_input_held.current_input.has_state_column || while_input_held.current_input.state_count != 1
                    || while_input_held.current_input.state_addresses.size() != 1
                    || while_input_held.current_input.dynamic_array_size != grid_points
                    || while_input_held.current_input.state_addresses.front() != first_queued_address
                    || while_input_held.group_count != 0)
                    return;
                merger->work();
                const auto after_consume = PromQLStateLifetimeTestAccess::inspect(*merger);
                observed.destination_distinct = after_consume.group_count == 1
                    && after_consume.group_state_addresses.size() == 1 && !after_consume.current_input.has_state_column
                    && after_consume.group_arena_present && after_consume.group_arena_used_bytes > 0
                    && PromQLStateLifetimeTestAccess::inspect(*lane).group_state_addresses == before_queue.group_state_addresses
                    && !live_addresses.contains(after_consume.group_state_addresses.front());
                if (!observed.destination_distinct)
                    return;
                thread_status.flushUntrackedMemory();
                observed.with_destination = measured_tracker.get();

                bool lane_last_group_emitted = false;
                for (size_t iteration = 0; iteration < 128; ++iteration)
                {
                    const auto sink_status = sink->prepare();
                    if (sink_status == IProcessor::Status::Finished)
                    {
                        observed.drained = true;
                        break;
                    }
                    if (sink_status == IProcessor::Status::Ready)
                        sink->work();
                    if (merger->prepare() == IProcessor::Status::Ready)
                        merger->work();
                    if (lane->prepare() == IProcessor::Status::Ready)
                    {
                        const auto groups_before_work = PromQLStateLifetimeTestAccess::inspect(*lane).group_count;
                        lane->work();
                        const auto lane_state = PromQLStateLifetimeTestAccess::inspect(*lane);
                        if (groups_before_work > 0 && lane_state.group_count == 0)
                        {
                            if (lane_last_group_emitted || !lane_state.pending_output.has_state_column
                                || lane_state.pending_output.state_count != 1 || !original_arena.expired())
                                return;
                            lane_last_group_emitted = true;
                        }
                    }
                    if (source->prepare() == IProcessor::Status::Ready)
                        source->work();
                }
                if (!observed.drained || !lane_last_group_emitted)
                    return;
                observed.lane_originals_released = original_arena.expired();
                const auto lane_final = PromQLStateLifetimeTestAccess::inspect(*lane);
                const auto merger_final = PromQLStateLifetimeTestAccess::inspect(*merger);
                observed.final_states_empty = lane_final.group_count == 0 && lane_final.group_arena_present
                    && lane_final.group_arena_used_bytes == 0 && !lane_final.current_input.has_state_column
                    && !lane_final.pending_output.has_state_column && merger_final.group_count == 0
                    && merger_final.group_arena_present && merger_final.group_arena_used_bytes == 0
                    && !merger_final.current_input.has_state_column && !merger_final.pending_output.has_state_column;
                if (!observed.final_states_empty || sink->getChunks().size() != 1)
                    return;

                const auto & output = sink->getChunks().front();
                if (output.getNumRows() != group_count || output.getColumns().size() != 2)
                    return;
                const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns()[0]);
                const auto & values = *output.getColumns()[1];
                std::vector<std::pair<UInt64, ColumnPtr>> expected_rates;
                for (UInt64 slope = 1; slope <= group_count; ++slope)
                    expected_rates.emplace_back(slope, makeExpectedConstantRate(
                        rate_function->getResultType(), static_cast<Float64>(slope), grid_points));
                std::set<UInt64> unique_groups;
                std::set<UInt64> observed_slopes;
                for (size_t row = 0; row < group_count; ++row)
                {
                    if (!unique_groups.insert(groups.getElement(row)).second)
                        return;
                    bool matches_known_slope = false;
                    for (const auto & [slope, expected_rate] : expected_rates)
                    {
                        if (values.compareAt(row, 0, *expected_rate, 0) == 0)
                        {
                            if (collector->extractTag(groups.getElement(row), "series") != "series" + std::to_string(slope))
                                return;
                            matches_known_slope = true;
                            observed_slopes.insert(slope);
                        }
                    }
                    if (!matches_known_slope)
                        return;
                }
                observed.exact_results = unique_groups.size() == group_count && observed_slopes.size() == group_count;
                observed.completed = observed.exact_results;
            }();

            thread_status.flushUntrackedMemory();
            observed.after_body = measured_tracker.get();
            Int64 control_delta = 0;
            SCOPE_EXIT({ thread_status.memory_tracker.adjustWithUntrackedMemory(-control_delta); });
            std::ignore = CurrentMemoryTracker::free(control_bytes);
            control_delta -= control_bytes;
            observed.extra_free = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::allocNoThrow(control_bytes);
            control_delta += control_bytes;
            observed.free_reversed = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::allocNoThrow(control_bytes);
            control_delta += control_bytes;
            observed.extra_charge = measured_tracker.get();
            std::ignore = CurrentMemoryTracker::free(control_bytes);
            control_delta -= control_bytes;
            observed.charge_reversed = measured_tracker.get();
        }
        catch (...)
        {
            failure = std::current_exception();
        }
    }).join();

    if (failure)
        std::rethrow_exception(failure);
    ::testing::Test::RecordProperty("baseline", std::to_string(observed.baseline));
    ::testing::Test::RecordProperty("with_originals_queued_pending", std::to_string(observed.with_originals_queued_pending));
    ::testing::Test::RecordProperty("with_destination", std::to_string(observed.with_destination));
    ::testing::Test::RecordProperty("after_body", std::to_string(observed.after_body));
    ::testing::Test::RecordProperty("extra_free", std::to_string(observed.extra_free));
    ::testing::Test::RecordProperty("free_reversed", std::to_string(observed.free_reversed));
    ::testing::Test::RecordProperty("extra_charge", std::to_string(observed.extra_charge));
    ::testing::Test::RecordProperty("charge_reversed", std::to_string(observed.charge_reversed));
    ASSERT_TRUE(observed.completed);
    EXPECT_TRUE(observed.originals_queued_pending_distinct);
    EXPECT_TRUE(observed.destination_distinct);
    EXPECT_TRUE(observed.drained);
    EXPECT_TRUE(observed.lane_originals_released);
    EXPECT_TRUE(observed.final_states_empty);
    EXPECT_TRUE(observed.exact_results);
    ASSERT_EQ(observed.baseline, 0);
    EXPECT_GT(observed.with_originals_queued_pending, observed.baseline);
    EXPECT_GT(observed.with_destination, observed.baseline);
    EXPECT_EQ(observed.after_body, observed.baseline);
    EXPECT_EQ(observed.extra_free, observed.baseline - control_bytes);
    EXPECT_EQ(observed.free_reversed, observed.baseline);
    EXPECT_EQ(observed.extra_charge, observed.baseline + control_bytes);
    EXPECT_EQ(observed.charge_reversed, observed.baseline);
}

TEST_P(PromQLStateLifetimeTest, QuiescentCancelReleasesUnfinishedLaneStatesOnTeardown)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, group_count, nullptr, true);
    auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), sink->getPort());
    ASSERT_NO_FATAL_FAILURE(consumeLaneInput(*source, *lane, *sink));

    const auto unfinished = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(unfinished.group_count, group_count - 1);
    ASSERT_EQ(unfinished.group_state_addresses.size(), group_count - 1);
    ASSERT_TRUE(unfinished.has_current_series);
    ASSERT_TRUE(unfinished.rate_state_created);
    ASSERT_EQ(unfinished.current_series_samples, 271);
    ASSERT_FALSE(unfinished.current_input.has_chunk);
    ASSERT_FALSE(unfinished.pending_output.has_chunk);
    ASSERT_TRUE(recordGroupArenaAccounting("cancel_unfinished_original_groups", unfinished));
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_FALSE(original_group_arena.expired());

    /// All synchronous work has returned. Cancellation is not an immediate-release contract;
    /// the Arena observation below checks only destruction of the quiescent test-owned pipeline.
    lane->cancel(IProcessor::CancelReason::CancelledByUser);
    source->cancel(IProcessor::CancelReason::CancelledByUser);
    sink->cancel(IProcessor::CancelReason::CancelledByUser);
    ASSERT_TRUE(lane->isCancelled());
    ASSERT_TRUE(source->isCancelled());
    ASSERT_TRUE(sink->isCancelled());
    lane.reset();
    source.reset();
    sink.reset();
    ASSERT_TRUE(original_group_arena.expired());
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

TEST_P(PromQLStateLifetimeTest, QuiescentCancelReleasesPendingTransferredStatesOnTeardown)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;
    constexpr size_t grid_points = 256;

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, group_count, nullptr, true);
    auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), sink->getPort());
    ASSERT_NO_FATAL_FAILURE(consumeLaneInput(*source, *lane, *sink));
    ASSERT_EQ(PromQLStateLifetimeTestAccess::inspect(*lane).group_count, group_count - 1);
    PromQLStateLifetimeTestAccess::finishCurrentSeries(*lane);
    const auto originals = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(originals.group_count, group_count);
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_NO_FATAL_FAILURE(generateFirstLaneOutput(*source, *lane));

    const auto generated = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(generated.group_count, 0);
    ASSERT_EQ(generated.group_arena_used_bytes, 0);
    ASSERT_TRUE(generated.pending_output.has_state_column);
    ASSERT_EQ(generated.pending_output.state_count, group_count);
    ASSERT_EQ(generated.pending_output.dynamic_array_size, grid_points);
    auto transferred_addresses = generated.pending_output.state_addresses;
    std::sort(transferred_addresses.begin(), transferred_addresses.end());
    ASSERT_EQ(transferred_addresses, originals.group_state_addresses);
    ASSERT_EQ(generated.pending_output.column_byte_size, generated.pending_output.row_pointer_bytes);
    ASSERT_EQ(generated.pending_output.column_allocated_bytes, generated.pending_output.pointer_capacity_bytes);
    ASSERT_FALSE(lane->getOutputPort().hasData());
    ASSERT_FALSE(sink->getPort().hasData());
    ASSERT_TRUE(sink->getChunks().empty());
    const auto fresh_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_FALSE(original_group_arena.expired());
    ASSERT_FALSE(fresh_group_arena.expired());
    ASSERT_TRUE(original_group_arena.owner_before(fresh_group_arena) || fresh_group_arena.owner_before(original_group_arena));

    lane->cancel(IProcessor::CancelReason::CancelledByUser);
    source->cancel(IProcessor::CancelReason::CancelledByUser);
    sink->cancel(IProcessor::CancelReason::CancelledByUser);
    ASSERT_TRUE(lane->isCancelled());
    ASSERT_TRUE(source->isCancelled());
    ASSERT_TRUE(sink->isCancelled());
    lane.reset();
    source.reset();
    sink.reset();
    ASSERT_TRUE(original_group_arena.expired());
    ASSERT_TRUE(fresh_group_arena.expired());
    recordNumber("cancel_pending_transferred_states", group_count);
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

TEST_P(PromQLStateLifetimeTest, QuiescentCancelPreservesRetainedTransferredView)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    constexpr size_t group_count = 4;
    constexpr size_t grid_points = 256;
    constexpr size_t retained_count = 2;

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, 512, group_count, group_count, nullptr, true);
    auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), sink->getPort());
    ASSERT_NO_FATAL_FAILURE(consumeLaneInput(*source, *lane, *sink));
    ASSERT_EQ(PromQLStateLifetimeTestAccess::inspect(*lane).group_count, group_count - 1);
    PromQLStateLifetimeTestAccess::finishCurrentSeries(*lane);
    const auto originals = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(originals.group_count, group_count);
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_NO_FATAL_FAILURE(generateFirstLaneOutput(*source, *lane));
    const auto generated = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(generated.group_count, 0);
    auto transferred_addresses = generated.pending_output.state_addresses;
    std::sort(transferred_addresses.begin(), transferred_addresses.end());
    ASSERT_EQ(transferred_addresses, originals.group_state_addresses);
    const auto fresh_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);

    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    ASSERT_TRUE(sink->getPort().hasData());
    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->getChunks().size(), 1);
    Chunk output = sink->takeChunk(0);
    ASSERT_EQ(output.getNumRows(), group_count);
    std::vector<UInt64> retained_groups;
    ColumnPtr retained_view;
    {
        const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns()[0]);
        const auto & states = assert_cast<const ColumnAggregateFunction &>(*output.getColumns()[1]);
        IColumn::Filter filter(group_count, 0);
        filter[0] = 1;
        filter[group_count - 1] = 1;
        retained_groups.push_back(groups.getElement(0));
        retained_groups.push_back(groups.getElement(group_count - 1));
        retained_view = states.filter(filter, retained_count);
        ASSERT_NE(retained_view.get(), &states);
        const auto & view_states = assert_cast<const ColumnAggregateFunction &>(*retained_view);
        ASSERT_EQ(view_states.size(), retained_count);
        ASSERT_EQ(view_states.getData()[0], states.getData()[0]);
        ASSERT_EQ(view_states.getData()[1], states.getData()[group_count - 1]);
    }
    output.clear();

    lane->cancel(IProcessor::CancelReason::CancelledByUser);
    source->cancel(IProcessor::CancelReason::CancelledByUser);
    sink->cancel(IProcessor::CancelReason::CancelledByUser);
    ASSERT_TRUE(lane->isCancelled());
    ASSERT_TRUE(source->isCancelled());
    ASSERT_TRUE(sink->isCancelled());
    lane.reset();
    source.reset();
    sink.reset();
    ASSERT_TRUE(fresh_group_arena.expired());
    ASSERT_FALSE(original_group_arena.expired());
    ASSERT_NO_FATAL_FAILURE(assertRetainedConstantRates(
        assert_cast<const ColumnAggregateFunction &>(*retained_view), retained_groups, sum_function,
        rate_function->getResultType(), *collector, group_count, grid_points));
    retained_view.reset();
    ASSERT_TRUE(original_group_arena.expired());
    recordNumber("cancel_retained_transferred_view_states", retained_count);
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

TEST_P(PromQLStateLifetimeTest, QuiescentCancelPreservesLargeGridQueuedCopy)
{
    constexpr size_t group_count = 9;
    constexpr UInt32 grid_points = 4096;
    constexpr size_t block_size = 3;
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector(group_count);
    const auto rate_function = makeRateFunction(samples_type, grid_points);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks input_chunks;
    input_chunks.emplace_back(makeInputChunk(samples_type, group_count, grid_points));
    auto source = std::make_shared<LifetimeChunksSource>(input_header, std::move(input_chunks));
    auto lane = std::make_shared<PromQLRangeSumByTransform>(
        input_header, collector, rate_function, sum_function, Strings{"series"}, grid_points + 15, group_count, block_size, nullptr, true);
    auto sink = std::make_shared<LifetimeCollectingSink>(lane->getOutputPort().getSharedHeader());
    connect(source->getPort(), lane->getInputPort());
    connect(lane->getOutputPort(), sink->getPort());
    ASSERT_NO_FATAL_FAILURE(consumeLaneInput(*source, *lane, *sink));
    const auto unfinished = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(unfinished.group_count, group_count - 1);
    ASSERT_TRUE(unfinished.has_current_series);
    ASSERT_TRUE(unfinished.rate_state_created);
    ASSERT_EQ(unfinished.current_series_samples, grid_points + 15);
    PromQLStateLifetimeTestAccess::finishCurrentSeries(*lane);
    const auto originals = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(originals.group_count, group_count);
    ASSERT_EQ(originals.group_state_addresses.size(), group_count);
    ASSERT_TRUE(recordGroupArenaAccounting("cancel_large_grid_original_groups", originals));
    const auto original_group_arena = PromQLStateLifetimeTestAccess::observeGroupArena(*lane);
    ASSERT_NO_FATAL_FAILURE(generateFirstLaneOutput(*source, *lane));

    const auto before_queue = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(before_queue.group_state_addresses, originals.group_state_addresses);
    ASSERT_TRUE(before_queue.pending_output.has_state_column);
    ASSERT_EQ(before_queue.pending_output.state_count, block_size);
    ASSERT_EQ(before_queue.pending_output.dynamic_array_size, grid_points);
    ASSERT_TRUE(recordOwnedColumnAccounting("cancel_large_grid_queued_copy", before_queue.pending_output));
    const void * connection_id = lane->getOutputPort().getConnectionId();
    ASSERT_EQ(lane->prepare(), IProcessor::Status::Ready);
    ASSERT_TRUE(lane->getOutputPort().hasData());
    ASSERT_TRUE(sink->getPort().hasData());
    ASSERT_EQ(connection_id, sink->getPort().getConnectionId());
    lane->work();

    const auto with_pending = PromQLStateLifetimeTestAccess::inspect(*lane);
    ASSERT_EQ(with_pending.group_state_addresses, originals.group_state_addresses);
    ASSERT_TRUE(with_pending.pending_output.has_state_column);
    ASSERT_EQ(with_pending.pending_output.state_count, block_size);
    ASSERT_EQ(with_pending.pending_output.dynamic_array_size, grid_points);
    ASSERT_TRUE(recordOwnedColumnAccounting("cancel_large_grid_pending_copy", with_pending.pending_output));
    std::set<std::uintptr_t> distinct_states(originals.group_state_addresses.begin(), originals.group_state_addresses.end());
    distinct_states.insert(before_queue.pending_output.state_addresses.begin(), before_queue.pending_output.state_addresses.end());
    distinct_states.insert(with_pending.pending_output.state_addresses.begin(), with_pending.pending_output.state_addresses.end());
    ASSERT_EQ(distinct_states.size(), group_count + 2 * block_size);
    ASSERT_FALSE(original_group_arena.expired());

    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->getChunks().size(), 1);
    Chunk retained_output = sink->takeChunk(0);
    ASSERT_EQ(retained_output.getNumRows(), block_size);
    std::vector<UInt64> retained_groups;
    {
        const auto & groups = assert_cast<const ColumnUInt64 &>(*retained_output.getColumns()[0]);
        const auto & states = assert_cast<const ColumnAggregateFunction &>(*retained_output.getColumns()[1]);
        ASSERT_EQ(states.size(), block_size);
        std::vector<std::uintptr_t> queued_addresses;
        for (size_t row = 0; row < block_size; ++row)
        {
            retained_groups.push_back(groups.getElement(row));
            queued_addresses.push_back(reinterpret_cast<std::uintptr_t>(states.getData()[row]));
        }
        ASSERT_EQ(queued_addresses, before_queue.pending_output.state_addresses);
    }

    /// Only the already emitted queued block survives; cancelled pending and un-emitted groups
    /// are not expected downstream. Numeric snapshots above retain no Arena or state ownership.
    lane->cancel(IProcessor::CancelReason::CancelledByUser);
    source->cancel(IProcessor::CancelReason::CancelledByUser);
    sink->cancel(IProcessor::CancelReason::CancelledByUser);
    ASSERT_TRUE(lane->isCancelled());
    ASSERT_TRUE(source->isCancelled());
    ASSERT_TRUE(sink->isCancelled());
    lane.reset();
    source.reset();
    sink.reset();
    ASSERT_TRUE(original_group_arena.expired());
    ASSERT_NO_FATAL_FAILURE(assertRetainedConstantRates(
        assert_cast<const ColumnAggregateFunction &>(*retained_output.getColumns()[1]), retained_groups,
        sum_function, rate_function->getResultType(), *collector, group_count, grid_points));
    retained_output.clear();
    recordNumber("cancel_large_grid_points", grid_points);
    recordNumber("cancel_large_grid_original_groups", group_count);
    recordNumber("cancel_large_grid_output_block_size", block_size);
    recordNumber("cancel_large_grid_retained_emitted_groups", retained_groups.size());
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

TEST_P(PromQLStateLifetimeTest, GroupLimitRejectsMovedInputAndRetainsExistingDestination)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    const auto state_header = PromQLRangeSumByTransform::transformHeader(sum_function, true);
    const auto merge_header = PromQLPartialGroupMergeTransform::transformHeader(sum_function);

    Chunks input_chunks;
    input_chunks.emplace_back(makeOwnedPartialStatesChunk(sum_function));
    auto source = std::make_shared<LifetimeChunksSource>(state_header, std::move(input_chunks));
    auto merger = std::make_shared<PromQLPartialGroupMergeTransform>(state_header, sum_function, 1, 4);
    auto sink = std::make_shared<LifetimeCollectingSink>(merge_header);
    connect(source->getPort(), merger->getInputPort());
    connect(merger->getOutputPort(), sink->getPort());

    ASSERT_EQ(sink->prepare(), IProcessor::Status::Ready);
    sink->work();
    ASSERT_EQ(sink->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(merger->prepare(), IProcessor::Status::NeedData);
    ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
    source->work();
    ASSERT_EQ(source->prepare(), IProcessor::Status::PortFull);
    ASSERT_EQ(merger->prepare(), IProcessor::Status::Ready);

    try
    {
        merger->work();
        FAIL() << "Expected TOO_MANY_ROWS_OR_BYTES when the second group exceeds the limit";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::TOO_MANY_ROWS_OR_BYTES) << exception.message();
    }

    const auto after_limit = PromQLStateLifetimeTestAccess::inspect(*merger);
    ASSERT_FALSE(after_limit.current_input.has_chunk);
    ASSERT_FALSE(after_limit.current_input.has_state_column);
    ASSERT_EQ(after_limit.group_count, 1);
    ASSERT_EQ(after_limit.group_state_addresses.size(), 1);
    ASSERT_TRUE(after_limit.group_arena_present);
    ASSERT_GT(after_limit.group_arena_used_bytes, 0);
    ASSERT_TRUE(recordGroupArenaAccounting("group_limit_destination_after_exception", after_limit));
    ::testing::Test::RecordProperty("group_limit_exception_code", ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
    ::testing::Test::RecordProperty("sum_function", GetParam());
}

INSTANTIATE_TEST_SUITE_P(
    ForEachAggregateFunctions,
    PromQLStateLifetimeTest,
    ::testing::Values("sumForEach", "sumPromQLForEach"));

}
