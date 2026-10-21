#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/PromQLRangeSumByStep.h>
#include <Processors/Transforms/PromQLPartialGroupMergeTransform.h>
#include <Processors/Transforms/PromQLRangeSumByTransform.h>
#include <Processors/Transforms/PromQLRangeTopKByTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/Exception.h>
#include <Common/AlignedBuffer.h>
#include <base/scope_guard.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <bit>
#include <cmath>
#include <initializer_list>
#include <limits>
#include <optional>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

using Collector = ContextTimeSeriesTagsCollector;
using Tags = Collector::TagNamesAndValues;
using TagsPtr = Collector::TagNamesAndValuesPtr;
using Samples = std::vector<std::pair<UInt32, Float64>>;

class ChunksSource final : public ISource
{
public:
    ChunksSource(SharedHeader header_, Chunks chunks_)
        : ISource(std::move(header_), /*enable_auto_progress=*/false)
        , chunks(std::move(chunks_))
    {
    }

    String getName() const override { return "PromQLRangeSumByChunksSource"; }

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

SharedHeader makeOrderedInputHeader(const DataTypePtr & samples_type)
{
    auto id_type = std::make_shared<DataTypeUInt64>();
    auto bucket_type = std::make_shared<DataTypeUInt32>();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(id_type->createColumn(), id_type, TimeSeriesColumnNames::ID),
        ColumnWithTypeAndName(bucket_type->createColumn(), bucket_type, TimeSeriesColumnNames::Bucket),
        ColumnWithTypeAndName(samples_type->createColumn(), samples_type, TimeSeriesColumnNames::TimeSeries),
    });
}

Chunk makeSamplesChunk(const DataTypePtr & samples_type, std::initializer_list<UInt64> ids, std::initializer_list<Samples> rows)
{
    EXPECT_EQ(ids.size(), rows.size());

    auto id_column = ColumnUInt64::create();
    for (UInt64 id : ids)
        id_column->insertValue(id);

    auto samples_column = samples_type->createColumn();
    for (const auto & row : rows)
    {
        Array samples;
        samples.reserve(row.size());
        for (const auto & [timestamp, value] : row)
            samples.emplace_back(Tuple{UInt64{timestamp}, value});
        samples_column->insert(samples);
    }

    return Chunk(Columns{std::move(id_column), std::move(samples_column)}, ids.size());
}

Chunk makeOrderedSamplesChunk(
    const DataTypePtr & samples_type,
    std::initializer_list<UInt64> ids,
    std::initializer_list<UInt32> buckets,
    std::initializer_list<Samples> rows)
{
    EXPECT_EQ(ids.size(), buckets.size());
    EXPECT_EQ(ids.size(), rows.size());

    auto id_column = ColumnUInt64::create();
    for (UInt64 id : ids)
        id_column->insertValue(id);

    auto bucket_column = ColumnUInt32::create();
    for (UInt32 bucket : buckets)
        bucket_column->insertValue(bucket);

    auto samples_column = samples_type->createColumn();
    for (const auto & row : rows)
    {
        Array samples;
        samples.reserve(row.size());
        for (const auto & [timestamp, value] : row)
            samples.emplace_back(Tuple{UInt64{timestamp}, value});
        samples_column->insert(samples);
    }

    return Chunk(Columns{std::move(id_column), std::move(bucket_column), std::move(samples_column)}, ids.size());
}

TagsPtr makeTags(String namespace_value, String series)
{
    auto tags = std::make_shared<Tags>();
    tags->emplace_back("__name__", "requests_total");
    tags->emplace_back("namespace", std::move(namespace_value));
    tags->emplace_back("series", std::move(series));
    return tags;
}

std::shared_ptr<Collector> makeCollector(bool duplicate_full_tags = false)
{
    auto collector = std::make_shared<Collector>();
    auto ids = ColumnUInt64::create();
    ids->insertValue(1);
    ids->insertValue(2);
    ids->insertValue(3);

    VectorWithMemoryTracking<TagsPtr> tags;
    tags.emplace_back(makeTags("a", "one"));
    tags.emplace_back(duplicate_full_tags ? makeTags("a", "one") : makeTags("a", "two"));
    tags.emplace_back(makeTags("b", "three"));

    collector->storeTags(std::move(ids), tags);
    return collector;
}

AggregateFunctionPtr makeRateFunction(const DataTypePtr & samples_type)
{
    Array parameters{UInt64{0}, UInt64{20}, UInt64{10}, UInt64{20}};
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{samples_type}, parameters, UInt32{0}, UInt32{20}, Int32{10}, Int32{20}, 0, 0);
}

AggregateFunctionPtr makeSumFunction(const AggregateFunctionPtr & rate_function, const char * name = "sumForEach")
{
    tryRegisterAggregateFunctions();
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(
        name, NullsAction::EMPTY, DataTypes{rate_function->getResultType()}, {}, properties);
}

class PromQLRangeSumByTransformTest : public ::testing::TestWithParam<const char *>
{
};

class PromQLRangeSumByStepTest : public ::testing::TestWithParam<const char *>
{
};

class PromQLPartialGroupMergeTransformTest : public ::testing::TestWithParam<const char *>
{
};

QueryPipeline makePipeline(
    const SharedHeader & header,
    Chunks chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    const AggregateFunctionPtr & sum_function,
    Strings labels_to_keep,
    size_t max_output_groups = 1024,
    size_t max_output_block_size = 1024,
    PromQLGroupLimitPtr group_limit = nullptr,
    size_t max_samples_per_series = 1024,
    bool emit_partial_state = false)
{
    auto source = std::make_shared<ChunksSource>(header, std::move(chunks));
    auto transform = std::make_shared<PromQLRangeSumByTransform>(header,
        collector,
        rate_function,
        sum_function,
        std::move(labels_to_keep),
        max_samples_per_series,
        max_output_groups,
        max_output_block_size,
        std::move(group_limit),
        emit_partial_state);
    Pipe pipe(source);
    pipe.addTransform(transform);
    return QueryPipeline(std::move(pipe));
}

Chunk makePartialGroupsChunk(
    const SharedHeader & header, std::initializer_list<UInt64> groups, std::initializer_list<Array> values)
{
    EXPECT_EQ(groups.size(), values.size());

    auto group_column = ColumnUInt64::create();
    for (UInt64 group : groups)
        group_column->insertValue(group);

    const auto values_position = header->getPositionByName(TimeSeriesColumnNames::Values);
    auto values_column = header->getByPosition(values_position).type->createColumn();
    for (const auto & value : values)
        values_column->insert(value);

    return Chunk(Columns{std::move(group_column), std::move(values_column)}, groups.size());
}

Chunk makePartialGroupStatesChunk(
    const AggregateFunctionPtr & sum_function,
    std::initializer_list<UInt64> groups,
    std::initializer_list<std::vector<Array>> group_rows)
{
    EXPECT_EQ(groups.size(), group_rows.size());

    auto group_column = ColumnUInt64::create();
    for (UInt64 group : groups)
        group_column->insertValue(group);

    auto states_column = ColumnAggregateFunction::create(sum_function);
    for (const auto & rows : group_rows)
    {
        Arena source_arena;
        AlignedBuffer place(sum_function->sizeOfData(), sum_function->alignOfData());
        sum_function->create(place.data());
        SCOPE_EXIT({ sum_function->destroy(place.data()); });

        auto values_column = sum_function->getArgumentTypes().front()->createColumn();
        for (const auto & row : rows)
            values_column->insert(row);
        const IColumn * columns[] = {values_column.get()};
        for (size_t row = 0; row < rows.size(); ++row)
            sum_function->add(place.data(), columns, row, &source_arena);

        /// Copy the complete state into the column's arena before the source state and arena are destroyed.
        states_column->insertFrom(place.data());
    }

    return Chunk(Columns{std::move(group_column), std::move(states_column)}, groups.size());
}

QueryPipeline makePartialGroupMergePipeline(
    const SharedHeader & header,
    Chunks chunks,
    const AggregateFunctionPtr & sum_function,
    size_t max_output_groups = 1024,
    size_t max_output_block_size = 1024,
    PromQLGroupLimitPtr group_limit = nullptr)
{
    auto source = std::make_shared<ChunksSource>(header, std::move(chunks));
    auto transform = std::make_shared<PromQLPartialGroupMergeTransform>(
        header, sum_function, max_output_groups, max_output_block_size, std::move(group_limit));
    Pipe pipe(source);
    pipe.addTransform(transform);
    return QueryPipeline(std::move(pipe));
}

QueryPipeline makeRangeTopKPipeline(
    const SharedHeader & header, Chunks chunks, UInt64 k, bool bottomk, size_t max_output_block_size = 1024)
{
    auto source = std::make_shared<ChunksSource>(header, std::move(chunks));
    auto transform = std::make_shared<PromQLRangeTopKByTransform>(header, k, bottomk, max_output_block_size);
    Pipe pipe(source);
    pipe.addTransform(transform);
    return QueryPipeline(std::move(pipe));
}

template <typename Function>
void expectExceptionCode(Function && function, int expected_code)
{
    try
    {
        function();
        FAIL() << "Expected DB::Exception with code " << expected_code;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << exception.message();
    }
}

void checkRateRow(const ColumnArray & arrays, size_t row, Float64 expected_first, Float64 expected_second)
{
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    const auto & null_map = nullable.getNullMapData();
    const size_t begin = arrays.getOffsets()[row - 1];
    const size_t end = arrays.getOffsets()[row];

    ASSERT_EQ(end - begin, 3);
    EXPECT_EQ(null_map[begin], 1);
    EXPECT_EQ(null_map[begin + 1], 0);
    EXPECT_EQ(null_map[begin + 2], 0);
    EXPECT_DOUBLE_EQ(values.getElement(begin + 1), expected_first);
    EXPECT_DOUBLE_EQ(values.getElement(begin + 2), expected_second);
}

void checkArrayRow(const ColumnArray & arrays, size_t row, std::initializer_list<Float64> expected)
{
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    const auto & null_map = nullable.getNullMapData();
    const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
    const size_t end = arrays.getOffsets()[row];

    ASSERT_EQ(end - begin, expected.size());
    size_t index = 0;
    for (Float64 value : expected)
    {
        ASSERT_EQ(null_map[begin + index], 0);
        EXPECT_DOUBLE_EQ(values.getElement(begin + index), value);
        ++index;
    }
}

void checkNullableArrayRow(
    const ColumnArray & arrays, size_t row, std::initializer_list<std::optional<Float64>> expected)
{
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    const auto & null_map = nullable.getNullMapData();
    const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
    const size_t end = arrays.getOffsets()[row];

    ASSERT_EQ(end - begin, expected.size());
    size_t index = 0;
    for (const auto & value : expected)
    {
        if (!value)
            EXPECT_EQ(null_map[begin + index], 1);
        else
        {
            ASSERT_EQ(null_map[begin + index], 0);
            const Float64 actual = values.getElement(begin + index);
            if (std::isnan(*value))
                EXPECT_TRUE(std::isnan(actual));
            else
                EXPECT_DOUBLE_EQ(actual, *value);
        }
        ++index;
    }
}

}

TEST_P(PromQLRangeSumByTransformTest, StreamsOneRateStateAndSumsProjectedGroups)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {{{0, 0.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {1, 2}, {{{10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 10.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {2, 3}, {{{20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function, sum_function, Strings{"namespace"});
    PullingPipelineExecutor executor(pipeline);

    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 2);

    const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
    const auto & values = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
    ASSERT_LT(groups.getElement(0), groups.getElement(1));

    size_t namespace_a_row = 0;
    size_t namespace_b_row = 0;
    bool found_namespace_a = false;
    bool found_namespace_b = false;
    for (size_t row = 0; row < groups.size(); ++row)
    {
        const auto tags = collector->getTagsByGroup(groups.getElement(row));
        ASSERT_EQ(tags->size(), 1);
        ASSERT_EQ(tags->front().first, "namespace");
        if (tags->front().second == "a")
        {
            namespace_a_row = row;
            found_namespace_a = true;
        }
        else if (tags->front().second == "b")
        {
            namespace_b_row = row;
            found_namespace_b = true;
        }
        else
            FAIL() << "Unexpected output namespace " << tags->front().second;
    }

    ASSERT_TRUE(found_namespace_a);
    ASSERT_TRUE(found_namespace_b);
    checkRateRow(values, namespace_a_row, 1.0, 2.0);
    checkRateRow(values, namespace_b_row, 1.0, 2.0);
    ASSERT_FALSE(executor.pull(output));
}

TEST_P(PromQLRangeSumByTransformTest, DropsMetricNameBeforeByProjection)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {{{0, 0.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {1, 2}, {{{10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 10.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {2, 3}, {{{20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function, sum_function, Strings{"__name__"});
    PullingPipelineExecutor executor(pipeline);

    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);

    const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
    ASSERT_EQ(groups.getElement(0), Collector::getGroupForNoTags());
    ASSERT_TRUE(collector->getTagsByGroup(groups.getElement(0))->empty());

    const auto & values = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
    checkRateRow(values, 0, 2.0, 4.0);
    ASSERT_FALSE(executor.pull(output));
}

TEST_P(PromQLRangeSumByTransformTest, RejectsDecreasingIdentifiers)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {2, 1}, {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 10.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function, sum_function, Strings{"namespace"});
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST_P(PromQLRangeSumByTransformTest, EnforcesOutputGroupLimit)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1, 3}, {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    auto pipeline
        = makePipeline(header, std::move(chunks), collector, rate_function, sum_function, Strings{"namespace"}, /*max_output_groups=*/1);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST_P(PromQLRangeSumByTransformTest, EnforcesPerSeriesSampleLimitAcrossChunks)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {{{0, 0.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {{{10, 10.0}, {20, 20.0}}}));
    auto pipeline = makePipeline(
        header,
        std::move(chunks),
        collector,
        rate_function,
        sum_function,
        Strings{"namespace"},
        /*max_output_groups=*/1024,
        /*max_output_block_size=*/1024,
        /*group_limit=*/nullptr,
        /*max_samples_per_series=*/2);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST_P(PromQLRangeSumByTransformTest, CapsOutputBlockSize)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 3},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    auto pipeline = makePipeline(
        header,
        std::move(chunks),
        collector,
        rate_function,
        sum_function,
        Strings{"namespace"},
        /*max_output_groups=*/1024,
        /*max_output_block_size=*/1);

    PullingPipelineExecutor executor(pipeline);
    Chunk first;
    Chunk second;
    Chunk tail;
    ASSERT_TRUE(executor.pull(first));
    ASSERT_TRUE(executor.pull(second));
    ASSERT_FALSE(executor.pull(tail));
    ASSERT_EQ(first.getNumRows(), 1);
    ASSERT_EQ(second.getNumRows(), 1);

    const auto & first_groups = assert_cast<const ColumnUInt64 &>(*first.getColumns().at(0));
    const auto & second_groups = assert_cast<const ColumnUInt64 &>(*second.getColumns().at(0));
    EXPECT_LT(first_groups.getElement(0), second_groups.getElement(0));
}

TEST_P(PromQLRangeSumByTransformTest, UsesPopulatedTagsCollector)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 10.0}, {20, 20.0}}}));
    auto pipeline = makePipeline(
        header,
        std::move(chunks),
        collector,
        rate_function,
        sum_function,
        Strings{"namespace"},
        1024);

    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);
    ASSERT_FALSE(executor.pull(output));
}

TEST(ContextTimeSeriesTagsCollector, DetectsDifferentIdentifiersWithSameFullTags)
{
    ASSERT_FALSE(makeCollector()->hasMultipleIdentifiersForSameTags());
    ASSERT_TRUE(makeCollector(/*duplicate_full_tags=*/true)->hasMultipleIdentifiersForSameTags());
}

TEST_P(PromQLRangeSumByStepTest, BuildsAndRunsSingleStreamPipeline)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1, 2}, {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 10.0}}}));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe(std::make_shared<ChunksSource>(header, std::move(chunks))));

    PromQLRangeSumByStep step(
        header,
        collector,
        rate_function,
        sum_function,
        Strings{"namespace"},
        /*max_samples_per_series=*/1024,
        /*max_output_groups=*/1024,
        /*max_output_block_size=*/1024);
    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));

    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);
    ASSERT_FALSE(executor.pull(output));
}

TEST_P(PromQLRangeSumByStepTest, SingleOrderedStreamAcceptsSparseUInt64Identifiers)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeOrderedInputHeader(samples_type);
    auto collector = std::make_shared<Collector>();
    auto ids = ColumnUInt64::create();
    ids->insertValue(0);
    ids->insertValue(1);
    VectorWithMemoryTracking<TagsPtr> tags{makeTags("a", "zero"), makeTags("a", "one")};
    collector->storeTags(std::move(ids), tags);
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    auto execute = [&](bool sparse)
    {
        auto input = makeOrderedSamplesChunk(
            samples_type, {0, 0, 0, 1}, {0, 1, 2, 0},
            {{{0, 0.0}}, {{10, 10.0}}, {{20, 20.0}}, {{0, 0.0}, {10, 10.0}, {20, 20.0}}});
        if (sparse)
        {
            auto values = ColumnUInt64::create();
            values->insertValue(0);
            values->insertValue(1);
            auto offsets = ColumnUInt64::create();
            offsets->insertValue(3);
            auto columns = input.detachColumns();
            columns[0] = ColumnSparse::create(std::move(values), std::move(offsets), 4);
            input.setColumns(std::move(columns), 4);
        }
        Chunks chunks;
        chunks.emplace_back(std::move(input));
        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(Pipe(std::make_shared<ChunksSource>(header, std::move(chunks))));
        PromQLRangeSumByStep step(
            header, collector, rate_function, sum_function, Strings{"namespace"},
            /*max_samples_per_series=*/1024, /*max_output_groups=*/1024, /*max_output_block_size=*/1024);
        QueryPipelineBuilders inputs;
        inputs.emplace_back(std::move(builder));
        BuildQueryPipelineSettings settings(getContext().context);
        auto result = step.updatePipeline(std::move(inputs), settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));
        PullingPipelineExecutor executor(pipeline);
        Chunk output;
        EXPECT_TRUE(executor.pull(output));
        Chunk extra;
        EXPECT_FALSE(executor.pull(extra));
        return output;
    };

    const auto expected = execute(/*sparse=*/false);
    ASSERT_EQ(expected.getNumRows(), 1);
    checkRateRow(assert_cast<const ColumnArray &>(*expected.getColumns().at(1)), 0, 1.0, 2.0);
    const auto actual = execute(/*sparse=*/true);
    ASSERT_EQ(actual.getNumRows(), expected.getNumRows());
    ASSERT_EQ(actual.getNumColumns(), expected.getNumColumns());
    for (size_t column = 0; column < expected.getNumColumns(); ++column)
        EXPECT_EQ(expected.getColumns()[column]->compareAt(0, 0, *actual.getColumns()[column], 1), 0);
}

TEST_P(PromQLRangeSumByStepTest, MergesSortedInputStreamsBeforeNativeKernel)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeOrderedInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    auto execute = [&](int input_order)
    {
        Pipes pipes;
        if (input_order == 2)
        {
            Chunks canonical_chunks;
            canonical_chunks.emplace_back(makeOrderedSamplesChunk(
                samples_type,
                {1, 2, 3},
                {0, 0, 0},
                {{{0, 0.0}, {10, 15.0}, {20, 20.0}},
                 {{0, 0.0}, {10, 10.0}, {20, 20.0}},
                 {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
            pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(canonical_chunks)));
        }
        else
        {
            Chunks first_stream_chunks;
            first_stream_chunks.emplace_back(makeOrderedSamplesChunk(
                samples_type,
                {1, 3},
                {0, 0},
                {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

            Chunks second_stream_chunks;
            second_stream_chunks.emplace_back(makeOrderedSamplesChunk(
                samples_type,
                {1, 2},
                {0, 0},
                {{{10, 15.0}, {20, 20.0}}, {{0, 0.0}, {10, 10.0}, {20, 20.0}}}));

            if (input_order == 1)
            {
                pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_stream_chunks)));
                pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_stream_chunks)));
            }
            else
            {
                pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_stream_chunks)));
                pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_stream_chunks)));
            }
        }

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(Pipe::unitePipes(std::move(pipes)));

        PromQLRangeSumByStep step(
            header,
            collector,
            rate_function,
            sum_function,
            Strings{"namespace"},
            /*max_samples_per_series=*/1024,
            /*max_output_groups=*/1024,
            /*max_output_block_size=*/1024);
        QueryPipelineBuilders inputs;
        inputs.emplace_back(std::move(builder));
        BuildQueryPipelineSettings settings(getContext().context);
        auto result = step.updatePipeline(std::move(inputs), settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));

        PullingPipelineExecutor executor(pipeline);
        Chunk output;
        EXPECT_TRUE(executor.pull(output));
        Chunk tail;
        EXPECT_FALSE(executor.pull(tail));
        return output;
    };

    Chunk output = execute(0);
    Chunk reverse_output = execute(1);
    Chunk canonical_output = execute(2);
    ASSERT_EQ(output.getNumRows(), 2);
    ASSERT_EQ(reverse_output.getNumRows(), output.getNumRows());
    ASSERT_EQ(reverse_output.getNumColumns(), output.getNumColumns());
    ASSERT_EQ(canonical_output.getNumRows(), output.getNumRows());
    ASSERT_EQ(canonical_output.getNumColumns(), output.getNumColumns());
    for (size_t column = 0; column < output.getNumColumns(); ++column)
    {
        for (size_t row = 0; row < output.getNumRows(); ++row)
        {
            EXPECT_EQ(output.getColumns()[column]->compareAt(row, row, *reverse_output.getColumns()[column], 1), 0);
            EXPECT_EQ(output.getColumns()[column]->compareAt(row, row, *canonical_output.getColumns()[column], 1), 0);
        }
    }

    const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
    bool found_namespace_a = false;
    bool found_namespace_b = false;
    for (size_t row = 0; row < groups.size(); ++row)
    {
        const auto tags = collector->getTagsByGroup(groups.getElement(row));
        ASSERT_EQ(tags->size(), 1);
        ASSERT_EQ(tags->front().first, "namespace");
        if (tags->front().second == "a")
        {
            ASSERT_FALSE(found_namespace_a);
            found_namespace_a = true;
        }
        else if (tags->front().second == "b")
        {
            ASSERT_FALSE(found_namespace_b);
            found_namespace_b = true;
        }
        else
            FAIL() << "Unexpected output namespace " << tags->front().second;
    }
    ASSERT_TRUE(found_namespace_a);
    ASSERT_TRUE(found_namespace_b);
}

TEST(PromQLGroupLimit, BoundsDistinctOutputGroups)
{
    PromQLGroupLimit limit(/* max_groups_ = */ 2);
    ASSERT_TRUE(limit.tryRegister(10));
    ASSERT_TRUE(limit.tryRegister(10));
    ASSERT_TRUE(limit.tryRegister(20));
    ASSERT_FALSE(limit.tryRegister(30));
}

TEST_P(PromQLRangeSumByStepTest, ParallelLanesShareOneOutputGroupAtLimit)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeOrderedInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());

    Pipes pipes;
    Chunks first_stream_chunks;
    first_stream_chunks.emplace_back(makeOrderedSamplesChunk(
        samples_type, {1}, {0}, {{{0, 0.0}, {10, 10.0}, {20, 20.0}}}));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_stream_chunks)));

    Chunks second_stream_chunks;
    second_stream_chunks.emplace_back(makeOrderedSamplesChunk(
        samples_type, {2}, {0}, {{{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_stream_chunks)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLRangeSumByStep step(
        header,
        collector,
        rate_function,
        sum_function,
        Strings{"namespace"},
        /*max_samples_per_series=*/1024,
        /*max_output_groups=*/1,
        /*max_output_block_size=*/1024);
    step.enableParallelProcessing();
    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));

    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);
    ASSERT_FALSE(executor.pull(output));
}

TEST_P(PromQLPartialGroupMergeTransformTest, MergesPartialGroupsAndSortsGroupKeys)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function, /*emit_partial_state=*/true);

    Chunks chunks;
    chunks.emplace_back(makePartialGroupStatesChunk(
        sum_function,
        {20, 10},
        {{Array{Field{1.0}, Field{2.0}}}, {Array{Field{3.0}, Field{4.0}}}}));
    chunks.emplace_back(makePartialGroupStatesChunk(
        sum_function,
        {20, 30},
        {{Array{Field{5.0}, Field{6.0}}}, {Array{Field{7.0}, Field{8.0}}}}));

    auto pipeline = makePartialGroupMergePipeline(header, std::move(chunks), sum_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_FALSE(executor.pull(output));

    ASSERT_EQ(output.getNumRows(), 3);
    const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
    const auto & values = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
    EXPECT_EQ(groups.getElement(0), 10);
    EXPECT_EQ(groups.getElement(1), 20);
    EXPECT_EQ(groups.getElement(2), 30);
    checkArrayRow(values, 0, {3.0, 4.0});
    checkArrayRow(values, 1, {6.0, 8.0});
    checkArrayRow(values, 2, {7.0, 8.0});
}

TEST(PromQLPartialGroupMergeTransform, KeepsCompensationAfterSourceStatesAreDestroyed)
{
    const auto rate_function = makeRateFunction(makeSamplesType());
    const auto sum_function = makeSumFunction(rate_function, "sumPromQLForEach");
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function, /*emit_partial_state=*/true);
    const Float64 epsilon = 0x1p-53;

    Chunks chunks;
    chunks.emplace_back(makePartialGroupStatesChunk(sum_function, {10}, {{Array{1.0}, Array{epsilon}}}));
    chunks.emplace_back(makePartialGroupStatesChunk(sum_function, {10}, {{Array{0.0}, Array{epsilon}}}));

    auto pipeline = makePartialGroupMergePipeline(header, std::move(chunks), sum_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);
    const auto & arrays = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    ASSERT_EQ(values.size(), 1);
    ASSERT_EQ(nullable.getNullMapData()[0], 0);
    EXPECT_EQ(std::bit_cast<UInt64>(values.getElement(0)), std::bit_cast<UInt64>(1.0 + 0x1p-52));
    EXPECT_EQ(std::ceil(values.getElement(0)), 2.0);
    ASSERT_FALSE(executor.pull(output));

    /// Finalizing the first lane loses its compensation; adding the second lane then stays at one.
    EXPECT_EQ(std::bit_cast<UInt64>(1.0 + epsilon), std::bit_cast<UInt64>(1.0));
    EXPECT_EQ(std::ceil(1.0 + epsilon), 1.0);
}

TEST(PromQLRangeSumByTransform, EmittedStateSurvivesLanePipelineDestruction)
{
    const auto samples_type = makeSamplesType();
    const auto input_header = makeInputHeader(samples_type);
    const auto collector = makeCollector();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, "sumPromQLForEach");
    const Float64 epsilon = 0x1p-53;

    Chunk lane_output;
    {
        Chunks input;
        input.emplace_back(makeSamplesChunk(
            samples_type, {1, 2},
            {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 10.0 * epsilon}, {20, 20.0 * epsilon}}}));
        auto pipeline = makePipeline(
            input_header, std::move(input), collector, rate_function, sum_function, Strings{"namespace"},
            1024, 1024, nullptr, 1024, /*emit_partial_state=*/true);
        PullingPipelineExecutor executor(pipeline);
        ASSERT_TRUE(executor.pull(lane_output));
        ASSERT_EQ(lane_output.getNumRows(), 1);
        ASSERT_NE(typeid_cast<const ColumnAggregateFunction *>(lane_output.getColumns().at(1).get()), nullptr);
        Chunk tail;
        ASSERT_FALSE(executor.pull(tail));
    }

    /// The producing transform and its arena are gone before the output is merged.
    const auto group = assert_cast<const ColumnUInt64 &>(*lane_output.getColumns().at(0)).getElement(0);
    Chunks partials;
    partials.emplace_back(std::move(lane_output));
    partials.emplace_back(makePartialGroupStatesChunk(sum_function, {group}, {{Array{Field{}, epsilon / 2, epsilon}}}));
    auto pipeline = makePartialGroupMergePipeline(
        PromQLRangeSumByTransform::transformHeader(sum_function, /*emit_partial_state=*/true), std::move(partials), sum_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    ASSERT_EQ(output.getNumRows(), 1);
    const auto & arrays = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(nullable.getNullMapData()[0], 1);
    EXPECT_EQ(nullable.getNullMapData()[1], 0);
    EXPECT_EQ(nullable.getNullMapData()[2], 0);
    EXPECT_EQ(std::bit_cast<UInt64>(values.getElement(1)), std::bit_cast<UInt64>(0.5 + 0x1p-53));
    EXPECT_EQ(std::bit_cast<UInt64>(values.getElement(2)), std::bit_cast<UInt64>(1.0 + 0x1p-52));
    EXPECT_EQ(std::ceil(values.getElement(2)), 2.0);
    ASSERT_FALSE(executor.pull(output));
}

TEST_P(PromQLPartialGroupMergeTransformTest, RejectsFinalizedGridInput)
{
    const auto rate_function = makeRateFunction(makeSamplesType());
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function);
    expectExceptionCode(
        [&] { PromQLPartialGroupMergeTransform transform(header, sum_function, 1024, 1024); }, ErrorCodes::BAD_ARGUMENTS);
}

TEST_P(PromQLPartialGroupMergeTransformTest, RejectsOtherModeStateHeader)
{
    const auto rate_function = makeRateFunction(makeSamplesType());
    const auto producer_sum_function = makeSumFunction(rate_function, GetParam());
    const auto merger_sum_function = makeSumFunction(
        rate_function, producer_sum_function->getName() == "sumForEach" ? "sumPromQLForEach" : "sumForEach");
    const auto header = PromQLRangeSumByTransform::transformHeader(producer_sum_function, /*emit_partial_state=*/true);

    ASSERT_FALSE(header->getByName(TimeSeriesColumnNames::Values).type->equals(*merger_sum_function->getStateType()));
    expectExceptionCode(
        [&] { PromQLPartialGroupMergeTransform transform(header, merger_sum_function, 1024, 1024); }, ErrorCodes::BAD_ARGUMENTS);
}

TEST_P(PromQLPartialGroupMergeTransformTest, EnforcesGlobalOutputGroupLimit)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function, /*emit_partial_state=*/true);

    Chunks chunks;
    chunks.emplace_back(makePartialGroupStatesChunk(sum_function, {10}, {{Array{Field{1.0}}}}));
    chunks.emplace_back(makePartialGroupStatesChunk(sum_function, {20}, {{Array{Field{2.0}}}}));

    auto pipeline = makePartialGroupMergePipeline(header, std::move(chunks), sum_function, /*max_output_groups=*/1);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST_P(PromQLPartialGroupMergeTransformTest, CapsOutputBlockSize)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function, GetParam());
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function, /*emit_partial_state=*/true);

    Chunks chunks;
    chunks.emplace_back(makePartialGroupStatesChunk(
        sum_function,
        {30, 10, 20},
        {{Array{Field{3.0}}}, {Array{Field{1.0}}}, {Array{Field{2.0}}}}));
    auto pipeline = makePartialGroupMergePipeline(
        header,
        std::move(chunks),
        sum_function,
        /*max_output_groups=*/1024,
        /*max_output_block_size=*/1);

    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> groups;
    Chunk output;
    while (executor.pull(output))
    {
        ASSERT_EQ(output.getNumRows(), 1);
        const auto & group_column = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
        groups.push_back(group_column.getElement(0));
    }
    EXPECT_EQ(groups, (std::vector<UInt64>{10, 20, 30}));
}

INSTANTIATE_TEST_SUITE_P(
    SumModes, PromQLRangeSumByTransformTest, ::testing::Values("sumForEach", "sumPromQLForEach"));
INSTANTIATE_TEST_SUITE_P(
    SumModes, PromQLRangeSumByStepTest, ::testing::Values("sumForEach", "sumPromQLForEach"));
INSTANTIATE_TEST_SUITE_P(
    SumModes, PromQLPartialGroupMergeTransformTest, ::testing::Values("sumForEach", "sumPromQLForEach"));

TEST(PromQLRangeTopKByTransform, SelectsPerStepWithDeterministicPromQLSemantics)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function);
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function);
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();

    auto execute = [&](bool bottomk)
    {
        Chunks chunks;
        chunks.emplace_back(makePartialGroupsChunk(
            header,
            {30, 20},
            {
                Array{Field{3.0}, Field{nan}, Field{}, Field{1.0}, Field{nan}},
                Array{Field{2.0}, Field{5.0}, Field{7.0}, Field{1.0}, Field{}},
            }));
        chunks.emplace_back(makePartialGroupsChunk(
            header,
            {10},
            {Array{Field{3.0}, Field{4.0}, Field{8.0}, Field{}, Field{}}}));

        auto pipeline = makeRangeTopKPipeline(header, std::move(chunks), /*k=*/1, bottomk);
        PullingPipelineExecutor executor(pipeline);
        Chunk output;
        EXPECT_TRUE(executor.pull(output));
        Chunk tail;
        EXPECT_FALSE(executor.pull(tail));
        return output;
    };

    Chunk topk = execute(/*bottomk=*/false);
    ASSERT_EQ(topk.getNumRows(), 3);
    const auto & topk_groups = assert_cast<const ColumnUInt64 &>(*topk.getColumns().at(0));
    const auto & topk_values = assert_cast<const ColumnArray &>(*topk.getColumns().at(1));
    EXPECT_EQ(topk_groups.getElement(0), 10);
    EXPECT_EQ(topk_groups.getElement(1), 20);
    EXPECT_EQ(topk_groups.getElement(2), 30);
    checkNullableArrayRow(topk_values, 0, {3.0, std::nullopt, 8.0, std::nullopt, std::nullopt});
    checkNullableArrayRow(topk_values, 1, {std::nullopt, 5.0, std::nullopt, 1.0, std::nullopt});
    checkNullableArrayRow(topk_values, 2, {std::nullopt, std::nullopt, std::nullopt, std::nullopt, nan});

    Chunk bottomk = execute(/*bottomk=*/true);
    ASSERT_EQ(bottomk.getNumRows(), 3);
    const auto & bottomk_groups = assert_cast<const ColumnUInt64 &>(*bottomk.getColumns().at(0));
    const auto & bottomk_values = assert_cast<const ColumnArray &>(*bottomk.getColumns().at(1));
    EXPECT_EQ(bottomk_groups.getElement(0), 10);
    EXPECT_EQ(bottomk_groups.getElement(1), 20);
    EXPECT_EQ(bottomk_groups.getElement(2), 30);
    checkNullableArrayRow(bottomk_values, 0, {std::nullopt, 4.0, std::nullopt, std::nullopt, std::nullopt});
    checkNullableArrayRow(bottomk_values, 1, {2.0, std::nullopt, 7.0, 1.0, std::nullopt});
    checkNullableArrayRow(bottomk_values, 2, {std::nullopt, std::nullopt, std::nullopt, std::nullopt, nan});
}

TEST(PromQLRangeTopKByTransform, ZeroKProducesNoRows)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function);
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function);

    Chunks chunks;
    chunks.emplace_back(makePartialGroupsChunk(header, {10}, {Array{Field{1.0}, Field{2.0}}}));
    auto pipeline = makeRangeTopKPipeline(header, std::move(chunks), /*k=*/0, /*bottomk=*/false);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    EXPECT_FALSE(executor.pull(output));
}

TEST(PromQLRangeTopKByTransform, CapsOutputBlockSizeAcrossStepUnion)
{
    const auto samples_type = makeSamplesType();
    const auto rate_function = makeRateFunction(samples_type);
    const auto sum_function = makeSumFunction(rate_function);
    const auto header = PromQLRangeSumByTransform::transformHeader(sum_function);

    Chunks chunks;
    chunks.emplace_back(makePartialGroupsChunk(
        header,
        {30, 20, 10},
        {
            Array{Field{3.0}, Field{1.0}, Field{1.0}},
            Array{Field{1.0}, Field{3.0}, Field{1.0}},
            Array{Field{1.0}, Field{1.0}, Field{3.0}},
        }));
    auto pipeline = makeRangeTopKPipeline(
        header, std::move(chunks), /*k=*/1, /*bottomk=*/false, /*max_output_block_size=*/1);
    PullingPipelineExecutor executor(pipeline);

    std::vector<UInt64> groups;
    Chunk output;
    while (executor.pull(output))
    {
        ASSERT_EQ(output.getNumRows(), 1);
        const auto & group_column = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
        groups.push_back(group_column.getElement(0));
    }
    EXPECT_EQ(groups, (std::vector<UInt64>{10, 20, 30}));
}

}
