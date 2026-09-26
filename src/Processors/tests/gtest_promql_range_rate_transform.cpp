#include <gtest/gtest.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/PromQLRangeRateStep.h>
#include <Processors/Transforms/PromQLRangeRateTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>
#include <cmath>
#include <initializer_list>
#include <limits>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int INCORRECT_DATA;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

using Collector = ContextTimeSeriesTagsCollector;
using Group = Collector::Group;
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

    String getName() const override { return "PromQLRangeRateChunksSource"; }

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

SharedHeader makeInputHeader(
    const DataTypePtr & samples_type,
    const String & samples_column_name = TimeSeriesColumnNames::TimeSeries)
{
    auto id_type = std::make_shared<DataTypeUInt64>();
    auto bucket_type = std::make_shared<DataTypeUInt32>();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(id_type->createColumn(), id_type, TimeSeriesColumnNames::ID),
        ColumnWithTypeAndName(bucket_type->createColumn(), bucket_type, TimeSeriesColumnNames::Bucket),
        ColumnWithTypeAndName(samples_type->createColumn(), samples_type, samples_column_name),
    });
}

Chunk makeSamplesChunk(
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

    return Chunk(
        Columns{std::move(id_column), std::move(bucket_column), std::move(samples_column)},
        ids.size());
}

TagsPtr makeTags(String metric_name, String namespace_value, String series)
{
    auto tags = std::make_shared<Tags>();
    tags->emplace_back(TimeSeriesTagNames::MetricName, std::move(metric_name));
    tags->emplace_back("namespace", std::move(namespace_value));
    tags->emplace_back("series", std::move(series));
    return tags;
}

std::shared_ptr<Collector> makeCollector(
    std::initializer_list<std::tuple<UInt64, String, String, String>> series)
{
    auto collector = std::make_shared<Collector>();
    auto ids = ColumnUInt64::create();
    VectorWithMemoryTracking<TagsPtr> tags;
    for (const auto & [id, metric_name, namespace_value, series_value] : series)
    {
        ids->insertValue(id);
        tags.emplace_back(makeTags(metric_name, namespace_value, series_value));
    }
    collector->storeTags(std::move(ids), tags);
    return collector;
}

AggregateFunctionPtr makeRateFunction(const DataTypePtr & samples_type)
{
    Array parameters{UInt64{0}, UInt64{20}, UInt64{10}, UInt64{20}};
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{samples_type}, parameters, UInt32{0}, UInt32{20}, Int32{10}, Int32{20}, 0, 0);
}

AggregateFunctionPtr makeRawRateFunction()
{
    Array parameters{UInt64{0}, UInt64{20}, UInt64{10}, UInt64{20}};
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()},
        parameters,
        UInt32{0},
        UInt32{20},
        Int32{10},
        Int32{20},
        0,
        0);
}

QueryPipeline makePipeline(
    const SharedHeader & header,
    Chunks chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series = 1024,
    size_t max_output_block_size = 1024,
    std::optional<Field> raw_min_time = {},
    std::optional<Field> raw_max_time = {})
{
    auto source = std::make_shared<ChunksSource>(header, std::move(chunks));
    auto transform = std::make_shared<PromQLRangeRateTransform>(
        header,
        collector,
        rate_function,
        max_samples_per_series,
        max_output_block_size,
        nullptr,
        std::move(raw_min_time),
        std::move(raw_max_time));
    Pipe pipe(source);
    pipe.addTransform(transform);
    return QueryPipeline(std::move(pipe));
}

QueryPipeline makeMaterializedMergePipeline(
    const SharedHeader & header,
    std::vector<Chunks> source_chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series,
    size_t max_output_block_size,
    std::optional<Field> raw_min_time = {},
    std::optional<Field> raw_max_time = {})
{
    Pipes pipes;
    for (auto & chunks : source_chunks)
        pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(chunks)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    SortDescription description;
    description.emplace_back(TimeSeriesColumnNames::ID, 1, 1);
    description.emplace_back(TimeSeriesColumnNames::Bucket, 1, 1);
    builder->addTransform(std::make_shared<MergingSortedTransform>(
        header,
        source_chunks.size(),
        description,
        max_output_block_size,
        /*max_block_size_bytes=*/0,
        /*max_dynamic_subcolumns=*/std::nullopt,
        SortingQueueStrategy::Batch));
    builder->addSimpleTransform(
        [collector,
         rate_function,
         max_samples_per_series,
         max_output_block_size,
         raw_min_time,
         raw_max_time](const SharedHeader & transformed_header)
        {
            return std::make_shared<PromQLRangeRateTransform>(
                transformed_header,
                collector,
                rate_function,
                max_samples_per_series,
                max_output_block_size,
                nullptr,
                raw_min_time,
                raw_max_time);
        });

    return QueryPipelineBuilder::getPipeline(std::move(*builder));
}

QueryPipeline makeFusedMergePipeline(
    const SharedHeader & header,
    std::vector<Chunks> source_chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series,
    size_t max_output_block_size,
    std::optional<Field> raw_min_time = {},
    std::optional<Field> raw_max_time = {})
{
    Pipes pipes;
    for (auto & chunks : source_chunks)
        pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(chunks)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLRangeRateStep step(
        header,
        collector,
        rate_function,
        max_samples_per_series,
        max_output_block_size,
        /*parallel_processing_requested=*/false,
        /*max_parallel_lanes=*/0,
        raw_min_time,
        raw_max_time);
    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    return QueryPipelineBuilder::getPipeline(std::move(*result));
}

std::vector<Chunks> cloneSourceChunks(const std::vector<Chunks> & source_chunks)
{
    std::vector<Chunks> result;
    result.reserve(source_chunks.size());
    for (const auto & source : source_chunks)
    {
        Chunks cloned_source;
        cloned_source.reserve(source.size());
        for (const auto & chunk : source)
            cloned_source.emplace_back(chunk.clone());
        result.emplace_back(std::move(cloned_source));
    }
    return result;
}

std::vector<Chunk> pullAllOutputChunks(QueryPipeline pipeline)
{
    PullingPipelineExecutor executor(pipeline);
    std::vector<Chunk> result;
    Chunk output;
    while (executor.pull(output))
        result.emplace_back(std::move(output));
    return result;
}

void expectEquivalentOutput(const std::vector<Chunk> & expected, const std::vector<Chunk> & actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t chunk_num = 0; chunk_num < expected.size(); ++chunk_num)
    {
        const auto & expected_chunk = expected[chunk_num];
        const auto & actual_chunk = actual[chunk_num];
        ASSERT_EQ(expected_chunk.getNumRows(), actual_chunk.getNumRows());
        ASSERT_EQ(expected_chunk.getNumColumns(), actual_chunk.getNumColumns());
        for (size_t column_num = 0; column_num < expected_chunk.getNumColumns(); ++column_num)
        {
            const auto & expected_column = expected_chunk.getColumns()[column_num];
            const auto & actual_column = actual_chunk.getColumns()[column_num];
            for (size_t row = 0; row < expected_chunk.getNumRows(); ++row)
                EXPECT_EQ(expected_column->compareAt(row, row, *actual_column, 1), 0)
                    << "chunk=" << chunk_num << " column=" << column_num << " row=" << row;
        }
    }
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
    const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
    const size_t end = arrays.getOffsets()[row];

    ASSERT_EQ(end - begin, 3);
    EXPECT_EQ(null_map[begin], 1);
    EXPECT_EQ(null_map[begin + 1], 0);
    EXPECT_EQ(null_map[begin + 2], 0);
    EXPECT_DOUBLE_EQ(values.getElement(begin + 1), expected_first);
    EXPECT_DOUBLE_EQ(values.getElement(begin + 2), expected_second);
}

void checkNaNRateRow(const ColumnArray & arrays, size_t row)
{
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
    const auto & null_map = nullable.getNullMapData();
    const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
    const size_t end = arrays.getOffsets()[row];

    ASSERT_EQ(end - begin, 3);
    EXPECT_EQ(null_map[begin], 1);
    EXPECT_EQ(null_map[begin + 1], 0);
    EXPECT_EQ(null_map[begin + 2], 0);
    EXPECT_TRUE(std::isnan(values.getElement(begin + 1)));
    EXPECT_TRUE(std::isnan(values.getElement(begin + 2)));
}

Chunk pullSingleOutputChunk(QueryPipeline pipeline)
{
    PullingPipelineExecutor executor(pipeline);
    Chunk result;
    EXPECT_TRUE(executor.pull(result));
    Chunk extra;
    EXPECT_FALSE(executor.pull(extra));
    return result;
}

}

TEST(PromQLRangeRateTransform, AccumulatesOneStateAcrossChunksAndBucketsWithBoundedOutput)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {0}, {{{0, 0.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {1}, {{{10, 10.0}, {20, 20.0}}}));
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2, 2},
        {0, 1},
        {{{0, 0.0}, {10, 20.0}}, {{20, 40.0}}}));

    auto pipeline = makePipeline(
        header,
        std::move(chunks),
        collector,
        rate_function,
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1);
    PullingPipelineExecutor executor(pipeline);

    std::vector<Chunk> output_chunks;
    Chunk output;
    while (executor.pull(output))
        output_chunks.emplace_back(std::move(output));

    ASSERT_EQ(output_chunks.size(), 2);
    for (const auto & chunk : output_chunks)
        ASSERT_EQ(chunk.getNumRows(), 1);

    const auto & first_values = assert_cast<const ColumnArray &>(*output_chunks[0].getColumns().at(1));
    const auto & second_values = assert_cast<const ColumnArray &>(*output_chunks[1].getColumns().at(1));
    checkRateRow(first_values, 0, 0.5, 1.0);
    checkRateRow(second_values, 0, 1.0, 2.0);
}

TEST(PromQLRangeRateTransform, DropsMetricNameFromEachOutputGroup)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function);
    PullingPipelineExecutor executor(pipeline);

    std::vector<Group> output_groups;
    Chunk output;
    while (executor.pull(output))
    {
        const auto & groups = assert_cast<const ColumnUInt64 &>(*output.getColumns().at(0));
        for (size_t row = 0; row < groups.size(); ++row)
            output_groups.push_back(groups.getElement(row));
    }

    ASSERT_EQ(output_groups.size(), 2);
    for (Group group : output_groups)
    {
        const auto tags = collector->getTagsByGroup(group);
        ASSERT_EQ(tags->size(), 2);
        for (const auto & [name, value] : *tags)
        {
            EXPECT_NE(name, TimeSeriesTagNames::MetricName);
            EXPECT_FALSE(value.empty());
        }
    }
}

TEST(PromQLRangeRateTransform, RejectsDuplicatePostMetricNameGroups)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLRangeRateTransform, RejectsDecreasingIdentifiers)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2, 1},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 10.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLRangeRateTransform, RejectsDecreasingBucketsWithinSeries)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 1},
        {1, 0},
        {{{0, 0.0}}, {{10, 10.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLRangeRateTransform, RejectsArrayBeforeCumulativeSampleLimitIsCrossed)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {0}, {{{0, 0.0}, {10, 10.0}}}));
    chunks.emplace_back(makeSamplesChunk(samples_type, {1}, {1}, {{{20, 20.0}, {30, 30.0}}}));
    auto pipeline = makePipeline(
        header,
        std::move(chunks),
        collector,
        rate_function,
        /*max_samples_per_series=*/3);
    PullingPipelineExecutor executor(pipeline);
    Chunk output;
    expectExceptionCode([&] { executor.pull(output); }, ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST(PromQLRangeRateTransform, RawSamplesUseInclusiveNestedSlicesAndSkipEmptySeriesBeforeRegistration)
{
    const auto samples_type = makeSamplesType();
    const auto raw_header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto sliced_header = makeInputHeader(samples_type);

    /// IDs 1 and 2 deliberately collapse to the same post-metric-name group. ID 1
    /// has metadata-overlapping rows but no sample in [10, 20], so it must not
    /// register the group and block the non-empty ID 2 series.
    const auto raw_collector = makeCollector({
        {1, "empty_metric", "a", "same"},
        {2, "nonempty_metric", "a", "same"},
    });
    Chunks raw_chunks;
    raw_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 1, 2},
        {0, 1, 0},
        {{}, {{9, 1.0}, {21, 2.0}}, {{9, 3.0}, {10, 4.0}, {10, 5.0}}}));
    raw_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2},
        {1},
        {{{20, 6.0}, {20, 7.0}, {21, 8.0}}}));

    const auto raw_output = pullSingleOutputChunk(makePipeline(
        raw_header,
        std::move(raw_chunks),
        raw_collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/4,
        /*max_output_block_size=*/1024,
        Field{UInt64{10}},
        Field{UInt64{20}}));

    const auto sliced_collector = makeCollector({{2, "nonempty_metric", "a", "same"}});
    Chunks sliced_chunks;
    sliced_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2, 2},
        {0, 1},
        {{{10, 4.0}, {10, 5.0}}, {{20, 6.0}, {20, 7.0}}}));
    const auto sliced_output = pullSingleOutputChunk(makePipeline(
        sliced_header,
        std::move(sliced_chunks),
        sliced_collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/4));

    ASSERT_EQ(raw_output.getNumRows(), 1);
    ASSERT_EQ(sliced_output.getNumRows(), 1);
    EXPECT_EQ(
        raw_output.getColumns().at(1)->compareAt(0, 0, *sliced_output.getColumns().at(1), 1),
        0);
}

TEST(PromQLRangeRateStep, MergesStreamsAndExposesVectorGridHeader)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks first_chunks;
    first_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    Chunks second_chunks;
    second_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {1},
        {{{10, 10.0}, {20, 20.0}}}));

    Pipes pipes;
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_chunks)));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_chunks)));
    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLRangeRateStep step(
        header,
        collector,
        rate_function,
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1);
    ASSERT_EQ(step.getOutputHeader()->columns(), 2);
    EXPECT_EQ(step.getOutputHeader()->getByPosition(0).name, TimeSeriesColumnNames::Group);
    EXPECT_EQ(step.getOutputHeader()->getByPosition(1).name, TimeSeriesColumnNames::Values);
    EXPECT_TRUE(step.getOutputHeader()->getByPosition(1).type->equals(*rate_function->getResultType()));

    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));
    PullingPipelineExecutor executor(pipeline);

    size_t output_rows = 0;
    Chunk output;
    while (executor.pull(output))
    {
        EXPECT_LE(output.getNumRows(), 1);
        output_rows += output.getNumRows();
    }
    EXPECT_EQ(output_rows, 2);
}

TEST(PromQLRangeRateStep, FusedMergeMatchesMaterializedMergeForInterleavedSources)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
        {3, "requests_total", "c", "three"},
        {4, "requests_total", "d", "four"},
    });

    std::vector<Chunks> source_chunks(3);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}}}));
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {2, 4},
        {2, 0},
        {{{20, 40.0}}, {{0, 0.0}, {10, 30.0}}}));

    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {1, 1},
        {{{20, 20.0}}, {{10, 20.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {3},
        {0},
        {{{0, 0.0}, {10, 5.0}}}));

    source_chunks[2].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {1, 2},
        {{{20, 25.0}}, {{30, 60.0}}}));
    source_chunks[2].emplace_back(makeSamplesChunk(
        samples_type,
        {3, 4},
        {1, 1},
        {{{20, 10.0}}, {{20, 60.0}}}));

    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));

    ASSERT_EQ(actual.size(), 4);
    for (const auto & chunk : actual)
        ASSERT_EQ(chunk.getNumRows(), 1);
    expectEquivalentOutput(expected, actual);
}

TEST(PromQLRangeRateStep, FusedBatchMergeResumesAtUnconsumedRowAfterOutputFlush)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
        {3, "requests_total", "c", "three"},
        {4, "requests_total", "d", "four"},
        {5, "requests_total", "e", "five"},
    });

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3, 4},
        {0, 0, 0, 0},
        {{{0, 0.0}, {10, 10.0}},
         {{0, 0.0}, {10, 20.0}},
         {{0, 0.0}, {10, 30.0}},
         {{0, 0.0}, {10, 40.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {5},
        {0},
        {{{0, 0.0}, {10, 50.0}}}));

    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));

    ASSERT_EQ(actual.size(), 5);
    for (const auto & chunk : actual)
        ASSERT_EQ(chunk.getNumRows(), 1);
    expectEquivalentOutput(expected, actual);
}

TEST(PromQLRangeRateStep, FusedMergeAccumulatesAscendingBucketsWithinSeries)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 1},
        {0, 2},
        {{{0, 0.0}}, {{20, 20.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {1},
        {{{10, 10.0}}}));

    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1));

    ASSERT_EQ(actual.size(), 1);
    ASSERT_EQ(actual.front().getNumRows(), 1);
    expectEquivalentOutput(expected, actual);
}

TEST(PromQLRangeRateStep, FusedMergeRejectsDescendingBucketsWithinSource)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});

    std::vector<Chunks> source_chunks(1);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 1},
        {1, 0},
        {{{0, 0.0}}, {{10, 10.0}}}));

    auto pipeline = makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1);
    PullingPipelineExecutor executor(pipeline);
    expectExceptionCode(
        [&]
        {
            Chunk output;
            while (executor.pull(output))
            {
            }
        },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLRangeRateStep, FusedMergeMatchesMaterializedMergeForRawInclusiveSlices)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
        {3, "requests_total", "c", "three"},
    });

    std::vector<Chunks> source_chunks(3);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {1, 1},
        {{{9, 1.0}, {10, 10.0}, {20, 20.0}, {21, 21.0}}, {{9, 1.0}, {10, 5.0}}}));
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {2},
        {1},
        {{{20, 10.0}, {21, 11.0}}}));

    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {1, 1},
        {{{10, 15.0}, {20, 25.0}}, {{10, 15.0}, {20, 25.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {3},
        {1},
        {{{9, 0.0}, {10, 3.0}, {20, 6.0}}}));

    source_chunks[2].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 3},
        {1, 1},
        {{{10, 16.0}, {20, 26.0}}, {{10, 8.0}, {21, 9.0}}}));

    const Field raw_min_time{UInt64{10}};
    const Field raw_max_time{UInt64{20}};
    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));

    ASSERT_EQ(actual.size(), 3);
    for (const auto & chunk : actual)
        ASSERT_EQ(chunk.getNumRows(), 1);
    expectEquivalentOutput(expected, actual);
}

TEST(PromQLRangeRateStep, FusedRawEqualBucketDeduplicatesAcrossSourcesAndChunks)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();

    std::vector<Chunks> source_chunks(3);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, nan}}}));
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{10, 3.0}}, {{0, nan}, {10, 10.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 5.0}, {10, nan}}, {{0, nan}}}));
    source_chunks[2].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{10, 7.0}, {20, 9.0}}, {{10, 20.0}, {20, nan}}}));

    const Field raw_min_time{UInt64{0}};
    const Field raw_max_time{UInt64{20}};
    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));

    ASSERT_EQ(actual.size(), 2);
    for (const auto & chunk : actual)
        ASSERT_EQ(chunk.getNumRows(), 1);
    expectEquivalentOutput(expected, actual);

    for (const auto * output : {&expected, &actual})
    {
        ASSERT_EQ(output->size(), 2);
        const auto & first_values = assert_cast<const ColumnArray &>(*output->at(0).getColumns().at(1));
        const auto & second_values = assert_cast<const ColumnArray &>(*output->at(1).getColumns().at(1));
        checkRateRow(first_values, 0, 0.2, 0.2);
        checkNaNRateRow(second_values, 0);
    }
}

TEST(PromQLRangeRateStep, FusedRawCompactsAcrossInternalBucketsAndPreservesResetBoundary)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});

    /// External bucket 0 spans two rate buckets. External bucket 1 then extends
    /// the second rate bucket and starts with a counter reset.
    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 0.0}, {5, 100.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {1},
        {{{7, 10.0}, {10, 20.0}}}));

    const Field raw_min_time{UInt64{0}};
    const Field raw_max_time{UInt64{20}};
    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));

    expectEquivalentOutput(expected, actual);
    ASSERT_EQ(actual.size(), 1);
    const auto & values = assert_cast<const ColumnArray &>(*actual.front().getColumns().at(1));
    checkRateRow(values, 0, 6.0, 1.5);
}

TEST(PromQLRangeRateStep, FusedRawEmptyRowsDoNotRegisterSeries)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({
        {1, "empty_metric", "a", "same"},
        {2, "nonempty_metric", "a", "same"},
        {3, "other_metric", "b", "other"},
    });

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3},
        {0, 0, 0},
        {{}, {{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3},
        {1, 1, 1},
        {{{21, 1.0}}, {{20, 20.0}}, {{20, 40.0}}}));

    const Field raw_min_time{UInt64{0}};
    const Field raw_max_time{UInt64{20}};
    auto expected = pullAllOutputChunks(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));
    auto actual = pullAllOutputChunks(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        raw_min_time,
        raw_max_time));

    ASSERT_EQ(actual.size(), 2);
    expectEquivalentOutput(expected, actual);
    const auto & first_values = assert_cast<const ColumnArray &>(*actual.at(0).getColumns().at(1));
    const auto & second_values = assert_cast<const ColumnArray &>(*actual.at(1).getColumns().at(1));
    checkRateRow(first_values, 0, 0.5, 1.0);
    checkRateRow(second_values, 0, 1.0, 2.0);
}

TEST(PromQLRangeRateStep, FusedRawLimitCountsPhysicalSamplesBeforeDeduplication)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 0.0}, {10, 10.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 5.0}, {10, 20.0}}}));

    auto pipeline = makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/3,
        /*max_output_block_size=*/1,
        Field{UInt64{0}},
        Field{UInt64{20}});
    PullingPipelineExecutor executor(pipeline);
    expectExceptionCode(
        [&]
        {
            Chunk output;
            while (executor.pull(output))
            {
            }
        },
        ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST(PromQLRangeRateStep, FusedRawFailsClosedOnTimestampOverlapAcrossExternalBuckets)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type, TimeSeriesColumnNames::Samples);
    const auto collector = makeCollector({{1, "requests_total", "a", "one"}});

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{5, 5.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {1},
        {{{5, 7.0}}}));

    auto pipeline = makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRawRateFunction(),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        Field{UInt64{0}},
        Field{UInt64{20}});
    PullingPipelineExecutor executor(pipeline);
    expectExceptionCode(
        [&]
        {
            Chunk output;
            while (executor.pull(output))
            {
            }
        },
        ErrorCodes::INCORRECT_DATA);
}

TEST(PromQLRangeRateStep, ProcessesDisjointIdentifierRangesInParallel)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks first_chunks;
    first_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}}));
    Chunks second_chunks;
    second_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2},
        {0},
        {{{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    Pipes pipes;
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_chunks)));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_chunks)));
    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLRangeRateStep step(
        header,
        collector,
        rate_function,
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1,
        /*parallel_processing_requested=*/true);
    EXPECT_TRUE(step.isParallelProcessingRequested());
    EXPECT_FALSE(step.isParallelProcessingEnabled());
    step.enableParallelProcessing();
    EXPECT_TRUE(step.isParallelProcessingEnabled());

    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));
    PullingPipelineExecutor executor(pipeline);

    std::vector<Float64> final_rates;
    Chunk output;
    while (executor.pull(output))
    {
        const auto & arrays = assert_cast<const ColumnArray &>(*output.getColumns().at(1));
        const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
        const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
        for (size_t row = 0; row < output.getNumRows(); ++row)
        {
            const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
            ASSERT_EQ(arrays.getOffsets()[row] - begin, 3);
            final_rates.push_back(values.getElement(begin + 2));
        }
    }

    std::sort(final_rates.begin(), final_rates.end());
    ASSERT_EQ(final_rates.size(), 2);
    EXPECT_DOUBLE_EQ(final_rates[0], 1.0);
    EXPECT_DOUBLE_EQ(final_rates[1], 2.0);
}

TEST(PromQLRangeRateStep, RejectsDuplicateGroupsAcrossParallelLanes)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks first_chunks;
    first_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 0.0}, {10, 10.0}}}));
    Chunks second_chunks;
    second_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {2},
        {0},
        {{{0, 0.0}, {10, 20.0}}}));

    Pipes pipes;
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_chunks)));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_chunks)));
    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLRangeRateStep step(
        header,
        collector,
        rate_function,
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1,
        /*parallel_processing_requested=*/true);
    step.enableParallelProcessing();

    QueryPipelineBuilders inputs;
    inputs.emplace_back(std::move(builder));
    BuildQueryPipelineSettings settings(getContext().context);
    auto result = step.updatePipeline(std::move(inputs), settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*result));
    PullingPipelineExecutor executor(pipeline);

    expectExceptionCode(
        [&]
        {
            Chunk output;
            while (executor.pull(output))
            {
            }
        },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLRangeRateTransform, CancellationReleasesAnActiveSeriesState)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}}));
    auto pipeline = makePipeline(header, std::move(chunks), collector, rate_function);
    PullingPipelineExecutor executor(pipeline);

    Chunk output;
    ASSERT_TRUE(executor.pull(output));
    executor.cancel();
}

TEST(PromQLRangeRateStep, StreamingRawKernelDeduplicatesOutOfOrderBatchesBeforeCompaction)
{
    Array parameters{UInt64{10}, UInt64{10}, UInt64{0}, UInt64{10}};
    auto rate_function = std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()},
        parameters,
        UInt32{10},
        UInt32{10},
        Int32{0},
        Int32{10},
        0,
        0);
    const auto * streaming = dynamic_cast<const ITimeSeriesRateToGridStreaming *>(rate_function.get());
    ASSERT_NE(streaming, nullptr);
    auto state = streaming->createStreamingState();

    auto timestamps = ColumnUInt32::create();
    auto values = ColumnFloat64::create();
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    for (const auto & [timestamp, value] : Samples{{1, 1.0}, {9, nan}, {5, 5.0}, {9, 9.0}, {10, 10.0}})
    {
        timestamps->insertValue(timestamp);
        values->insertValue(value);
    }

    streaming->addRawSamples(*state, *timestamps, *values, 0, 1);
    streaming->addRawSamples(*state, *timestamps, *values, 1, 5);
    streaming->finishExternalBucket(*state);

    auto result = rate_function->getResultType()->createColumn();
    streaming->insertStreamingResultInto(*state, *result);
    const auto & arrays = assert_cast<const ColumnArray &>(*result);
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & rate_values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());

    ASSERT_EQ(arrays.size(), 1);
    ASSERT_EQ(arrays.getOffsets()[0], 1);
    ASSERT_EQ(nullable.getNullMapData()[0], 0);
    EXPECT_DOUBLE_EQ(rate_values.getElement(0), 1.0);
}

}
