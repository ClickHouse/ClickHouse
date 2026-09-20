#include <gtest/gtest.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
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
#include <initializer_list>
#include <optional>
#include <tuple>
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
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Int32, Float64>>(
        DataTypes{samples_type}, parameters, UInt32{0}, UInt32{20}, Int32{10}, Int32{20}, 0);
}

AggregateFunctionPtr makeRawRateFunction()
{
    Array parameters{UInt64{0}, UInt64{20}, UInt64{10}, UInt64{20}};
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Int32, Float64>>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()},
        parameters,
        UInt32{0},
        UInt32{20},
        Int32{10},
        Int32{20},
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

}
