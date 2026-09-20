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
#include <Processors/QueryPlan/PromQLTwoRangeRatesStep.h>
#include <Processors/Transforms/PromQLTwoRangeRatesTransform.h>
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

    String getName() const override { return "PromQLTwoRangeRatesChunksSource"; }

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
    auto bucket_type = std::make_shared<DataTypeUInt32>();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(id_type->createColumn(), id_type, TimeSeriesColumnNames::ID),
        ColumnWithTypeAndName(bucket_type->createColumn(), bucket_type, TimeSeriesColumnNames::Bucket),
        ColumnWithTypeAndName(samples_type->createColumn(), samples_type, TimeSeriesColumnNames::TimeSeries),
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

QueryPipeline makePipeline(
    const SharedHeader & header,
    Chunks chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series = 1024,
    size_t max_output_block_size = 1024,
    size_t max_output_groups = 1024,
    size_t max_grid_cells = 1024)
{
    auto source = std::make_shared<ChunksSource>(header, std::move(chunks));
    auto transform = std::make_shared<PromQLTwoRangeRatesTransform>(
        header,
        collector,
        rate_function,
        "requests_total",
        "errors_total",
        max_samples_per_series,
        max_output_block_size,
        max_output_groups,
        max_grid_cells);
    Pipe pipe(source);
    pipe.addTransform(transform);
    return QueryPipeline(std::move(pipe));
}

std::vector<Chunk> pullAll(QueryPipeline pipeline)
{
    PullingPipelineExecutor executor(pipeline);
    std::vector<Chunk> result;
    Chunk output;
    while (executor.pull(output))
        result.emplace_back(std::move(output));
    return result;
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

}

TEST(PromQLTwoRangeRatesGroupState, MatchingSideReplacesPendingGridWithinLimit)
{
    PromQLTwoRangeRatesGroupState state(/*max_output_groups=*/2, /*max_grid_cells=*/6);

    MutableColumnPtr first_a = ColumnUInt64::create();
    MutableColumnPtr first_b = ColumnUInt64::create();
    MutableColumnPtr second_a = ColumnUInt64::create();
    MutableColumnPtr second_b = ColumnUInt64::create();

    EXPECT_FALSE(state.add(/*join_group=*/10, /*side=*/0, "requests_total", first_a, /*grid_cells=*/3));
    EXPECT_FALSE(state.add(/*join_group=*/20, /*side=*/0, "requests_total", first_b, /*grid_cells=*/3));

    auto first_match = state.add(/*join_group=*/10, /*side=*/1, "errors_total", second_a, /*grid_cells=*/3);
    ASSERT_TRUE(first_match);
    EXPECT_EQ(first_match->group, 10);

    auto second_match = state.add(/*join_group=*/20, /*side=*/1, "errors_total", second_b, /*grid_cells=*/3);
    ASSERT_TRUE(second_match);
    EXPECT_EQ(second_match->group, 20);
}

TEST(PromQLTwoRangeRatesTransform, MatchesReversedMetricOrderAndEmitsAddedGrid)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "errors_total", "a", "one"},
        {2, "requests_total", "a", "one"},
        {3, "requests_total", "b", "two"},
        {4, "errors_total", "b", "two"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3, 4},
        {0, 0, 0, 0},
        {{{0, 0.0}, {10, 20.0}, {20, 40.0}},
         {{0, 0.0}, {10, 10.0}, {20, 20.0}},
         {{0, 0.0}, {10, 30.0}, {20, 60.0}},
         {{0, 0.0}, {10, 40.0}, {20, 80.0}}}));

    const auto output = pullAll(makePipeline(header, std::move(chunks), collector, makeRateFunction(samples_type)));
    ASSERT_EQ(output.size(), 2);
    ASSERT_EQ(output[0].getNumRows(), 1);
    ASSERT_EQ(output[1].getNumRows(), 1);
    const auto & first_arrays = assert_cast<const ColumnArray &>(*output[0].getColumns().at(1));
    const auto & first_nullable = assert_cast<const ColumnNullable &>(first_arrays.getData());
    const auto & first_values = assert_cast<const ColumnFloat64 &>(first_nullable.getNestedColumn());
    const auto & second_arrays = assert_cast<const ColumnArray &>(*output[1].getColumns().at(1));
    const auto & second_nullable = assert_cast<const ColumnNullable &>(second_arrays.getData());
    const auto & second_values = assert_cast<const ColumnFloat64 &>(second_nullable.getNestedColumn());
    EXPECT_DOUBLE_EQ(first_values.getElement(2), 3.0);
    EXPECT_DOUBLE_EQ(second_values.getElement(2), 7.0);
}

TEST(PromQLTwoRangeRatesTransform, PropagatesNullWhenOneSideHasInsufficientSamples)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    const auto output = pullAll(makePipeline(header, std::move(chunks), collector, makeRateFunction(samples_type)));
    ASSERT_EQ(output.size(), 1);
    const auto & arrays = assert_cast<const ColumnArray &>(*output.front().getColumns().at(1));
    const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
    const auto & null_map = nullable.getNullMapData();
    ASSERT_EQ(arrays.getSize(0), 3);
    EXPECT_EQ(null_map[0], 1);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 0);
}

TEST(PromQLTwoRangeRatesTransform, HandlesInterleavedJoinGroups)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
        {3, "errors_total", "a", "one"},
        {4, "errors_total", "b", "two"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3, 4},
        {0, 0, 0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}},
         {{0, 0.0}, {10, 30.0}, {20, 60.0}},
         {{0, 0.0}, {10, 20.0}, {20, 40.0}},
         {{0, 0.0}, {10, 40.0}, {20, 80.0}}}));

    const auto output = pullAll(makePipeline(header, std::move(chunks), collector, makeRateFunction(samples_type)));
    ASSERT_EQ(output.size(), 2);
    ASSERT_EQ(output[0].getNumRows(), 1);
    ASSERT_EQ(output[1].getNumRows(), 1);
    const auto & first_arrays = assert_cast<const ColumnArray &>(*output[0].getColumns().at(1));
    const auto & first_nullable = assert_cast<const ColumnNullable &>(first_arrays.getData());
    const auto & first_values = assert_cast<const ColumnFloat64 &>(first_nullable.getNestedColumn());
    const auto & second_arrays = assert_cast<const ColumnArray &>(*output[1].getColumns().at(1));
    const auto & second_nullable = assert_cast<const ColumnNullable &>(second_arrays.getData());
    const auto & second_values = assert_cast<const ColumnFloat64 &>(second_nullable.getNestedColumn());
    EXPECT_DOUBLE_EQ(first_values.getElement(2), 3.0);
    EXPECT_DOUBLE_EQ(second_values.getElement(2), 7.0);
}

TEST(PromQLTwoRangeRatesTransform, DropsOrphanGroupsAtEndOfInput)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "b", "two"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    const auto output = pullAll(makePipeline(header, std::move(chunks), collector, makeRateFunction(samples_type)));
    EXPECT_TRUE(output.empty());
}

TEST(PromQLTwoRangeRatesTransform, RejectsDuplicateMetricSideForOneJoinGroup)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "a", "one"},
        {3, "errors_total", "a", "one"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3},
        {0, 0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}, {{0, 0.0}, {10, 30.0}}}));

    expectExceptionCode(
        [&]
        {
            pullAll(makePipeline(header, std::move(chunks), collector, makeRateFunction(samples_type)));
        },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
}

TEST(PromQLTwoRangeRatesTransform, BoundsDistinctJoinGroups)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
        {3, "requests_total", "b", "two"},
        {4, "errors_total", "b", "two"},
    });
    Chunks chunks;
    chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2, 3, 4},
        {0, 0, 0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}, {{0, 0.0}, {10, 30.0}}, {{0, 0.0}, {10, 40.0}}}));

    expectExceptionCode(
        [&]
        {
            pullAll(makePipeline(
                header,
                std::move(chunks),
                collector,
                makeRateFunction(samples_type),
                1024,
                1024,
                1));
        },
        ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST(PromQLTwoRangeRatesTransform, BoundsGridCellsAndSamples)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
    });

    Chunks grid_chunks;
    grid_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    expectExceptionCode(
        [&]
        {
            pullAll(makePipeline(
                header,
                std::move(grid_chunks),
                collector,
                makeRateFunction(samples_type),
                1024,
                1024,
                1024,
                2));
        },
        ErrorCodes::TOO_MANY_ROWS_OR_BYTES);

    const auto sample_collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
    });
    Chunks sample_chunks;
    sample_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 20.0}, {20, 40.0}}}));
    expectExceptionCode(
        [&]
        {
            pullAll(makePipeline(
                header,
                std::move(sample_chunks),
                sample_collector,
                makeRateFunction(samples_type),
                2));
        },
        ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

TEST(PromQLTwoRangeRatesStep, MatchesMetricSidesAcrossParallelIdentifierRanges)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "requests_total", "b", "two"},
        {3, "errors_total", "a", "one"},
        {4, "errors_total", "b", "two"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks first_chunks;
    first_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}, {{0, 0.0}, {10, 30.0}, {20, 60.0}}}));
    Chunks second_chunks;
    second_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {3, 4},
        {0, 0},
        {{{0, 0.0}, {10, 20.0}, {20, 40.0}}, {{0, 0.0}, {10, 40.0}, {20, 80.0}}}));

    Pipes pipes;
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_chunks)));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_chunks)));
    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLTwoRangeRatesStep step(
        header,
        collector,
        rate_function,
        "requests_total",
        "errors_total",
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1,
        /*max_output_groups=*/1024,
        /*max_grid_cells=*/1024,
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
        EXPECT_LE(output.getNumRows(), 1);
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
    EXPECT_DOUBLE_EQ(final_rates[0], 3.0);
    EXPECT_DOUBLE_EQ(final_rates[1], 7.0);
}

TEST(PromQLTwoRangeRatesStep, RejectsDuplicateMetricSideAcrossParallelIdentifierRanges)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
        {3, "requests_total", "a", "one"},
    });
    const auto rate_function = makeRateFunction(samples_type);

    Chunks first_chunks;
    first_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 0},
        {{{0, 0.0}, {10, 10.0}}, {{0, 0.0}, {10, 20.0}}}));
    Chunks second_chunks;
    second_chunks.emplace_back(makeSamplesChunk(
        samples_type,
        {3},
        {0},
        {{{0, 0.0}, {10, 30.0}}}));

    Pipes pipes;
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(first_chunks)));
    pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(second_chunks)));
    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    PromQLTwoRangeRatesStep step(
        header,
        collector,
        rate_function,
        "requests_total",
        "errors_total",
        /*max_samples_per_series=*/1024,
        /*max_output_block_size=*/1,
        /*max_output_groups=*/1024,
        /*max_grid_cells=*/1024,
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

}
