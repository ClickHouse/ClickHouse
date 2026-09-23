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
#include <Processors/Merges/PromQLTwoRangeRatesMergingTransform.h>
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
    return std::make_shared<AggregateFunctionTimeseriesRateToGrid<UInt32, Float64>>(
        DataTypes{samples_type}, parameters, UInt32{0}, UInt32{20}, Int32{10}, Int32{20}, 0, 0);
}

QueryPipeline makePipeline(
    const SharedHeader & header,
    Chunks chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series = 1024,
    size_t max_output_block_size = 1024,
    size_t max_join_groups = 1024,
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
        max_join_groups,
        max_grid_cells);
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
    size_t max_join_groups,
    size_t max_grid_cells)
{
    const size_t num_inputs = source_chunks.size();
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
        num_inputs,
        description,
        max_output_block_size,
        /*max_block_size_bytes=*/0,
        /*max_dynamic_subcolumns=*/std::nullopt,
        SortingQueueStrategy::Batch));
    builder->addSimpleTransform(
        [collector, rate_function, max_samples_per_series, max_output_block_size, max_join_groups, max_grid_cells](
            const SharedHeader & transformed_header)
        {
            return std::make_shared<PromQLTwoRangeRatesTransform>(
                transformed_header,
                collector,
                rate_function,
                "requests_total",
                "errors_total",
                max_samples_per_series,
                max_output_block_size,
                max_join_groups,
                max_grid_cells);
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
    size_t max_join_groups,
    size_t max_grid_cells)
{
    const size_t num_inputs = source_chunks.size();
    Pipes pipes;
    for (auto & chunks : source_chunks)
        pipes.emplace_back(std::make_shared<ChunksSource>(header, std::move(chunks)));

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));

    auto config = std::make_shared<const PromQLTwoRangeRatesFusionConfig>(
        collector,
        rate_function,
        "requests_total",
        "errors_total",
        max_samples_per_series,
        max_output_block_size,
        max_join_groups,
        max_grid_cells,
        std::nullopt,
        std::nullopt,
        PromQLTwoRangeRatesTransform::transformHeader(rate_function));
    auto group_state = std::make_shared<PromQLTwoRangeRatesGroupState>(max_join_groups, max_grid_cells);
    builder->addTransform(
        std::make_shared<PromQLTwoRangeRatesMergingTransform>(header, num_inputs, std::move(config), std::move(group_state)));

    return QueryPipelineBuilder::getPipeline(std::move(*builder));
}

QueryPipeline makeLayeredFusedPipeline(
    const SharedHeader & header,
    std::vector<Chunks> layer_chunks,
    const std::shared_ptr<Collector> & collector,
    const AggregateFunctionPtr & rate_function,
    size_t max_samples_per_series,
    size_t max_output_block_size,
    size_t max_join_groups,
    size_t max_grid_cells)
{
    auto config = std::make_shared<const PromQLTwoRangeRatesFusionConfig>(
        collector,
        rate_function,
        "requests_total",
        "errors_total",
        max_samples_per_series,
        max_output_block_size,
        max_join_groups,
        max_grid_cells,
        std::nullopt,
        std::nullopt,
        PromQLTwoRangeRatesTransform::transformHeader(rate_function));
    auto group_state = std::make_shared<PromQLTwoRangeRatesGroupState>(max_join_groups, max_grid_cells);

    Pipes pipes;
    for (auto & chunks : layer_chunks)
    {
        Pipe pipe(std::make_shared<ChunksSource>(header, std::move(chunks)));
        pipe.addTransform(std::make_shared<PromQLTwoRangeRatesMergingTransform>(
            header, /*num_inputs=*/1, config, group_state));
        pipes.emplace_back(std::move(pipe));
    }

    auto result = Pipe::unitePipes(std::move(pipes));
    result.resize(1);
    return QueryPipeline(std::move(result));
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

std::vector<Chunk> pullAll(QueryPipeline pipeline)
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

std::vector<std::pair<UInt64, Float64>> collectFinalRates(const std::vector<Chunk> & chunks)
{
    std::vector<std::pair<UInt64, Float64>> result;
    for (const auto & chunk : chunks)
    {
        const auto & groups = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().at(0));
        const auto & arrays = assert_cast<const ColumnArray &>(*chunk.getColumns().at(1));
        const auto & nullable = assert_cast<const ColumnNullable &>(arrays.getData());
        const auto & values = assert_cast<const ColumnFloat64 &>(nullable.getNestedColumn());
        const auto & null_map = nullable.getNullMapData();

        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            const size_t begin = row == 0 ? 0 : arrays.getOffsets()[row - 1];
            const size_t end = arrays.getOffsets()[row];
            if (end - begin != 3)
            {
                ADD_FAILURE() << "expected three grid cells, got " << (end - begin);
                continue;
            }
            if (null_map[begin + 2])
            {
                ADD_FAILURE() << "expected a non-null final grid cell";
                continue;
            }
            result.emplace_back(groups.getElement(row), values.getElement(begin + 2));
        }
    }
    std::sort(result.begin(), result.end());
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
    PromQLTwoRangeRatesGroupState state(/*max_join_groups=*/2, /*max_grid_cells=*/6);

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

TEST(PromQLTwoRangeRatesMergingTransform, MatchesMaterializedPathForInterleavedMetricSidesAcrossStreams)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "b", "two"},
        {3, "errors_total", "a", "one"},
        {4, "requests_total", "b", "two"},
    });

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {0, 1},
        {{{0, 0.0}}, {{10, 40.0}, {20, 80.0}}}));
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {3, 4},
        {0, 1},
        {{{0, 0.0}, {10, 20.0}}, {{20, 60.0}}}));

    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 2},
        {1, 0},
        {{{10, 10.0}, {20, 20.0}}, {{0, 0.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {3, 4},
        {1, 0},
        {{{20, 40.0}}, {{0, 0.0}, {10, 30.0}}}));

    auto expected = pullAll(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        /*max_join_groups=*/16,
        /*max_grid_cells=*/64));
    auto actual = pullAll(makeFusedMergePipeline(
        header,
        std::move(source_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/1,
        /*max_join_groups=*/16,
        /*max_grid_cells=*/64));

    ASSERT_EQ(actual.size(), 2);
    for (const auto & chunk : actual)
        ASSERT_EQ(chunk.getNumRows(), 1);
    expectEquivalentOutput(expected, actual);
}

TEST(PromQLTwoRangeRatesMergingTransform, PreservesMatchesAcrossIndependentLayerPipesAfterResize)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "a", "one"},
        {3, "requests_total", "b", "two"},
        {4, "errors_total", "b", "two"},
        {5, "requests_total", "c", "three"},
        {6, "errors_total", "c", "three"},
    });

    std::vector<Chunks> layer_chunks(2);
    layer_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1, 3, 5},
        {0, 0, 0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}},
         {{0, 0.0}, {10, 30.0}, {20, 60.0}},
         {{0, 0.0}, {10, 50.0}, {20, 100.0}}}));
    layer_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {2, 4, 6},
        {0, 0, 0},
        {{{0, 0.0}, {10, 20.0}, {20, 40.0}},
         {{0, 0.0}, {10, 40.0}, {20, 80.0}},
         {{0, 0.0}, {10, 60.0}, {20, 120.0}}}));

    const auto expected = pullAll(makeMaterializedMergePipeline(
        header,
        cloneSourceChunks(layer_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/16,
        /*max_join_groups=*/16,
        /*max_grid_cells=*/64));
    const auto actual = pullAll(makeLayeredFusedPipeline(
        header,
        std::move(layer_chunks),
        collector,
        makeRateFunction(samples_type),
        /*max_samples_per_series=*/16,
        /*max_output_block_size=*/16,
        /*max_join_groups=*/16,
        /*max_grid_cells=*/64));

    const auto expected_rates = collectFinalRates(expected);
    const auto actual_rates = collectFinalRates(actual);
    ASSERT_EQ(expected_rates.size(), 3);
    ASSERT_EQ(actual_rates.size(), expected_rates.size());
    for (size_t row = 0; row < expected_rates.size(); ++row)
    {
        EXPECT_EQ(actual_rates[row].first, expected_rates[row].first);
        EXPECT_DOUBLE_EQ(actual_rates[row].second, expected_rates[row].second);
    }
}

TEST(PromQLTwoRangeRatesMergingTransform, BoundsPendingGroupsAcrossStreams)
{
    const auto samples_type = makeSamplesType();
    const auto header = makeInputHeader(samples_type);
    const auto collector = makeCollector({
        {1, "requests_total", "a", "one"},
        {2, "errors_total", "b", "two"},
    });

    std::vector<Chunks> source_chunks(2);
    source_chunks[0].emplace_back(makeSamplesChunk(
        samples_type,
        {1},
        {0},
        {{{0, 0.0}, {10, 10.0}, {20, 20.0}}}));
    source_chunks[1].emplace_back(makeSamplesChunk(
        samples_type,
        {2},
        {0},
        {{{0, 0.0}, {10, 20.0}, {20, 40.0}}}));

    expectExceptionCode(
        [&]
        {
            pullAll(makeFusedMergePipeline(
                header,
                std::move(source_chunks),
                collector,
                makeRateFunction(samples_type),
                /*max_samples_per_series=*/16,
                /*max_output_block_size=*/1,
                /*max_join_groups=*/1,
                /*max_grid_cells=*/64));
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
        /*max_join_groups=*/1024,
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
        /*max_join_groups=*/1024,
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
