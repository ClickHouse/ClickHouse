#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <vector>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// A value of the join column: NULL is written as `std::nullopt`.
using Value = std::optional<UInt64>;
using Values = std::vector<Value>;
/// One output row of the join, in (probe value, build value) order.
using JoinedRow = std::pair<Value, Value>;

DataTypePtr valueType(bool nullable)
{
    DataTypePtr type = std::make_shared<DataTypeUInt64>();
    return nullable ? makeNullable(type) : type;
}

SharedHeader makeHeader(const String & name, bool nullable)
{
    Block header;
    header.insert(ColumnWithTypeAndName(valueType(nullable)->createColumn(), valueType(nullable), name));
    return std::make_shared<const Block>(std::move(header));
}

ColumnPtr makeColumn(const Values & values, bool nullable)
{
    auto column = valueType(nullable)->createColumn();
    for (const auto & value : values)
    {
        if (value)
            column->insert(Field(*value));
        else
            column->insert(Field());
    }
    return column;
}

Value valueAt(const IColumn & column, size_t row)
{
    if (column.isNullAt(row))
        return std::nullopt;
    return column.getUInt(row);
}

/// The whole `ON` condition as the operator sees it: `probe <op> build` over the two input headers.
BlockNestedLoopPredicate makePredicate(const String & function_name, bool nullable)
{
    tryRegisterFunctions();

    ActionsDAG dag(NamesAndTypesList{{"probe", valueType(nullable)}, {"build", valueType(nullable)}});
    const auto & function = FunctionFactory::instance().get(function_name, getContext().context);
    const auto & condition = dag.addFunction(function, {dag.getInputs()[0], dag.getInputs()[1]}, "condition");
    dag.getOutputs() = {&condition};

    BlockNestedLoopPredicate predicate;
    predicate.actions = std::make_shared<ExpressionActions>(std::move(dag));
    predicate.inputs = {{.side = 0, .position = 0}, {.side = 1, .position = 0}};
    return predicate;
}

/// The join's own output header: the probe column followed by the build column.
SharedHeader makeOutputHeader(const SharedHeader & probe_header, const SharedHeader & build_header)
{
    Block output_block;
    output_block.insert(probe_header->getByPosition(0));
    output_block.insert(build_header->getByPosition(0));
    return std::make_shared<const Block>(std::move(output_block));
}

BlockNestedLoopJoinDataPtr makeData(
    const std::vector<Values> & build_blocks,
    const SharedHeader & build_header,
    JoinKind kind,
    JoinStrictness strictness,
    bool nullable)
{
    auto data = std::make_shared<BlockNestedLoopJoinData>(build_header, kind, strictness, SizeLimits{});
    for (const auto & values : build_blocks)
    {
        Block block;
        block.insert(ColumnWithTypeAndName(makeColumn(values, nullable), valueType(nullable), "build"));
        data->addBlock(std::move(block), values.size());
    }
    data->finish();
    return data;
}

struct ProbeResult
{
    std::vector<JoinedRow> rows;
    /// The size of every output chunk, in order.
    std::vector<size_t> chunk_sizes;
};

ProbeResult runProbe(
    const std::vector<Values> & build_blocks,
    const std::vector<Values> & probe_chunks,
    JoinKind kind = JoinKind::Inner,
    JoinStrictness strictness = JoinStrictness::All,
    size_t max_block_size = 0,
    const String & function_name = "less",
    bool nullable = false)
{
    auto probe_header = makeHeader("probe", nullable);
    auto build_header = makeHeader("build", nullable);
    auto output_header = makeOutputHeader(probe_header, build_header);
    auto data = makeData(build_blocks, build_header, kind, strictness, nullable);

    Chunks chunks;
    for (const auto & values : probe_chunks)
        chunks.emplace_back(Columns{makeColumn(values, nullable)}, values.size());

    auto source = std::make_shared<SourceFromChunks>(probe_header, std::move(chunks));
    auto probe = std::make_shared<BlockNestedLoopProbeTransform>(
        probe_header, output_header, data, makePredicate(function_name, nullable), max_block_size, 0);

    connect(source->getPort(), probe->getInputs().front());

    auto * output_port = &probe->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(probe));

    if (keepsUnmatchedBuildRows(kind, strictness))
    {
        /// The same wiring the step builds: the probe stream is the main port of `DelayedPorts` and
        /// the scan over the stored blocks its delayed one, so the scan starts only once the probe
        /// stream is done and every match flag is set.
        VectorWithMemoryTracking<UInt64> delayed_ports;
        delayed_ports.push_back(1);
        auto delayed = std::make_shared<DelayedPortsProcessor>(output_header, 2, delayed_ports);
        auto unmatched = std::make_shared<BlockNestedLoopUnmatchedBuildRowsTransform>(
            output_header, data, max_block_size, 0, 1);

        auto next_input = delayed->getInputs().begin();
        connect(*output_port, *next_input++);
        connect(unmatched->getPort(), *next_input++);

        auto resize = std::make_shared<ResizeProcessor>(output_header, 2, 1);
        auto next_resize_input = resize->getInputs().begin();
        for (auto & port : delayed->getOutputs())
            connect(port, *next_resize_input++);

        output_port = &resize->getOutputs().front();
        processors->emplace_back(std::move(unmatched));
        processors->emplace_back(std::move(delayed));
        processors->emplace_back(std::move(resize));
    }

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);

    ProbeResult result;
    PullingPipelineExecutor executor(pipeline);
    Chunk chunk;
    while (executor.pull(chunk))
    {
        if (chunk.getNumRows() == 0)
            continue;
        result.chunk_sizes.push_back(chunk.getNumRows());
        const auto & columns = chunk.getColumns();
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
            result.rows.emplace_back(valueAt(*columns[0], row), valueAt(*columns[1], row));
    }
    return result;
}

/// The build values each stream of the unmatched scan emits, given the build rows the probe flagged.
std::vector<std::vector<UInt64>> runUnmatchedScan(
    const std::vector<Values> & build_blocks, const std::vector<size_t> & matched_rows, size_t num_streams)
{
    auto probe_header = makeHeader("probe", /*nullable=*/ false);
    auto build_header = makeHeader("build", /*nullable=*/ false);
    auto output_header = makeOutputHeader(probe_header, build_header);
    auto data = makeData(build_blocks, build_header, JoinKind::Right, JoinStrictness::All, /*nullable=*/ false);

    for (auto row : matched_rows)
        data->setBuildRowMatched(row);

    std::vector<std::vector<UInt64>> per_stream(num_streams);
    for (size_t stream_index = 0; stream_index < num_streams; ++stream_index)
    {
        auto unmatched = std::make_shared<BlockNestedLoopUnmatchedBuildRowsTransform>(
            output_header, data, /*max_block_size=*/ 0, stream_index, num_streams);
        auto * output_port = &unmatched->getPort();
        auto processors = std::make_shared<Processors>();
        processors->emplace_back(std::move(unmatched));
        QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);

        PullingPipelineExecutor executor(pipeline);
        Chunk chunk;
        while (executor.pull(chunk))
        {
            const auto & columns = chunk.getColumns();
            for (size_t row = 0; row < chunk.getNumRows(); ++row)
            {
                /// The probe side of an unmatched build row is padded with the column's default.
                EXPECT_EQ(columns[0]->getUInt(row), 0);
                per_stream[stream_index].push_back(columns[1]->getUInt(row));
            }
        }
    }
    return per_stream;
}

std::vector<JoinedRow> sorted(std::vector<JoinedRow> rows)
{
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

TEST(BlockNestedLoopJoinProbe, EnumeratesEveryMatchingPair)
{
    /// Every (probe, build) pair with probe < build, over a build side split into three blocks.
    auto result = runProbe({{2}, {3, 5}, {1}}, {{1, 3, 4}});

    EXPECT_EQ(sorted(result.rows), sorted({{1, 2}, {1, 3}, {1, 5}, {3, 5}, {4, 5}}));
}

TEST(BlockNestedLoopJoinProbe, EvaluatesEveryProbeChunkAgainstTheWholeBuildSide)
{
    auto result = runProbe({{2}, {3}}, {{1}, {2}, {4}});

    EXPECT_EQ(sorted(result.rows), sorted({{1, 2}, {1, 3}, {2, 3}}));
}

TEST(BlockNestedLoopJoinProbe, AnAlwaysFalseConditionProducesNothingForInner)
{
    EXPECT_TRUE(runProbe({{1, 2, 3}}, {{4, 5}}).rows.empty());
}

TEST(BlockNestedLoopJoinProbe, EmptyBuildSideProducesNothingForInner)
{
    EXPECT_TRUE(runProbe({}, {{1, 2}}).rows.empty());
}

TEST(BlockNestedLoopJoinProbe, EmptyProbeSideProducesNothing)
{
    EXPECT_TRUE(runProbe({{1, 2}}, {}).rows.empty());
    EXPECT_TRUE(runProbe({{1, 2}}, {{}}).rows.empty());
    EXPECT_TRUE(runProbe({{1, 2}}, {{}}, JoinKind::Left).rows.empty());
}

TEST(BlockNestedLoopJoinProbe, NullConditionValueIsNotAMatch)
{
    /// `probe < build` is NULL wherever either side is NULL, and NULL is not a match.
    auto result = runProbe(
        {{2, std::nullopt}}, {{1, std::nullopt, 3}},
        JoinKind::Inner, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(1), Value(2)}}));
}

TEST(BlockNestedLoopJoinProbe, LeftKeepsUnmatchedProbeRowsPadded)
{
    auto result = runProbe(
        {{2}, {5}}, {{1, 4, 9}},
        JoinKind::Left, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(1), Value(2)}, {Value(1), Value(5)}, {Value(4), Value(5)}, {Value(9), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, LeftWithAnEmptyBuildSidePadsEveryProbeRow)
{
    auto result = runProbe(
        {}, {{1, 2}},
        JoinKind::Left, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(1), std::nullopt}, {Value(2), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, LeftWithAnAlwaysFalseConditionPadsEveryProbeRow)
{
    auto result = runProbe(
        {{1}, {2}}, {{5, 6}},
        JoinKind::Left, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(5), std::nullopt}, {Value(6), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, OutputIsCutToTheBlockSizeLimit)
{
    /// 4 probe rows against 5 build rows, all pairs matching: 20 output rows in chunks of 3.
    /// A single probe chunk therefore spans several `work` calls, and the accumulated pairs of one
    /// call are emitted over several of them.
    std::vector<Values> build_blocks{{10, 11}, {12, 13, 14}};
    auto result = runProbe(build_blocks, {{1, 2, 3, 4}}, JoinKind::Inner, JoinStrictness::All, /*max_block_size=*/ 3);

    EXPECT_EQ(result.rows.size(), 20);
    for (auto chunk_size : result.chunk_sizes)
        EXPECT_LE(chunk_size, 3);
    EXPECT_GE(result.chunk_sizes.size(), 7);

    std::vector<JoinedRow> expected;
    for (UInt64 probe = 1; probe <= 4; ++probe)
        for (UInt64 build = 10; build <= 14; ++build)
            expected.emplace_back(probe, build);
    EXPECT_EQ(sorted(result.rows), sorted(expected));
}

TEST(BlockNestedLoopJoinProbe, UnmatchedProbeRowsAreCutToTheBlockSizeLimit)
{
    auto result = runProbe(
        {{1}}, {{5, 6, 7, 8, 9}},
        JoinKind::Left, JoinStrictness::All, /*max_block_size=*/ 2, "less", /*nullable=*/ true);

    EXPECT_EQ(result.rows.size(), 5);
    for (auto chunk_size : result.chunk_sizes)
        EXPECT_LE(chunk_size, 2);
}

TEST(BlockNestedLoopJoinProbe, AnOutputChunkCanSpanSeveralStoredBlocks)
{
    /// Each build block holds one row, so an output chunk of four rows is gathered from four
    /// different stored blocks.
    auto result = runProbe({{10}, {11}, {12}, {13}}, {{1}}, JoinKind::Inner, JoinStrictness::All, /*max_block_size=*/ 4);

    ASSERT_EQ(result.chunk_sizes.size(), 1);
    EXPECT_EQ(result.chunk_sizes[0], 4);
    EXPECT_EQ(sorted(result.rows), sorted({{1, 10}, {1, 11}, {1, 12}, {1, 13}}));
}

TEST(BlockNestedLoopJoinProbe, RightKeepsUnmatchedBuildRowsPadded)
{
    /// `probe < build` over probe {4, 9} and build {2, 5, 7}: only 5 and 7 are matched, so 2 is the
    /// build row the scan after the probe emits. The probe row 9 matches nothing and is dropped.
    auto result = runProbe(
        {{2, 5}, {7}}, {{4, 9}},
        JoinKind::Right, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(4), Value(5)}, {Value(4), Value(7)}, {std::nullopt, Value(2)}}));
}

TEST(BlockNestedLoopJoinProbe, FullKeepsUnmatchedRowsOfBothSides)
{
    auto result = runProbe(
        {{2, 5}, {7}}, {{4, 9}},
        JoinKind::Full, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({
        {Value(4), Value(5)}, {Value(4), Value(7)}, {std::nullopt, Value(2)}, {Value(9), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, RightWithAnEmptyProbeSideEmitsEveryBuildRow)
{
    const std::vector<JoinedRow> expected{{std::nullopt, Value(2)}, {std::nullopt, Value(5)}, {std::nullopt, Value(7)}};

    /// No probe chunk at all, ...
    auto without_chunks = runProbe(
        {{2}, {5, 7}}, {},
        JoinKind::Right, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);
    EXPECT_EQ(sorted(without_chunks.rows), sorted(expected));

    /// ... and one chunk with no rows.
    auto with_an_empty_chunk = runProbe(
        {{2}, {5, 7}}, {{}},
        JoinKind::Right, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);
    EXPECT_EQ(sorted(with_an_empty_chunk.rows), sorted(expected));
}

TEST(BlockNestedLoopJoinProbe, RightWithAnAlwaysFalseConditionEmitsEveryBuildRow)
{
    auto result = runProbe(
        {{1}, {2}}, {{5, 6}},
        JoinKind::Right, JoinStrictness::All, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{std::nullopt, Value(1)}, {std::nullopt, Value(2)}}));
}

TEST(BlockNestedLoopJoinProbe, RightWithAnEmptyBuildSideProducesNothing)
{
    EXPECT_TRUE(runProbe({}, {{1, 2}}, JoinKind::Right).rows.empty());
}

TEST(BlockNestedLoopJoinProbe, UnmatchedBuildRowsAreCutToTheBlockSizeLimit)
{
    /// One build block of five rows, none of them matched, emitted in chunks of two.
    auto result = runProbe(
        {{5, 6, 7, 8, 9}}, {{20}},
        JoinKind::Right, JoinStrictness::All, /*max_block_size=*/ 2, "less", /*nullable=*/ true);

    EXPECT_EQ(result.rows.size(), 5);
    for (auto chunk_size : result.chunk_sizes)
        EXPECT_LE(chunk_size, 2);
}

TEST(BlockNestedLoopJoinProbe, TheUnmatchedScanPartitionsTheBuildBlocksOverStreams)
{
    /// Five blocks, and the rows 1, 4 and 7 flagged as matched by the probe.
    const std::vector<Values> build_blocks{{10, 11}, {12, 13}, {14, 15}, {16, 17}, {18, 19}};
    const std::vector<size_t> matched_rows{1, 4, 7};
    const std::vector<UInt64> expected{10, 12, 13, 15, 16, 18, 19};

    for (size_t num_streams : std::vector<size_t>{1, 2, 3, 8})
    {
        auto per_stream = runUnmatchedScan(build_blocks, matched_rows, num_streams);

        std::vector<UInt64> all;
        for (const auto & stream_rows : per_stream)
            all.insert(all.end(), stream_rows.begin(), stream_rows.end());
        std::sort(all.begin(), all.end());

        /// Every unmatched row is emitted exactly once: the streams cover the build side and do not
        /// overlap, whatever the ratio of streams to blocks.
        EXPECT_EQ(all, expected) << "with " << num_streams << " streams";
    }

    /// Blocks are dealt out one by one, so with as many streams as blocks each takes exactly one.
    auto per_stream = runUnmatchedScan(build_blocks, matched_rows, build_blocks.size());
    EXPECT_EQ(per_stream[0], (std::vector<UInt64>{10}));
    EXPECT_EQ(per_stream[1], (std::vector<UInt64>{12, 13}));
    EXPECT_EQ(per_stream[2], (std::vector<UInt64>{15}));
    EXPECT_EQ(per_stream[3], (std::vector<UInt64>{16}));
    EXPECT_EQ(per_stream[4], (std::vector<UInt64>{18, 19}));
}

TEST(BlockNestedLoopJoinProbe, UnsupportedJoinTypesAreRejected)
{
    EXPECT_THROW(runProbe({{1}}, {{2}}, JoinKind::Left, JoinStrictness::Any), Exception);
    EXPECT_THROW(runProbe({{1}}, {{2}}, JoinKind::Left, JoinStrictness::Semi), Exception);
    EXPECT_THROW(runProbe({{1}}, {{2}}, JoinKind::Left, JoinStrictness::Anti), Exception);
}
