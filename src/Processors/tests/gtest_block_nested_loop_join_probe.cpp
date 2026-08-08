#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <vector>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Poco/TemporaryFile.h>

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

using PairCounter = std::shared_ptr<std::atomic<size_t>>;

/// `probe < build`, counting the candidate pairs the condition is evaluated on. A `NULL` operand
/// makes the pair no match, which is what the operator makes of a `NULL` condition value anyway.
class CountingLess final : public IFunction
{
public:
    explicit CountingLess(PairCounter counter_) : counter(std::move(counter_)) {}

    String getName() const override { return "countingLess"; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        counter->fetch_add(input_rows_count);

        auto result = ColumnUInt8::create(input_rows_count);
        auto & values = result->getData();
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            auto left = valueAt(*arguments[0].column, row);
            auto right = valueAt(*arguments[1].column, row);
            values[row] = left && right && *left < *right;
        }
        return result;
    }

private:
    PairCounter counter;
};

/// The whole `ON` condition as the operator sees it: `probe <op> build` over the two input headers.
/// With a counter, the condition is a counting `less` instead of the named function.
BlockNestedLoopPredicate makePredicate(const String & function_name, bool nullable, const PairCounter & counter = nullptr)
{
    tryRegisterFunctions();

    ActionsDAG dag(NamesAndTypesList{{"probe", valueType(nullable)}, {"build", valueType(nullable)}});
    FunctionOverloadResolverPtr function;
    if (counter)
        function = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<CountingLess>(counter));
    else
        function = FunctionFactory::instance().get(function_name, getContext().context);
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

/// A temporary storage scope backed by a directory that lives as long as the returned holder.
struct TemporaryStorage
{
    std::unique_ptr<Poco::TemporaryFile> directory;
    TemporaryDataOnDiskScopePtr scope;
};

TemporaryStorage makeTemporaryStorage()
{
    TemporaryStorage storage;
    storage.directory = std::make_unique<Poco::TemporaryFile>();
    storage.directory->createDirectories();
    auto disk = std::make_shared<DiskLocal>("block_nested_loop_join_tmp", storage.directory->path() + "/");
    VolumePtr volume = std::make_shared<SingleDiskVolume>("block_nested_loop_join_tmp", disk, 0);
    storage.scope = std::make_shared<TemporaryDataOnDiskScope>(TemporaryDataOnDiskSettings{}, volume);
    return storage;
}

/// Every block goes to disk: a threshold of one byte is passed by any of them.
BlockNestedLoopStoreSettings spillEverything(const TemporaryDataOnDiskScopePtr & scope)
{
    BlockNestedLoopStoreSettings settings;
    settings.max_bytes_in_memory = 1;
    settings.tmp_data = scope;
    return settings;
}

/// Every block is compressed: the very first one already passes the threshold.
BlockNestedLoopStoreSettings compressEverything()
{
    BlockNestedLoopStoreSettings settings;
    settings.min_rows_to_compress = 1;
    return settings;
}

BlockNestedLoopJoinDataPtr makeData(
    const std::vector<Values> & build_blocks,
    const SharedHeader & build_header,
    JoinKind kind,
    JoinStrictness strictness,
    bool nullable,
    BlockNestedLoopStoreSettings store_settings = {})
{
    auto data = std::make_shared<BlockNestedLoopJoinData>(
        build_header, kind, strictness, SizeLimits{}, std::move(store_settings));
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
    bool nullable = false,
    const PairCounter & counter = nullptr,
    BlockNestedLoopStoreSettings store_settings = {})
{
    auto probe_header = makeHeader("probe", nullable);
    auto build_header = makeHeader("build", nullable);
    auto output_header = makeOutputHeader(probe_header, build_header);
    auto data = makeData(build_blocks, build_header, kind, strictness, nullable, std::move(store_settings));

    Chunks chunks;
    for (const auto & values : probe_chunks)
        chunks.emplace_back(Columns{makeColumn(values, nullable)}, values.size());

    auto source = std::make_shared<SourceFromChunks>(probe_header, std::move(chunks));
    auto probe = std::make_shared<BlockNestedLoopProbeTransform>(
        probe_header, output_header, data, makePredicate(function_name, nullable, counter), max_block_size, 0);

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
            output_header, data, max_block_size, /*max_block_bytes=*/ 0, 0, 1);

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
            output_header, data, /*max_block_size=*/ 0, /*max_block_bytes=*/ 0, stream_index, num_streams);
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

TEST(BlockNestedLoopJoinProbe, MatchesTheSameWayAgainstASpilledBuildSide)
{
    /// Every build block is on disk, and each of the three probe chunks walks the whole file, so
    /// the reader has to start it over between them.
    auto storage = makeTemporaryStorage();

    const std::vector<Values> build_blocks{{2, 5}, {7}, {11}};
    const std::vector<Values> probe_chunks{{1, 6}, {8}, {4}};

    auto in_memory = runProbe(build_blocks, probe_chunks, JoinKind::Full, JoinStrictness::All,
        /*max_block_size=*/ 0, "less", /*nullable=*/ true);
    auto spilled = runProbe(build_blocks, probe_chunks, JoinKind::Full, JoinStrictness::All,
        /*max_block_size=*/ 0, "less", /*nullable=*/ true, /*counter=*/ nullptr, spillEverything(storage.scope));

    EXPECT_EQ(sorted(spilled.rows), sorted(in_memory.rows));
    EXPECT_FALSE(spilled.rows.empty());
}

TEST(BlockNestedLoopJoinProbe, MatchesTheSameWayAgainstACompressedBuildSide)
{
    const std::vector<Values> build_blocks{{2, 5}, {7}, {11}};
    const std::vector<Values> probe_chunks{{1, 6}, {8}, {4}};

    auto in_memory = runProbe(build_blocks, probe_chunks, JoinKind::Full, JoinStrictness::All,
        /*max_block_size=*/ 0, "less", /*nullable=*/ true);
    auto compressed = runProbe(build_blocks, probe_chunks, JoinKind::Full, JoinStrictness::All,
        /*max_block_size=*/ 0, "less", /*nullable=*/ true, /*counter=*/ nullptr, compressEverything());

    EXPECT_EQ(sorted(compressed.rows), sorted(in_memory.rows));
    EXPECT_FALSE(compressed.rows.empty());
}

TEST(BlockNestedLoopJoinProbe, UnsupportedJoinTypesAreRejected)
{
    /// `ASOF` prescribes the shape of its condition, so an arbitrary predicate is not one it can
    /// express, and the operator is never chosen for it.
    EXPECT_THROW(runProbe({{1}}, {{2}}, JoinKind::Left, JoinStrictness::Asof), Exception);
    EXPECT_THROW(runProbe({{1}}, {{2}}, JoinKind::Inner, JoinStrictness::Unspecified), Exception);
}

/// The build side of every left-driven test below: the walk visits 2, then 5, then 7, so the first
/// build row a probe row matches under `probe < build` is the smallest one greater than it.
const std::vector<Values> left_driven_build_blocks{{2, 5}, {7}};

TEST(BlockNestedLoopJoinProbe, AnyLeftKeepsOneMatchPerProbeRowAndPadsTheRest)
{
    for (auto strictness : {JoinStrictness::Any, JoinStrictness::RightAny})
    {
        auto result = runProbe(
            left_driven_build_blocks, {{1, 4, 6, 9}},
            JoinKind::Left, strictness, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

        EXPECT_EQ(sorted(result.rows), sorted({
            {Value(1), Value(2)}, {Value(4), Value(5)}, {Value(6), Value(7)}, {Value(9), std::nullopt}}))
            << toString(strictness);
    }
}

TEST(BlockNestedLoopJoinProbe, AnyInnerTakesEachRowOfEitherSideOnce)
{
    /// `ANY INNER` disables the cartesian product on both sides: the probe row 1 takes 2, the probe
    /// row 4 takes 5, and the probe row 3 - which matches 5 and 7 - has to settle for 7, because 5
    /// is already taken. Nothing is left for the probe row 9.
    auto result = runProbe(left_driven_build_blocks, {{1, 4, 3, 9}}, JoinKind::Inner, JoinStrictness::Any);

    EXPECT_EQ(sorted(result.rows), sorted({{1, 2}, {4, 5}, {3, 7}}));
}

TEST(BlockNestedLoopJoinProbe, AnyInnerLeavesOverProbeRowsWhenTheBuildSideRunsOut)
{
    /// Two probe rows compete for one build row, so one of them ends up with nothing - which is
    /// what makes the result the same however the planner orders the two inputs.
    auto result = runProbe({{5}}, {{1, 2}}, JoinKind::Inner, JoinStrictness::Any);

    EXPECT_EQ(result.rows, (std::vector<JoinedRow>{{1, 5}}));
}

TEST(BlockNestedLoopJoinProbe, SemiLeftEmitsEveryMatchedProbeRowOnce)
{
    auto result = runProbe(left_driven_build_blocks, {{1, 4, 9}}, JoinKind::Left, JoinStrictness::Semi);

    EXPECT_EQ(sorted(result.rows), sorted({{1, 2}, {4, 5}}));
}

TEST(BlockNestedLoopJoinProbe, AntiLeftEmitsOnlyTheProbeRowsThatMatchedNothing)
{
    auto result = runProbe(
        left_driven_build_blocks, {{1, 4, 9, 8}},
        JoinKind::Left, JoinStrictness::Anti, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(9), std::nullopt}, {Value(8), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, AntiLeftWithAnEmptyBuildSideKeepsEveryProbeRow)
{
    auto result = runProbe(
        {}, {{1, 2}},
        JoinKind::Left, JoinStrictness::Anti, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(1), std::nullopt}, {Value(2), std::nullopt}}));
}

TEST(BlockNestedLoopJoinProbe, SemiRightEmitsEveryMatchedBuildRowOnce)
{
    /// Probe {4, 9, 1} against build {2, 5, 7}: the walk gives 5 and 7 to the probe row 4, which
    /// reaches them first, and 2 to the probe row 1. Nothing matches the probe row 9.
    auto result = runProbe({{2, 5}, {7}}, {{4, 9, 1}}, JoinKind::Right, JoinStrictness::Semi);

    EXPECT_EQ(sorted(result.rows), sorted({{1, 2}, {4, 5}, {4, 7}}));
}

TEST(BlockNestedLoopJoinProbe, AnyRightEmitsEveryBuildRowExactlyOnce)
{
    /// Each build row leaves with the first probe row that matches it; the build row 1, which
    /// nothing matches, is the one the scan after the probe pads.
    auto result = runProbe(
        {{1, 2}, {5, 7}}, {{4, 9, 1}},
        JoinKind::Right, JoinStrictness::Any, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({
        {Value(1), Value(2)}, {Value(4), Value(5)}, {Value(4), Value(7)}, {std::nullopt, Value(1)}}));
}

TEST(BlockNestedLoopJoinProbe, AntiRightEmitsOnlyTheBuildRowsThatMatchedNothing)
{
    auto result = runProbe(
        {{2, 5}, {7}}, {{4, 9}},
        JoinKind::Right, JoinStrictness::Anti, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{std::nullopt, Value(2)}}));
}

TEST(BlockNestedLoopJoinProbe, AntiRightWithAnEmptyProbeSideKeepsEveryBuildRow)
{
    auto result = runProbe(
        {{2}, {5}}, {},
        JoinKind::Right, JoinStrictness::Anti, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{std::nullopt, Value(2)}, {std::nullopt, Value(5)}}));
}

TEST(BlockNestedLoopJoinProbe, RightAnyKeepsOneMatchPerProbeRowAndDropsTheBuildRowsItPassedOver)
{
    /// The old `ANY`: the probe row 4 takes 5, the first build row it matches, and 7 - which it
    /// matched but did not take - counts as matched all the same, so the scan after the probe pads
    /// only the build row 2, which nothing matched.
    auto result = runProbe(
        {{5, 7}, {2}}, {{4}},
        JoinKind::Right, JoinStrictness::RightAny, /*max_block_size=*/ 0, "less", /*nullable=*/ true);

    EXPECT_EQ(sorted(result.rows), sorted({{Value(4), Value(5)}, {std::nullopt, Value(2)}}));
}

TEST(BlockNestedLoopJoinProbe, EarlyExitStopsTheBuildSideWalkAtTheFirstMatch)
{
    /// One block per build row, so the walk can stop between any two of them.
    const std::vector<Values> build_blocks{{100}, {101}, {102}, {103}};

    auto pairs_evaluated = [&](JoinKind kind, JoinStrictness strictness, const Values & probe_values)
    {
        auto counter = std::make_shared<std::atomic<size_t>>(0);
        runProbe(build_blocks, {probe_values}, kind, strictness, /*max_block_size=*/ 0, "less",
            /*nullable=*/ true, counter);
        return counter->load();
    };

    /// `ALL` evaluates the whole cartesian product of the chunk and the build side.
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::All, {1, 2}), 8);

    /// The strictnesses that keep one match per probe row stop after the first build block, which
    /// every probe row matches.
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::Any, {1, 2}), 2);
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::Semi, {1, 2}), 2);
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::Anti, {1, 2}), 2);

    /// `ANY INNER` also takes each build row once, so the second probe row walks on to the second
    /// block after losing the first one: 2 pairs on the first block and 1 on the second.
    EXPECT_EQ(pairs_evaluated(JoinKind::Inner, JoinStrictness::Any, {1, 2}), 3);

    /// A probe row that matches nothing keeps the walk going, on its own: 2 pairs on the first
    /// block, then one per block for the probe row that is still looking. A `NULL` probe value is
    /// such a row - the condition is never true for it.
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::Any, {1, 200}), 5);
    EXPECT_EQ(pairs_evaluated(JoinKind::Left, JoinStrictness::Any, {1, std::nullopt}), 5);

    /// The right-driven selections need every build row, and so does a kind that pads the build
    /// rows nothing matched, whichever side drives it.
    EXPECT_EQ(pairs_evaluated(JoinKind::Right, JoinStrictness::Any, {1, 2}), 8);
    EXPECT_EQ(pairs_evaluated(JoinKind::Right, JoinStrictness::Semi, {1, 2}), 8);
    EXPECT_EQ(pairs_evaluated(JoinKind::Right, JoinStrictness::Anti, {1, 2}), 8);
    EXPECT_EQ(pairs_evaluated(JoinKind::Full, JoinStrictness::RightAny, {1, 2}), 8);
}
