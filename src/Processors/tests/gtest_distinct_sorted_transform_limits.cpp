#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Port.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

/// Regression tests for cumulative limit accounting in DistinctSortedTransform. The
/// transform's clearable set is reset on every sort-prefix run, so the set size reflects
/// only the current run: max_rows_in_distinct / limit_hint checked against it never trip
/// on a stream of many runs that are each below the limit. They must be checked against
/// the cumulative count of emitted distinct rows (`total_output_rows`); the byte limit
/// stays on `data.getTotalByteCount()`, whose allocation does not shrink on clear.
///
/// This is not reachable from an SQL test: every plan shape places a preliminary DISTINCT
/// (DistinctSortedStreamTransform, which already counts cumulatively) before the final
/// DistinctSortedTransform, and the preliminary stage trips the limit first. Hence a
/// processor-level test: feed the transform sort-prefix runs directly, each below the
/// limit, and require the limit to trip on their sum.

namespace
{

/// `transform(Chunk &)` is `protected` in the concrete transform but `public` in the
/// abstract `ISimpleTransform` base, so the test drives it through the base reference,
/// which virtual-dispatches to the override (exactly what the pipeline executor does).
void runTransform(ISimpleTransform & transform, Chunk & chunk)
{
    transform.transform(chunk);
}

SharedHeader makeHeader()
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "a"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "b"),
    };
    return std::make_shared<const Block>(std::move(header));
}

SortDescription makeSortDescription()
{
    SortDescription description;
    description.push_back(SortColumnDescription("a"));
    return description;
}

/// One chunk = one sort-prefix run: "a" (the sort prefix) is `run_key` in every row, "b" is
/// 0..rows-1, so the run holds `rows` distinct rows. Ascending `run_key` across chunks keeps
/// the stream sorted, and each new run clears the transform's set.
Chunk makeRunChunk(UInt64 run_key, UInt64 rows)
{
    auto a_column = ColumnUInt64::create();
    auto b_column = ColumnUInt64::create();
    for (UInt64 i = 0; i < rows; ++i)
    {
        a_column->insertValue(run_key);
        b_column->insertValue(i);
    }
    return Chunk(Columns{std::move(a_column), std::move(b_column)}, rows);
}

/// source (num_runs runs of run_rows distinct rows each) -> DistinctSortedTransform -> pull.
/// Returns the total number of rows the transform emitted. Stopping early is the transform's
/// `stopReading()`: the executor then closes its input and finishes the source, which is the
/// only externally observable effect of the 'break' overflow mode and of limit_hint.
size_t countDistinctOutputRows(UInt64 num_runs, UInt64 run_rows, const SizeLimits & set_size_limits, UInt64 limit_hint)
{
    auto header = makeHeader();

    Chunks chunks;
    for (UInt64 run = 0; run < num_runs; ++run)
        chunks.emplace_back(makeRunChunk(run, run_rows));
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));

    auto transform
        = std::make_shared<DistinctSortedTransform>(header, makeSortDescription(), set_size_limits, limit_hint, Names{"a", "b"});

    connect(source->getPort(), transform->getInputPort());
    auto * output_port = &transform->getOutputPort();

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(transform));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block;
    while (executor.pull(block))
        total_rows += block.rows();
    return total_rows;
}

}

TEST(DistinctSortedTransform, BreakLimitIsCumulativeAcrossSortPrefixRuns)
{
    /// 100 runs of 5 distinct rows: no run reaches max_rows = 25, only their sum does.
    /// 'break' soft-checks at >=, so reading stops on the run that makes the total 25;
    /// that run is still emitted. Against the per-run set size the limit never trips
    /// and all 500 rows come out.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::BREAK);
    EXPECT_EQ(countDistinctOutputRows(/*num_runs=*/ 100, /*run_rows=*/ 5, set_size_limits, /*limit_hint=*/ 0), 25u);
}

TEST(DistinctSortedTransform, LimitHintStopsReadingAcrossSortPrefixRuns)
{
    /// Same run layout, no size limits: DISTINCT ... LIMIT 25 passes limit_hint = 25 and
    /// expects reading to stop once 25 distinct rows have been emitted overall.
    SizeLimits no_limits;
    EXPECT_EQ(countDistinctOutputRows(/*num_runs=*/ 100, /*run_rows=*/ 5, no_limits, /*limit_hint=*/ 25), 25u);
}

TEST(DistinctSortedTransform, ThrowLimitIsCumulativeAcrossSortPrefixRuns)
{
    /// 'throw' fails at >, so runs totalling exactly 25 pass and the next one must throw
    /// even though its own run (and therefore the clearable set) holds only 5 rows.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::THROW);
    DistinctSortedTransform transform(
        makeHeader(), makeSortDescription(), set_size_limits, /*limit_hint_=*/ 0, Names{"a", "b"});

    for (UInt64 run = 0; run < 5; ++run)
    {
        Chunk chunk = makeRunChunk(run, /*rows=*/ 5);
        ASSERT_NO_THROW(runTransform(transform, chunk));
        EXPECT_EQ(chunk.getNumRows(), 5u);
    }

    Chunk crossing_chunk = makeRunChunk(/*run_key=*/ 5, /*rows=*/ 5);
    try
    {
        runTransform(transform, crossing_chunk);
        FAIL() << "expected SET_SIZE_LIMIT_EXCEEDED on the run that pushes the total to 30 > 25";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
}
