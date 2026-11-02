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
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

/// Pins the cumulative limit accounting of DistinctSortedStreamTransform. The transform serves
/// both the per-stream pre-distinct and the final distinct over a stream sorted by a prefix of
/// the distinct columns. Its per-range hash set is cleared between ranges of equal sort-prefix
/// values, so the set size reflects only the current range: max_rows_in_distinct and limit_hint
/// must instead be checked against the cumulative count of emitted distinct rows
/// (`total_output_rows`) — a stream of many ranges that are each below the limit has to trip on
/// their sum. The byte limit stays on `data.getTotalByteCount()`, whose allocation does not
/// shrink on clear.

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

/// One chunk = one sort-prefix range: "a" (the sort prefix) is `run_key` in every row, "b" is
/// 0..rows-1, so the range holds `rows` distinct rows. Ascending `run_key` across chunks keeps
/// the stream sorted, and each new range clears the transform's set.
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

/// source (num_runs ranges of run_rows distinct rows each) -> DistinctSortedStreamTransform ->
/// pull. Returns the total number of rows the transform emitted. Stopping early is the
/// transform's `stopReading()`: the executor then closes its input and finishes the source,
/// which is the only externally observable effect of the 'break' overflow mode and of
/// limit_hint.
size_t countDistinctOutputRows(UInt64 num_runs, UInt64 run_rows, const SizeLimits & set_size_limits, UInt64 limit_hint)
{
    auto header = makeHeader();

    Chunks chunks;
    for (UInt64 run = 0; run < num_runs; ++run)
        chunks.emplace_back(makeRunChunk(run, run_rows));
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));

    auto transform
        = std::make_shared<DistinctSortedStreamTransform>(header, set_size_limits, limit_hint, makeSortDescription(), Names{"a", "b"});

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

TEST(DistinctSortedStreamTransform, BreakLimitIsCumulativeAcrossSortPrefixRanges)
{
    /// 100 ranges of 5 distinct rows: no range reaches max_rows = 25, only their sum does.
    /// 'break' soft-checks at >=, so reading stops on the chunk that makes the total 25;
    /// that chunk is still emitted.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::BREAK);
    EXPECT_EQ(countDistinctOutputRows(/*num_runs=*/ 100, /*run_rows=*/ 5, set_size_limits, /*limit_hint=*/ 0), 25u);
}

TEST(DistinctSortedStreamTransform, LimitHintStopsReadingAcrossSortPrefixRanges)
{
    /// Same range layout, no size limits: DISTINCT ... LIMIT 25 passes limit_hint = 25 and
    /// expects reading to stop once 25 distinct rows have been emitted overall.
    SizeLimits no_limits;
    EXPECT_EQ(countDistinctOutputRows(/*num_runs=*/ 100, /*run_rows=*/ 5, no_limits, /*limit_hint=*/ 25), 25u);
}

TEST(DistinctSortedStreamTransform, ThrowLimitIsNotMaskedByLimitHint)
{
    /// One chunk can reach the limit hint and exceed the size limit at the same time: here the
    /// third range takes the total from 20 to 30, which satisfies both limit_hint = 25 and
    /// max_rows = 25. The size limit must be evaluated first: in the 'throw' overflow mode the
    /// query has to fail with SET_SIZE_LIMIT_EXCEEDED, not stop silently as if it were 'break'.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::THROW);
    DistinctSortedStreamTransform transform(
        makeHeader(), set_size_limits, /*limit_hint_=*/ 25, makeSortDescription(), Names{"a", "b"});

    for (UInt64 run = 0; run < 2; ++run)
    {
        Chunk chunk = makeRunChunk(run, /*rows=*/ 10);
        ASSERT_NO_THROW(runTransform(transform, chunk));
        EXPECT_EQ(chunk.getNumRows(), 10u);
    }

    Chunk crossing_chunk = makeRunChunk(/*run_key=*/ 2, /*rows=*/ 10);
    try
    {
        runTransform(transform, crossing_chunk);
        FAIL() << "expected SET_SIZE_LIMIT_EXCEEDED on the range that pushes the total to 30 > 25";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
}

TEST(DistinctSortedStreamTransform, LimitHintReachedWithoutExceedingThrowLimit)
{
    /// The limit hint stops reading on its own when the size limit is not exceeded: totals go
    /// 10, 20, the hint fires at 20 >= 20, and max_rows = 25 in the 'throw' mode stays satisfied,
    /// so the transform stops silently with the complete LIMIT-sized result and no exception.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::THROW);
    EXPECT_EQ(countDistinctOutputRows(/*num_runs=*/ 100, /*run_rows=*/ 10, set_size_limits, /*limit_hint=*/ 20), 20u);
}

TEST(DistinctSortedStreamTransform, ThrowLimitIsCumulativeAcrossSortPrefixRanges)
{
    /// 'throw' fails at >, so ranges totalling exactly 25 pass and the next one must throw
    /// even though its own range (and therefore the per-range set) holds only 5 rows.
    SizeLimits set_size_limits(/*max_rows_=*/ 25, /*max_bytes_=*/ 0, OverflowMode::THROW);
    DistinctSortedStreamTransform transform(
        makeHeader(), set_size_limits, /*limit_hint_=*/ 0, makeSortDescription(), Names{"a", "b"});

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
        FAIL() << "expected SET_SIZE_LIMIT_EXCEEDED on the range that pushes the total to 30 > 25";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
}
