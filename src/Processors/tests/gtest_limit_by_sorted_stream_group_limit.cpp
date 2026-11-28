#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Port.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

using namespace DB;

/// Pins the early stop of LimitBySortedStreamTransform, which `pushLimitByIntoSort` arms from an outer
/// `LIMIT` above a `LIMIT n OFFSET m BY` whose keys are a sort prefix: the stream stops once the rows that
/// the final `LIMIT BY` returns from its groups, `min(n, max(0, rows_in_group - m))` each, reach the outer
/// `LIMIT`. Chunk sizes reachable from a server make one chunk hold far more rows than the outer `LIMIT`,
/// so the stopping chunk is the first one either way; these cases drive the transform chunk by chunk.

namespace
{

SharedHeader makeHeader()
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    return std::make_shared<const Block>(std::move(header));
}

SortDescription makeSortDescription()
{
    SortDescription description;
    description.push_back(SortColumnDescription("k"));
    return description;
}

/// One chunk holding `keys.size()` rows: row i has key `keys[i]` and a distinct value.
Chunk makeChunk(const std::vector<UInt64> & keys)
{
    auto key_column = ColumnUInt64::create();
    auto value_column = ColumnUInt64::create();
    for (size_t i = 0; i < keys.size(); ++i)
    {
        key_column->insertValue(keys[i]);
        value_column->insertValue(i);
    }
    return Chunk(Columns{std::move(key_column), std::move(value_column)}, keys.size());
}

/// source(chunks) -> LimitBySortedStreamTransform(n + m, offset 0, outer_limit, m) -> pull, as `SortingStep` creates it.
/// Returns the rows the transform emitted. `stopReading()` is observable only through this count:
/// the executor closes the transform's input, so later chunks never reach it.
size_t countOutputRows(
    const std::vector<std::vector<UInt64>> & chunk_keys, UInt64 limit_by_length, UInt64 limit_by_offset, UInt64 outer_limit)
{
    auto header = makeHeader();

    Chunks chunks;
    for (const auto & keys : chunk_keys)
        chunks.emplace_back(makeChunk(keys));
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));

    auto transform = std::make_shared<LimitBySortedStreamTransform>(
        header,
        limit_by_length + limit_by_offset,
        /*group_offset_=*/0,
        makeSortDescription(),
        /*always_read_till_end_=*/false,
        outer_limit,
        limit_by_offset);

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

/// `chunk_count` chunks, each holding `groups_per_chunk` groups of `rows_per_group` rows, with keys
/// increasing across the whole stream so no group spans two chunks.
std::vector<std::vector<UInt64>> makeWholeGroupChunks(size_t chunk_count, size_t groups_per_chunk, size_t rows_per_group)
{
    std::vector<std::vector<UInt64>> chunk_keys;
    UInt64 next_key = 0;
    for (size_t chunk = 0; chunk < chunk_count; ++chunk)
    {
        std::vector<UInt64> keys;
        for (size_t group = 0; group < groups_per_chunk; ++group, ++next_key)
            keys.insert(keys.end(), rows_per_group, next_key);
        chunk_keys.push_back(std::move(keys));
    }
    return chunk_keys;
}

}

TEST(LimitBySortedStreamTransform, OuterLimitZeroReadsEverything)
{
    /// The three untouched construction sites keep the default 0, which must read to the end.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/1, /*limit_by_offset=*/0, /*outer_limit=*/0), 10u);
}

TEST(LimitBySortedStreamTransform, LengthOneStopsOnTheChunkThatReachesTheLimit)
{
    /// One row of each group counts, the unfinished group of the current chunk included, so the 4th chunk
    /// reaches the limit of 4. Counting only completed groups reads a 5th chunk.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/1, /*limit_by_offset=*/0, /*outer_limit=*/4), 4u);
}

TEST(LimitBySortedStreamTransform, LengthAboveOneCountsRowsNotGroups)
{
    /// `LIMIT 3 BY k LIMIT 6`: each group returns 3 rows, so two groups are enough. Counting groups reads 7.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/3, /*limit_by_offset=*/0, /*outer_limit=*/6), 6u);
}

TEST(LimitBySortedStreamTransform, UnfinishedGroupCanReachTheLimit)
{
    /// `LIMIT 4 BY k LIMIT 3`: the first chunk already holds 4 rows of key 0, so the stream stops before
    /// the rest of that group.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 0, 0}, {0, 0, 1, 1}, {2, 2, 3, 3}};
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/4, /*limit_by_offset=*/0, /*outer_limit=*/3), 4u);
}

TEST(LimitBySortedStreamTransform, OffsetRowsAreNotCounted)
{
    /// `LIMIT 2 OFFSET 1 BY k LIMIT 3`: each group of 4 rows passes 3 rows and returns 2, so the second group
    /// is needed. Counting the passed rows stops after the first.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/2, /*limit_by_offset=*/1, /*outer_limit=*/3), 6u);
}

TEST(LimitBySortedStreamTransform, GroupsWithinTheOffsetNeverStop)
{
    /// `LIMIT 1 OFFSET 1 BY k LIMIT 1`: single-row groups return nothing, so the stream reads to the end.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/2, /*rows_per_group=*/1);
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/1, /*limit_by_offset=*/1, /*outer_limit=*/1), 20u);
}

TEST(LimitBySortedStreamTransform, ShortGroupsCountOnlyRowsPastTheOffset)
{
    /// `LIMIT 2 OFFSET 2 BY k LIMIT 3`: key 0 has 2 rows and returns none, key 1 has 3 and returns one,
    /// key 2 has 4 and returns two, which reaches the limit in the second chunk.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}};
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/2, /*limit_by_offset=*/2, /*outer_limit=*/3), 9u);
}

TEST(LimitBySortedStreamTransform, GroupSplitAcrossChunksIsCountedOnce)
{
    /// Key 0 spans all four chunks and key 1 only starts in the last one. Counting key 0 once per chunk
    /// reaches the limit of 2 during chunk 2 and stops before key 1 is ever seen.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 1, 1}};
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/1, /*limit_by_offset=*/0, /*outer_limit=*/2), 2u);
}

TEST(LimitBySortedStreamTransform, TrailingRowsOfTheLastCountedGroupSurvive)
{
    /// Group 1 straddles the first two chunks. The limit is reached inside the second chunk, and that
    /// chunk is still processed whole: two rows for group 0, two for group 1, two for group 2.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 1}, {1, 2, 2}, {3, 3, 4}};
    EXPECT_EQ(countOutputRows(chunks, /*limit_by_length=*/2, /*limit_by_offset=*/0, /*outer_limit=*/4), 6u);
}
