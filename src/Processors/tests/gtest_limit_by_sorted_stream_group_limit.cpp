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

/// Pins the group-count early stop of LimitBySortedStreamTransform, which `pushLimitByIntoSort` arms
/// from an outer `LIMIT` above a `LIMIT BY` whose keys are a sort prefix. A group counts as complete
/// only once a strictly different key follows it, so a group split across chunks is counted once and
/// its trailing rows are never cut off mid-group. Chunk sizes reachable from a server make one chunk
/// hold far more groups than the hint, so the stopping chunk is the first one either way; these arms
/// drive the transform chunk by chunk, where the completion predicate is observable.

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

/// source(chunks) -> LimitBySortedStreamTransform(group_length, groups_limit_hint) -> pull.
/// Returns the rows the transform emitted. `stopReading()` is observable only through this count:
/// the executor closes the transform's input, so later chunks never reach it.
size_t countOutputRows(const std::vector<std::vector<UInt64>> & chunk_keys, UInt64 group_length, UInt64 groups_limit_hint)
{
    auto header = makeHeader();

    Chunks chunks;
    for (const auto & keys : chunk_keys)
        chunks.emplace_back(makeChunk(keys));
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));

    auto transform = std::make_shared<LimitBySortedStreamTransform>(
        header, group_length, /*group_offset_=*/0, makeSortDescription(), groups_limit_hint);

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

TEST(LimitBySortedStreamTransform, GroupsLimitHintZeroReadsEverything)
{
    /// The three untouched construction sites keep the default 0, which must read to the end.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*group_length=*/1, /*groups_limit_hint=*/0), 10u);
}

TEST(LimitBySortedStreamTransform, GroupsLimitHintStopsAfterTheHintthGroup)
{
    /// One group per chunk, so a group is completed only when the next chunk starts a different key.
    /// The hint of 4 is reached while processing the 5th chunk, whose own group is still emitted, so
    /// 5 of the 10 groups come out. Counting a group when its first row is seen instead reaches 4 in
    /// the 3rd chunk and emits 3.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/10, /*groups_per_chunk=*/1, /*rows_per_group=*/4);
    EXPECT_EQ(countOutputRows(chunks, /*group_length=*/1, /*groups_limit_hint=*/4), 5u);
}

TEST(LimitBySortedStreamTransform, SeveralGroupsPerChunkStopOnTheChunkThatReachesTheHint)
{
    /// Three groups per chunk: the first chunk completes two of them (its last group is only known
    /// to be complete once the next chunk shows a different key), the second reaches five and stops.
    /// Both chunks are emitted whole, so no group inside them is cut short.
    const auto chunks = makeWholeGroupChunks(/*chunk_count=*/4, /*groups_per_chunk=*/3, /*rows_per_group=*/2);
    EXPECT_EQ(countOutputRows(chunks, /*group_length=*/1, /*groups_limit_hint=*/4), 6u);
}

TEST(LimitBySortedStreamTransform, GroupSplitAcrossChunksIsCountedOnce)
{
    /// Key 0 spans all four chunks and key 1 only starts in the last one. A per-chunk count would
    /// reach a hint of 2 during chunk 2 and stop before key 1 is ever seen; counting completions
    /// keeps reading, so both groups are emitted.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 1, 1}};
    EXPECT_EQ(countOutputRows(chunks, /*group_length=*/1, /*groups_limit_hint=*/2), 2u);
}

TEST(LimitBySortedStreamTransform, TrailingRowsOfTheHintthGroupSurvive)
{
    /// Group 1 straddles the first two chunks. The hint is reached inside the second chunk, and that
    /// chunk is still processed whole, so group 1's second row is emitted rather than cut off:
    /// two rows for group 0, two for group 1, two for group 2.
    const std::vector<std::vector<UInt64>> chunks{{0, 0, 1}, {1, 2, 2}, {3, 3, 4}};
    EXPECT_EQ(countOutputRows(chunks, /*group_length=*/2, /*groups_limit_hint=*/2), 6u);
}
