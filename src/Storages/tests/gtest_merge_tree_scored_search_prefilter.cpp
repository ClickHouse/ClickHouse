#include "config.h"

#if USE_USEARCH

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Port.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/ScoredSearch/DelayedCreatingBitmapsStep.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

namespace
{

/// Build the (_part_index, _part_offset) header expected by `BuildBitmapsTransform`.
SharedHeader makeBitmapInputHeader()
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "_part_index"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "_part_offset"),
    };
    return std::make_shared<const Block>(std::move(header));
}

/// Construct one chunk from raw (part_index, part_offset) pairs.
Chunk makeChunk(const std::vector<std::pair<UInt64, UInt64>> & pairs)
{
    auto part_index_col = ColumnUInt64::create();
    auto part_offset_col = ColumnUInt64::create();
    auto & part_index_data = part_index_col->getData();
    auto & part_offset_data = part_offset_col->getData();
    part_index_data.reserve(pairs.size());
    part_offset_data.reserve(pairs.size());
    for (const auto & [part_index, part_offset] : pairs)
    {
        part_index_data.push_back(part_index);
        part_offset_data.push_back(part_offset);
    }

    Columns cols;
    cols.emplace_back(std::move(part_index_col));
    cols.emplace_back(std::move(part_offset_col));
    return Chunk(std::move(cols), pairs.size());
}

/// Allocate `num_parts` empty per-part bitmaps. Mirrors the pre-allocation
/// `ReadFromMergeTreeScoredSearch` does before the pipeline is built: every
/// slot must be non-null so the transform fills it in place rather than replacing it.
std::shared_ptr<PerPartBitmaps> makeBitmaps(size_t num_parts)
{
    auto bitmaps = std::make_shared<PerPartBitmaps>(num_parts);
    for (auto & slot : *bitmaps)
        slot = std::make_shared<roaring::Roaring>();
    return bitmaps;
}

/// Wire a Source → BuildBitmapsTransform → EmptySink completed pipeline. The
/// transform receives a copy of `bitmaps` whose slots share the same `roaring::Roaring`
/// objects, so the caller observes the rows it adds through its own `bitmaps`.
QueryPipeline buildDrainPipeline(
    Chunks chunks,
    const PerPartBitmaps & bitmaps,
    UInt64 rows_budget)
{
    auto input_header = makeBitmapInputHeader();
    auto empty_header = std::make_shared<const Block>(Block{});

    auto source = std::make_shared<SourceFromChunks>(input_header, std::move(chunks));
    auto transform = std::make_shared<BuildBitmapsTransform>(
        input_header, empty_header, bitmaps, rows_budget);
    auto sink = std::make_shared<EmptySink>(empty_header);

    connect(source->getPort(), transform->getInputPort());
    connect(transform->getOutputPort(), sink->getPort());

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(transform));
    processors->emplace_back(std::move(sink));

    return QueryPipeline(QueryPlanResourceHolder{}, std::move(processors));
}

}

TEST(MergeTreeScoredSearchPrefilter, BuildBitmapsTransformPopulatesPerPartBitmaps)
{
    /// Three parts. Sprinkle offsets across parts and chunks, including duplicates
    /// (Roaring deduplicates) and the no-rows case for part 1.
    const size_t num_parts = 3;
    auto bitmaps = makeBitmaps(num_parts);

    Chunks chunks;
    chunks.emplace_back(makeChunk({{0, 5}, {0, 7}, {0, 5}, {2, 1}}));
    chunks.emplace_back(makeChunk({{0, 100}, {2, 1}, {2, 9}}));

    auto pipeline = buildDrainPipeline(std::move(chunks), *bitmaps, /*rows_budget=*/ 1ULL << 30);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    ASSERT_EQ(bitmaps->size(), num_parts);

    EXPECT_FALSE((*bitmaps)[0]->isEmpty());
    EXPECT_EQ((*bitmaps)[0]->cardinality(), 3u);
    EXPECT_TRUE((*bitmaps)[0]->contains(5));
    EXPECT_TRUE((*bitmaps)[0]->contains(7));
    EXPECT_TRUE((*bitmaps)[0]->contains(100));

    /// Part 1 had no surviving rows — the slot must stay empty (but non-null).
    EXPECT_TRUE((*bitmaps)[1]->isEmpty());

    EXPECT_FALSE((*bitmaps)[2]->isEmpty());
    EXPECT_EQ((*bitmaps)[2]->cardinality(), 2u);  /// 1 deduped, plus 9
    EXPECT_TRUE((*bitmaps)[2]->contains(1));
    EXPECT_TRUE((*bitmaps)[2]->contains(9));
}

TEST(MergeTreeScoredSearchPrefilter, BuildBitmapsTransformHandlesEmptyInput)
{
    auto bitmaps = makeBitmaps(2);
    Chunks chunks;
    auto pipeline = buildDrainPipeline(std::move(chunks), *bitmaps, /*rows_budget=*/ 1ULL << 30);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    EXPECT_TRUE((*bitmaps)[0]->isEmpty());
    EXPECT_TRUE((*bitmaps)[1]->isEmpty());
}

TEST(MergeTreeScoredSearchPrefilter, BuildBitmapsTransformThrowsOnRowBudgetOverflow)
{
    /// A row budget of 2 with 3 surviving rows must overflow on the first
    /// chunk — fail-close, no fallback.
    const size_t num_parts = 1;
    auto bitmaps = makeBitmaps(num_parts);

    Chunks chunks;
    chunks.emplace_back(makeChunk({{0, 1}, {0, 2}, {0, 3}}));

    auto pipeline = buildDrainPipeline(std::move(chunks), *bitmaps, /*rows_budget=*/ 2);
    CompletedPipelineExecutor executor(pipeline);

    try
    {
        executor.execute();
        FAIL() << "Expected LIMIT_EXCEEDED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
}

TEST(MergeTreeScoredSearchPrefilter, BuildBitmapsTransformRejectsPartIndexOutOfRange)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test triggers LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    /// Sized for one part, but a chunk carries part_index = 5 — `BuildBitmapsTransform`
    /// must surface this as a LOGICAL_ERROR rather than silently growing the vector.
    auto bitmaps = makeBitmaps(1);

    Chunks chunks;
    chunks.emplace_back(makeChunk({{5, 0}}));

    auto pipeline = buildDrainPipeline(std::move(chunks), *bitmaps, /*rows_budget=*/ 1ULL << 30);
    CompletedPipelineExecutor executor(pipeline);

    EXPECT_ANY_THROW(executor.execute());
#endif
}

#endif
