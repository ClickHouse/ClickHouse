#include "config.h"

#if USE_USEARCH

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Port.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/ScoredSearch/ScorerSource.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>

using namespace DB;

namespace
{

SharedHeader makeNarrowOutputHeader()
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "__global_row_index"),
        ColumnWithTypeAndName(std::make_shared<DataTypeFloat32>(), "_score"),
    };
    return std::make_shared<const Block>(std::move(header));
}

/// Minimal `RowScorer` that returns canned `(row_id, score)` lists from
/// `scorePart`, one per call, ignoring the part / prefilter / context. Lets
/// the tests drive `ScorerSource` (which claims parts from the shared queue
/// and calls `scorePart` per part) without real data parts.
class FakeRowScorer final : public RowScorer
{
public:
    FakeRowScorer(std::vector<ScoreResult> per_part_rows_, size_t top_k_)
        : RowScorer(top_k_), per_part_rows(std::move(per_part_rows_))
    {
    }

    ScoreResult scorePart(const MergeTreeData::DataPartPtr &, const Prefilter &, ContextPtr) override
    {
        return per_part_rows.at(next_part++);
    }

    /// No indexes: disables the pending-mutations check in `ScorerSource`,
    /// which would otherwise dereference the null part and context.
    std::vector<MergeTreeIndexPtr> getIndexes() const override { return {}; }

    /// No arguments to validate: the canned scores are independent of the query.
    void validate(const ContextPtr &) const override {}

protected:
    ScoreDirection getSortDirection() const override { return ScoreDirection::Ascending; }

private:
    std::vector<ScoreResult> per_part_rows;
    size_t next_part = 0;
};

/// A part entry for the shared queue. The explicit-`ranges` ctor (not the
/// 4-arg one, which dereferences `data_part` to auto-fill marks) keeps a
/// null `data_part` safe here — `FakeRowScorer` ignores it.
RangesInDataPart makeFakePart(size_t part_index_in_query, UInt64 part_starting_offset)
{
    return RangesInDataPart(
        /*data_part_=*/ nullptr,
        /*parent_part_=*/ nullptr,
        part_index_in_query,
        part_starting_offset,
        /*ranges_=*/ {},
        /*read_hints_=*/ {});
}

ScorerSharedPartsPtr makeSharedParts(RangesInDataParts parts)
{
    auto shared_parts = std::make_shared<ScorerSharedParts>();
    shared_parts->parts = std::move(parts);
    return shared_parts;
}

/// Pull the single non-empty chunk out of the source. `ScorerSource` yields
/// empty chunks between parts and emits its accumulated top-K as one chunk
/// when the shared queue is exhausted, so the pulling executor sees exactly
/// one non-empty result (or zero when the scorer yields nothing).
Chunk pullSingleChunk(SourcePtr source)
{
    QueryPipeline pipeline(std::static_pointer_cast<ISource>(std::move(source)));
    PullingPipelineExecutor executor(pipeline);

    Chunk result;
    Chunk chunk;
    while (executor.pull(chunk))
    {
        if (chunk.getNumRows() > 0 && !result)
            result = std::move(chunk);
    }
    return result;
}

}

TEST(MergeTreeScoredSearchSource, EmitsGlobalRowIndexFromPartStartingOffset)
{
    constexpr UInt64 part_starting_offset = 1'000;
    const RowScorer::ScoreResult rows_with_scores = {
        {0, 0.1f},
        {5, 0.5f},
        {42, 4.2f},
    };

    auto narrow_header = makeNarrowOutputHeader();

    RangesInDataParts parts;
    parts.push_back(makeFakePart(/*part_index_in_query=*/ 7, part_starting_offset));

    auto source = std::make_shared<ScorerSource>(
        narrow_header,
        std::make_shared<FakeRowScorer>(std::vector<RowScorer::ScoreResult>{rows_with_scores}, /*top_k_=*/ 100),
        makeSharedParts(std::move(parts)),
        /*mutations_snapshot=*/ nullptr,
        /*source_metadata=*/ nullptr,
        /*context=*/ nullptr);

    auto chunk = pullSingleChunk(std::move(source));

    ASSERT_EQ(chunk.getNumRows(), rows_with_scores.size());
    ASSERT_EQ(chunk.getNumColumns(), 2u);

    const auto & cols = chunk.getColumns();
    const auto * global_row_index_col = typeid_cast<const ColumnUInt64 *>(cols[narrow_header->getPositionByName("__global_row_index")].get());
    const auto * score_col = typeid_cast<const ColumnFloat32 *>(cols[narrow_header->getPositionByName("_score")].get());
    ASSERT_TRUE(global_row_index_col);
    ASSERT_TRUE(score_col);

    for (size_t i = 0; i < rows_with_scores.size(); ++i)
    {
        EXPECT_EQ(global_row_index_col->getData()[i], part_starting_offset + rows_with_scores[i].first);
        EXPECT_FLOAT_EQ(score_col->getData()[i], rows_with_scores[i].second);
    }
}

TEST(MergeTreeScoredSearchSource, EmptyScorerOutputProducesNoRows)
{
    auto narrow_header = makeNarrowOutputHeader();

    RangesInDataParts parts;
    parts.push_back(makeFakePart(/*part_index_in_query=*/ 0, /*part_starting_offset=*/ 0));

    auto source = std::make_shared<ScorerSource>(
        narrow_header,
        std::make_shared<FakeRowScorer>(std::vector<RowScorer::ScoreResult>{{}}, /*top_k_=*/ 100),
        makeSharedParts(std::move(parts)),
        /*mutations_snapshot=*/ nullptr,
        /*source_metadata=*/ nullptr,
        /*context=*/ nullptr);

    auto chunk = pullSingleChunk(std::move(source));

    /// `pullSingleChunk` returns a default-constructed `Chunk` whenever the
    /// source did not produce any non-empty chunks — exactly the no-rows
    /// case `LimitTransform` and `LazyMaterializingTransform` are expected
    /// to handle by finishing immediately.
    EXPECT_FALSE(chunk);
}

TEST(MergeTreeScoredSearchSource, MergesPartsIntoLocalTopK)
{
    /// One worker drains two parts from the shared queue and must emit a
    /// single chunk: per-part results merged by `(score ASC, row index ASC)`,
    /// row ids shifted by the part starting offsets, truncated to top-K.
    const std::vector<RowScorer::ScoreResult> per_part_rows = {
        {{5, 0.5f}, {7, 0.9f}},
        {{1, 0.2f}, {3, 0.7f}},
    };

    auto narrow_header = makeNarrowOutputHeader();

    RangesInDataParts parts;
    parts.push_back(makeFakePart(/*part_index_in_query=*/ 0, /*part_starting_offset=*/ 0));
    parts.push_back(makeFakePart(/*part_index_in_query=*/ 1, /*part_starting_offset=*/ 1'000));

    auto source = std::make_shared<ScorerSource>(
        narrow_header,
        std::make_shared<FakeRowScorer>(per_part_rows, /*top_k_=*/ 3),
        makeSharedParts(std::move(parts)),
        /*mutations_snapshot=*/ nullptr,
        /*source_metadata=*/ nullptr,
        /*context=*/ nullptr);

    auto chunk = pullSingleChunk(std::move(source));

    /// Global merge order: (1001, 0.2), (5, 0.5), (1003, 0.7), (7, 0.9);
    /// top-K = 3 drops the last row.
    const RowScorer::ScoreResult expected = {
        {1'001, 0.2f},
        {5, 0.5f},
        {1'003, 0.7f},
    };

    ASSERT_EQ(chunk.getNumRows(), expected.size());

    const auto & cols = chunk.getColumns();
    const auto * global_row_index_col = typeid_cast<const ColumnUInt64 *>(cols[narrow_header->getPositionByName("__global_row_index")].get());
    const auto * score_col = typeid_cast<const ColumnFloat32 *>(cols[narrow_header->getPositionByName("_score")].get());
    ASSERT_TRUE(global_row_index_col);
    ASSERT_TRUE(score_col);

    for (size_t i = 0; i < expected.size(); ++i)
    {
        EXPECT_EQ(global_row_index_col->getData()[i], expected[i].first);
        EXPECT_FLOAT_EQ(score_col->getData()[i], expected[i].second);
    }
}

#endif
