#pragma once
#include <atomic>
#include <memory>
#include <optional>
#include <Core/Block_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>
#include <roaring/roaring.hh>

namespace DB
{

/// Work queue shared by the scorer worker sources.
struct ScorerSharedParts
{
    RangesInDataParts parts;
    std::optional<PerPartBitmaps> bitmaps;
    std::atomic<size_t> next_part_index{0};
};

using ScorerSharedPartsPtr = std::shared_ptr<ScorerSharedParts>;

/// Worker source of the row-scorer pipeline.
/// A bounded number of workers claim parts from the shared queue.
/// Each `generate` call scores at most one part and merges
/// the result into a worker-local top-K accumulator.
class ScorerSource final : public ISource
{
public:
    ScorerSource(
        SharedHeader output_header_,
        std::shared_ptr<RowScorer> scorer_,
        ScorerSharedPartsPtr shared_parts_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageMetadataPtr source_metadata_,
        ContextPtr context_);

    String getName() const override { return "ScorerSource"; }

protected:
    Chunk generate() override;

private:
    void checkCanUseIndexes(const RangesInDataPart & part_ranges) const;

    /// Merge one part's sorted scores into `top_rows`, converting part-local
    /// row ids to global row indexes and truncating to the scorer's top-K.
    void mergePartResult(RowScorer::ScoreResult part_result, UInt64 part_starting_offset);

    SharedHeader output_header;
    std::shared_ptr<RowScorer> scorer;
    ScorerSharedPartsPtr shared_parts;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageMetadataPtr source_metadata;
    ContextPtr context;

    /// Worker-local top-K: `(global_row_index, score)` sorted by the merge
    /// comparator (`_score` in the scorer's direction, row index ASC).
    RowScorer::ScoreResult top_rows;
    bool generated = false;
};

}
