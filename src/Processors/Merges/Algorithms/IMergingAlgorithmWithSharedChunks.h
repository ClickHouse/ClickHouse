#pragma once
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/RowRef.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/Block_fwd.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>

namespace DB
{

class IMergingAlgorithmWithSharedChunks : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithSharedChunks(
        SharedHeader header_, size_t num_inputs, SortDescription description_, WriteBuffer * out_row_sources_buf_, size_t max_row_refs, std::unique_ptr<MergedData> merged_data_);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;

    MergedStats getMergedStats() const override { return merged_data->getMergedStats(); }

    size_t prev_unequal_column = 0;

private:
    SharedHeader header;
    SortDescription description;

    /// Allocator must be destroyed after source_chunks.
    detail::SharedChunkAllocator chunk_allocator;

    SortCursorImpls cursors;

protected:
    struct Source
    {
        detail::SharedChunkPtr chunk;
        bool skip_last_row{};
    };

    /// Sources currently being merged.
    using Sources = std::vector<Source>;
    Sources sources;
    std::vector<size_t> sources_origin_merge_tree_part_level;

    /// The batch queue identifies how many consecutive rows can be taken from the front
    /// cursor in one go (see `SortingQueueImpl::updateBatchSize`), so consuming rows one by
    /// one with `next(1)` restructures the queue once per batch instead of once per row.
    SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch> queue;

    /// Set by a derived algorithm before `initialize` when it can skip runs of equal keys within
    /// a batch (see `ReplacingSortedAlgorithm`). The batch detection is then enabled if some
    /// source actually starts with enough such runs; otherwise (and for algorithms that consume
    /// rows one by one) it is enabled only for expensive comparators, where the batches save
    /// comparisons. For cheap comparators on keys interleaved between the sources without
    /// runs (e.g. parts that each hold every key once) the detection is pure overhead.
    bool uses_runs_of_equal_keys = false;

    /// Whether some source (with a zero part level, read without a permutation) starts with
    /// enough runs of equal sort keys for skipping them to be cheaper than merging them row by
    /// row. A single duplicate is not enough: the probe for the end of a run runs on the rows
    /// outside runs too.
    bool sourcesHaveRunsWorthSkipping() const;

    /// Whether the queue detects batches longer than one row (decided in `initialize`).
    /// A batch of more than one row is not by itself evidence that the detection ran: with a
    /// single cursor left in the queue there is nothing to compare against, so its whole
    /// remainder is always reported as one batch. An algorithm that does extra work per batch
    /// must therefore test this flag rather than the batch size, or it would pay for batches
    /// exactly where the detection was disabled because they cannot pay off.
    bool batch_detection_enabled = false;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    std::unique_ptr<MergedData> merged_data;

    using RowRef = detail::RowRefWithOwnedChunk;
    void setRowRef(RowRef & row, SortCursor & cursor) { row.set(cursor, sources[cursor.impl->order].chunk); }
    bool skipLastRowFor(size_t input_number) const { return sources[input_number].skip_last_row; }
    bool rowsHaveDifferentSortColumns(const RowRef & lhs, const RowRef & rhs)
    {
        /// By the time this method is called, `sources_origin_merge_tree_part_level[lhs.source_stream_index]` must have been
        /// initialized in either `initialize` or `consume`
        if (lhs.source_stream_index == rhs.source_stream_index && sources_origin_merge_tree_part_level[lhs.source_stream_index] > 0)
            return true;

        auto first_non_equal = lhs.firstNonEqualSortColumnsWith(prev_unequal_column, rhs);

        if (first_non_equal < lhs.sort_columns->size())
        {
            prev_unequal_column = first_non_equal;
            return true;
        }

        return false;
    }
};

}
