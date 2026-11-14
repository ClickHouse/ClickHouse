#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

class IMergingAlgorithmWithDelayedChunk : public IMergingAlgorithm
{
public:
    IMergingAlgorithmWithDelayedChunk(SharedHeader header_, size_t num_inputs, SortDescription description_);

    size_t prev_unequal_column = 0;

protected:
    /// The batch queue identifies how many consecutive rows can be taken from the front
    /// cursor in one go (see `SortingQueueImpl::updateBatchSize`), so consuming rows one by
    /// one with `next(1)` restructures the queue once per batch instead of once per row.
    SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch> queue;
    SortDescription description;

    /// Previous row. May refer to last_chunk_sort_columns or row from source_chunks.
    detail::RowRef last_key;

    ColumnRawPtrs last_chunk_sort_columns; /// Point to last_chunk if valid.

    void initializeQueue(Inputs inputs);
    void updateCursor(Input & input, size_t source_num);
    bool skipLastRowFor(size_t input_number) const { return current_inputs[input_number].skip_last_row; }
    void setRowRef(detail::RowRef & row, SortCursor & cursor) { row.set(cursor); }
    bool rowsHaveDifferentSortColumns(const detail::RowRef & lhs, const detail::RowRef & rhs)
    {
        /// By the time this method is called, `inputs_origin_merge_tree_part_level[lhs.source_stream_index]` must have been
        /// initialized in either `initializeQueue` or `updateCursor`
        if (lhs.source_stream_index == rhs.source_stream_index && sourceHasUniqueKeys(lhs.source_stream_index))
            return true;

        auto first_non_equal = lhs.firstNonEqualSortColumnsWith(prev_unequal_column, rhs);

        if (first_non_equal < lhs.num_columns)
        {
            prev_unequal_column = first_non_equal;
            return true;
        }

        return false;
    }

    SharedHeader header;

    /// Set by a derived algorithm before `initializeQueue` when it takes the rows inside a batch from a
    /// source without duplicate keys (a part with a non-zero level, see `sourceHasUniqueKeys`)
    /// in one go (see `SummingSortedAlgorithm`). The batch detection is then enabled also for cheap
    /// comparators if such batches cover enough rows (see `sourcesHaveLongBatchesOfUniqueKeys`).
    bool uses_batches_of_unique_keys = false;

    /// Whether most rows at the beginning of the merge come in long batches from sources without duplicate keys
    /// (decided in `initializeQueue` if `uses_batches_of_unique_keys`, see `sourcesHaveLongBatchesOfUniqueKeys`).
    bool has_long_batches_of_unique_keys = false;

    /// The smallest batch from such a source that is taken in one go: shorter ones save too little.
    static constexpr size_t min_batch_size_to_take = 8;
    /// At most this share of the rows is sampled, as in `sourcesHaveRunsWorthSkipping`.
    static constexpr size_t max_share_of_rows_to_check = 8;

    /// Whether in the beginning of the only source with a chunk most rows have a key different from the next row
    /// (see `SummingSortedAlgorithm`, which takes such rows in one go when there is a single source).
    bool singleSourceHasMostlyDistinctKeys() const;

    /// Whether the current chunk of the source comes from a part without duplicate keys: a part with a non-zero level.
    /// Both `rowsHaveDifferentSortColumns` and the batches of unique keys rely on this one condition.
    bool sourceHasUniqueKeys(size_t source_num) const { return inputs_origin_merge_tree_part_level[source_num] > 0; }

private:
    /// Whether most rows at the beginning of the merge come in batches of at least `min_batch_size_to_take`
    /// rows from sources without duplicate keys.
    bool sourcesHaveLongBatchesOfUniqueKeys() const;

    /// Inputs currently being merged.
    Inputs current_inputs;
    SortCursorImpls cursors;

    VectorWithMemoryTracking<size_t> inputs_origin_merge_tree_part_level;

    /// In merging algorithm, we need to compare current sort key with the last one.
    /// So, sorting columns for last row needed to be stored.
    /// In order to do it, we extend lifetime of last chunk and it's sort columns (from corresponding sort cursor).
    Chunk last_chunk;
};

}
