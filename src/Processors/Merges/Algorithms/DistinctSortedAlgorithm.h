#pragma once

#include <Core/SortCursor.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

/// Merges external `DISTINCT` runs. The sort description contains the distinct keys without collators,
/// followed by the already-emitted flag descending. Each input contains only ordinary rows or only
/// suppression rows. Ordinary chunks are unique under the key sort order; suppression chunks may
/// contain sort-equal keys.
/// Equal ordinary keys retain the first row in source order. Suppression keys are never emitted.
class DistinctSortedAlgorithm final : public IMergingAlgorithm
{
public:
    DistinctSortedAlgorithm(
        SharedHeader header_, size_t num_inputs, SortDescription description_,
        size_t flag_column_pos_, size_t max_block_size_rows_);

    const char * getName() const override { return "DistinctSortedAlgorithm"; }
    void addInput();
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:
    /// Copies the boundary key before its input is replaced or forwarded to the output.
    void saveLastKey();
    /// Returns the accumulated output and resets the consumed-row count.
    Chunk pull();

    const SharedHeader header;
    SortDescription description;
    const size_t flag_column_pos;
    const size_t num_key_columns;
    const size_t max_block_size_rows;
    Inputs current_inputs;
    SortCursorImpls cursors;
    std::vector<ColumnRawPtrs> output_columns;
    SortingQueueBatch<SortCursor> queue;
    MergedData merged_data;

    /// Counts consumed rows, including suppressed rows, to bound the work between output chunks.
    size_t consumed_rows = 0;

    /// The last processed key includes suppressed rows. It refers to an owned input until that
    /// source is exhausted, then to the single-row columns saved below.
    detail::RowRef last_key;
    MutableColumns last_key_columns;
    ColumnRawPtrs last_key_column_ptrs;
};

}
