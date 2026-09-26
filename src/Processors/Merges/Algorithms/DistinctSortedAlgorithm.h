#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/SortCursor.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

/// Merges sorted external `DISTINCT` runs and removes duplicate keys. Suppression rows identify keys
/// already emitted before spilling; they suppress matching ordinary rows and are never emitted.
/// The sort description contains the distinct keys without collators, followed by the already-emitted
/// flag descending and an optional arrival number ascending. Key equality excludes the flag and
/// arrival number. When arrival numbers are present, equal ordinary keys retain the earliest row
/// independently of source order. Otherwise, any representative can survive.
/// Each input contains only ordinary rows or only suppression rows. Suppression inputs need only the
/// sort columns; ordinary inputs also provide every output column by name. Ordinary chunks are unique
/// under the key sort order; suppression chunks may contain sort-equal keys.
class DistinctSortedAlgorithm final : public IMergingAlgorithm
{
public:
    DistinctSortedAlgorithm(
        SharedHeaders input_headers, SharedHeader output_header_, SortDescription description_,
        size_t num_key_columns_, size_t max_block_size_rows_);

    const char * getName() const override { return "DistinctSortedAlgorithm"; }
    void addInput(SharedHeader header);
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:

    /// Copies the boundary key before its input is replaced or forwarded to the output.
    void saveLastKey();

    /// Returns the accumulated output and resets the consumed-row count.
    Chunk pull();

    /// Selects output columns while retaining chunk information and ownership of their values.
    Chunk projectOutput(Chunk chunk, size_t source_num) const;

    struct Source
    {
        SharedHeader header;
        ColumnNumbers output_positions;
        ColumnRawPtrs output_columns;
    };

    const SharedHeader output_header;
    SortDescription description;
    const size_t num_key_columns;
    const size_t max_block_size_rows;
    Inputs current_inputs;
    SortCursorImpls cursors;
    std::vector<Source> sources;
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
