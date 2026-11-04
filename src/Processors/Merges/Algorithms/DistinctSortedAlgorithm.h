#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortCursor.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

/// Merges inputs sorted by `key_description`, which contains the distinct keys without collators, into one
/// row per key. If set, `already_emitted_flag_column` names the `UInt8` column that marks suppression rows,
/// as in external `DISTINCT` runs, and each input then contains only ordinary rows or only suppression rows.
/// Without it, all rows are ordinary. Suppression inputs need only the keys and the flag; ordinary
/// inputs also provide every output column by name. Ordinary chunks are unique under the key sort order;
/// suppression chunks may contain sort-equal keys.
/// Equal ordinary keys retain the first row in source order. Suppression keys are never emitted.
class DistinctSortedAlgorithm final : public IMergingAlgorithm
{
public:
    DistinctSortedAlgorithm(
        SharedHeaders input_headers,
        SharedHeader output_header_,
        SortDescription key_description,
        const std::optional<String> & already_emitted_flag_column,
        size_t max_block_size_rows_);

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
    /// Without the already-emitted flag, no row is a suppression row.
    bool isSuppressionRow(const SortCursorImpl & cursor, size_t row) const;

    /// Selects output columns while retaining chunk information and ownership of their values.
    Chunk projectOutput(Chunk chunk, size_t source_num) const;

    struct Source
    {
        SharedHeader header;
        ColumnNumbers output_positions;
        ColumnRawPtrs output_columns;
    };

    const SharedHeader output_header;
    /// The keys, followed by the already-emitted flag descending if there is one.
    SortDescription description;
    const bool has_already_emitted_flag;
    const size_t num_key_columns;
    const size_t max_block_size_rows;
    Inputs current_inputs;
    SortCursorImpls cursors;
    VectorWithMemoryTracking<Source> sources;
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
