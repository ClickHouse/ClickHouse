#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortCursor.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

/// Merges inputs sorted by `key_description`, which contains the keys without collators, and keeps the input rows
/// whose rank within their group falls into `[group_offset, group_limit_end)`, as in the spill runs of a group limit
/// (`DISTINCT` or `LIMIT BY`). If set, `seen_rows_column` names the unsigned integer column of the rows seen before
/// spilling, and each input then contains only input rows, whose count is zero, or only seen rows, whose counts are
/// positive. Without it, all rows are input rows. Seen inputs need only the keys and the counts; input-row inputs
/// also provide every output column by name. An input-row chunk holds at most `group_limit_end` rows of a group, in
/// source order; seen chunks may contain sort-equal keys.
/// Within a group, seen rows come first and their counts start the rank; input rows follow in source order
/// and each takes the next rank. Seen rows are never emitted. With the window `[0, 1)` of `DISTINCT`, equal
/// keys retain the first input row in source order.
class GroupLimitSortedAlgorithm final : public IMergingAlgorithm
{
public:
    GroupLimitSortedAlgorithm(
        SharedHeaders input_headers,
        SharedHeader output_header_,
        SortDescription key_description,
        const std::optional<String> & seen_rows_column,
        UInt64 group_offset_,
        UInt64 group_limit_end_,
        size_t max_block_size_rows_);

    const char * getName() const override { return "GroupLimitSortedAlgorithm"; }
    void addInput(SharedHeader header);
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:
    /// The window `[0, 1)`: input-row chunks are unique, so batches are merged without comparing their rows.
    Status mergeDistinctRows();
    /// Any other window: rows are ranked one by one within their key.
    Status mergeRankedRows();

    /// Copies the boundary key before its input is replaced or forwarded to the output.
    void saveLastKey();
    /// Returns the accumulated output and resets the consumed-row count.
    Chunk pull();
    /// The rows seen before spilling that `row` of `cursor` stands for: zero for an input row, and for every row
    /// without the seen-rows column.
    UInt64 seenRows(const SortCursorImpl & cursor, size_t row) const;

    /// Selects output columns while retaining chunk information and ownership of their values.
    Chunk projectOutput(Chunk chunk, size_t source_num) const;

    struct Source
    {
        SharedHeader header;
        ColumnNumbers output_positions;
        ColumnRawPtrs output_columns;
    };

    const SharedHeader output_header;
    /// The keys, followed by the seen-rows column descending if there is one.
    SortDescription description;
    const bool has_seen_rows;
    const size_t num_key_columns;
    const UInt64 group_offset;
    const UInt64 group_limit_end;
    const size_t max_block_size_rows;
    Inputs current_inputs;
    SortCursorImpls cursors;
    VectorWithMemoryTracking<Source> sources;
    SortingQueueBatch<SortCursor> queue;
    MergedData merged_data;

    /// Counts consumed rows, including seen and skipped rows, to bound the work between output chunks.
    size_t consumed_rows = 0;

    /// The last processed key includes seen rows. It refers to an owned input until that source is
    /// exhausted, then to the single-row columns saved below.
    detail::RowRef last_key;
    MutableColumns last_key_columns;
    ColumnRawPtrs last_key_column_ptrs;
    /// Rank of the next row of the last key: its seen counts plus its input rows consumed so far.
    UInt64 last_key_rank = 0;
};

}
