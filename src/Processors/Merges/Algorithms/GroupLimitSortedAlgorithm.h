#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnNumbers.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortCursor.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

/// Merges inputs sorted by `key_description`, which contains the keys without collators, and keeps the input
/// rows whose rank within their group falls into `[group_offset, group_limit_end)`, as in the spill runs of a
/// group limit (`DISTINCT` or `LIMIT BY`). If set, `seen_rows_column` is the unsigned integer column of the
/// rows seen before spilling. An input that carries it holds only seen rows, whose counts are positive. An
/// input without it holds only input rows, and the merge appends zero counts of its type to every chunk, so
/// that all cursors compare the same columns. Without `seen_rows_column`, all rows are input rows. Seen
/// inputs need only the keys and the counts; input-row inputs also provide every output column by name. An
/// input-row chunk holds at most `group_limit_end` rows of a group, in source order; seen chunks may contain
/// sort-equal keys.
/// Within a group, seen rows come first and their counts start the rank; input rows follow in source order
/// and each takes the next rank. Seen rows are never emitted. With the window `[0, 1)` of `DISTINCT`, equal
/// keys retain the first input row in source order.
/// An output block ends after `max_block_size_rows` consumed rows, seen and skipped rows included, or with
/// the first row at which it holds at least `MergeSorter::MIN_BYTE_SIZED_BLOCK_ROWS` rows and its estimated
/// size reaches `max_block_size_bytes`, unless that is 0. The estimate counts each row as the average output
/// row of its input chunk, so it needs no per-row measurement. An input chunk forwarded whole stays one block
/// of its own size.
class GroupLimitSortedAlgorithm final : public IMergingAlgorithm
{
public:
    GroupLimitSortedAlgorithm(
        SharedHeaders input_headers,
        SharedHeader output_header_,
        SortDescription key_description,
        const std::optional<NameAndTypePair> & seen_rows_column,
        UInt64 group_offset_,
        UInt64 group_limit_end_,
        size_t max_block_size_rows_,
        size_t max_block_size_bytes_);

    /// The order of the inputs: the keys, followed by `seen_rows_column` descending if set, which places the seen rows
    /// of a group, whose counts are positive, before its input rows independently of input registration.
    static SortDescription inputSortDescription(SortDescription key_description, const std::optional<String> & seen_rows_column);

    [[nodiscard]] const char * getName() const override { return "GroupLimitSortedAlgorithm"; }
    void addInput(SharedHeader header);
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    [[nodiscard]] MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:
    /// The window `[0, 1)`: input-row chunks are unique, so batches are merged without comparing their rows.
    Status mergeDistinctRows();
    /// Any other window: rows are ranked one by one within their key.
    Status mergeRankedRows();

    /// Copies the boundary key before its input is replaced or forwarded to the output.
    void saveLastKey();
    /// Whether the output block has reached its row or byte bound (see the class comment).
    [[nodiscard]] bool isOutputBlockFull() const;
    /// Returns how many of the next `rows` rows of the current chunk of `source_num` to take: all of them, or
    /// those up to the row with which the output block reaches its byte bound (see the class comment). The
    /// caller guarantees that the block is not full and that `rows` is positive.
    [[nodiscard]] size_t rowsBeforeByteBound(size_t source_num, size_t rows) const;
    /// Inserts rows of the input-row chunk under `cursor` into the output block and adds their size estimate.
    void insertRows(const SortCursorImpl & cursor, size_t start, size_t length);
    /// Returns the accumulated output and resets the consumed-row count and the estimated size.
    Chunk pull();
    /// The rows seen before spilling that `row` of `cursor` stands for: zero for an input row, and for every row
    /// without the seen-rows column.
    [[nodiscard]] UInt64 seenRows(const SortCursorImpl & cursor, size_t row) const;

    /// Selects output columns while retaining chunk information and ownership of their values.
    [[nodiscard]] Chunk projectOutput(Chunk chunk, size_t source_num) const;

    /// Appends zero seen counts to a chunk of an input-row input.
    void addZeroSeenRows(Chunk & chunk) const;

    struct Source
    {
        /// The header of the input, with the appended seen counts of an input-row input.
        SharedHeader header;
        /// Whether the input holds seen rows, which its header shows by carrying the seen-rows column.
        bool holds_seen_rows = false;
        /// The positions of the output columns in `header`; empty for an input of seen rows.
        ColumnNumbers output_positions;
        /// The output columns of the current chunk, at `output_positions`.
        ColumnRawPtrs output_columns;
        /// The bytes of the output columns of the current chunk divided by its rows, rounded up; 0 for an
        /// input of seen rows.
        size_t average_row_bytes = 0;
    };

    const SharedHeader output_header;
    /// The seen-count column of the seen inputs, which the merge adds with zero counts to the input-row
    /// inputs that omit it.
    const std::optional<NameAndTypePair> seen_column;
    /// The order of the inputs (see `inputSortDescription`).
    SortDescription description;
    const size_t num_key_columns;
    const UInt64 group_offset;
    const UInt64 group_limit_end;
    const size_t max_block_size_rows;
    const size_t max_block_size_bytes;
    Inputs current_inputs;
    SortCursorImpls cursors;
    VectorWithMemoryTracking<Source> sources;
    SortingQueueBatch<SortCursor> queue;
    MergedData merged_data;

    /// Counts consumed rows, including seen and skipped rows, to bound the work between output chunks.
    size_t consumed_rows = 0;
    /// The estimated size of the rows in `merged_data`: each row counts as the average row of its chunk.
    size_t estimated_merged_bytes = 0;

    /// The last processed key includes seen rows. It refers to an owned input until that source is
    /// exhausted, then to the single-row columns saved below.
    detail::RowRef last_key;
    MutableColumns last_key_columns;
    ColumnRawPtrs last_key_column_ptrs;
    /// Rank of the next row of the last key: its seen counts plus its input rows consumed so far.
    UInt64 last_key_rank = 0;
};

}
