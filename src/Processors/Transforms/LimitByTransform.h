#pragma once

#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>


namespace DB
{

/// Chunk-local half-open range of rows `[start, start + length)`.
struct ChunkRowRange
{
    UInt64 start = 0;
    UInt64 length = 0;
};


/// General `LIMIT BY` transform for input where equal grouping keys are not
/// guaranteed to be contiguous.
///
/// Example:
///     SELECT number, number % 3 AS g
///     FROM numbers(15)
///     ORDER BY number
///     LIMIT 2 BY g
///
/// Rows are processed in input order while keeping a running count for each
/// group across chunks with help of a HashTable. Within one chunk, equal-group rows
/// are handled as contiguous runs. For each run, the current count for that group tells
/// which rows are still skipped by `OFFSET` and which following rows can still be
/// emitted by `LIMIT`.
///
/// Extra memory: O(number of distinct groups seen so far).
class LimitByTransform final : public ISimpleTransform
{
public:
    LimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names);

    String getName() const override { return "LimitByTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    void processRun(UInt64 run_start_row, UInt64 run_row_count, size_t group_idx);

    template <typename Method>
    void consumeImpl(Method & hash_method, const ColumnRawPtrs & grouping_key_columns, UInt64 row_count);

    /// Positions of the non-constant grouping key columns in the chunk header.
    std::vector<size_t> grouping_key_positions;

    /// Kept per-group interval is `[group_offset, group_limit_end)`.
    const UInt64 group_offset;
    const UInt64 group_limit_end;

    AggregatedDataVariants data;
    ColumnsHashing::HashMethodContextPtr hash_method_context;

    /// The total number of input rows already seen for that group. This is useful to
    /// determine if the next row for that group should be outputted or not.
    std::vector<UInt64> group_counts;

    /// Slices from the current chunk that will be emitted to output.
    std::vector<ChunkRowRange> output_slices;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;
};


/// Sorted-stream variant: runs when the input is already sorted by the `LIMIT BY` key
/// (or a prefix of it), e.g.
///
/// Example:
///     SELECT number, number % 3 AS g
///     FROM numbers(15)
///     ORDER BY g, number
///     LIMIT 2 BY g
///
/// Rows are processed in input order while keeping a running count only for the
/// current group. Because equal-group rows are contiguous, each chunk can be
/// split into contiguous same-group runs by comparing neighboring rows and by
/// checking whether row 0 continues the previous chunk's last group. For each
/// run, the current count for that group tells which rows are still skipped by
/// `OFFSET` and which following rows can still be emitted by `LIMIT`.
///
/// Extra memory: O(1).
class LimitBySortedStreamTransform final : public ISimpleTransform
{
public:
    LimitBySortedStreamTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names);

    String getName() const override { return "LimitBySortedStreamTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    /// Return whether row 0 continues the same logical group as the previous
    /// chunk's last row.
    bool firstRowContinuesPreviousChunkGroup(const Columns & chunk_columns) const;

    void rememberLastGroupingKey(const Columns & chunk_columns, UInt64 row_idx);

    void processRun(UInt64 run_start_row, UInt64 run_row_count);

    /// Positions of the non-constant grouping key columns in the chunk header.
    std::vector<size_t> grouping_key_positions;

    /// Kept per-group interval is `[group_offset, group_limit_end)`.
    const UInt64 group_offset;
    const UInt64 group_limit_end;

    MutableColumns previous_chunk_last_grouping_key_columns;

    /// Number of rows already seen for the current logical group.
    UInt64 current_group_rows_seen = 0;

    /// Slices from the current chunk that will be emitted to output.
    std::vector<ChunkRowRange> output_slices;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;
};

}
