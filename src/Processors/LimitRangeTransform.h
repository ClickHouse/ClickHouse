#pragma once

#include <optional>

#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Implements LIMIT [n] AFTER expr [ALL] [UNTIL expr].
 * Without ALL, outputs rows starting from the first row where start condition is true,
 * until the first row where end condition is true (exclusive) or limit is reached.
 * With ALL, outputs the union of all matching windows without duplicating rows.
 * If no start condition: output from first row.
 * If no end condition: output until limit or stream end.
 * If there is no limit length: no row cap.
 */
class LimitRangeTransform : public ISimpleTransform
{
public:
    LimitRangeTransform(
        SharedHeader header_,
        ExpressionActionsPtr start_expression_,
        const String & start_column_name_,
        ExpressionActionsPtr end_expression_,
        const String & end_column_name_,
        bool start_all_,
        std::optional<UInt64> limit_,
        bool always_read_till_end_ = false);

    String getName() const override { return "LimitRange"; }

    void transform(Chunk & chunk) override;

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override
    {
        rows_before_limit_at_least.swap(counter);
    }

private:
    /// Find first row where condition column is true (non-zero). Returns num_rows if none.
    static size_t findFirstTrue(const ColumnPtr & column, size_t num_rows);

    /// Slice chunk to rows [start_row, end_row)
    static void sliceChunk(Chunk & chunk, size_t start_row, size_t end_row);

    /// Filter chunk by arbitrary subset of rows.
    static void filterChunk(Chunk & chunk, const IColumn::Filter & filter, size_t filtered_rows);

    static bool isTrueAt(const ColumnPtr & column, size_t row_num);

    void transformAll(Chunk & chunk, const ColumnPtr & start_col, const ColumnPtr & end_col);

    /// Expression that evaluates the AFTER condition per row.
    ExpressionActionsPtr start_expression;
    /// Name of the boolean column produced by start_expression.
    String start_column_name;
    /// Expression that evaluates the UNTIL condition per row.
    ExpressionActionsPtr end_expression;
    /// Name of the boolean column produced by end_expression.
    String end_column_name;
    /// ALL mode: emit the union of all windows opened by AFTER matches.
    bool start_all = false;
    /// Maximum number of rows per window (nullopt = unlimited).
    std::optional<UInt64> limit;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// Keep reading input after output is done (for rows_before_limit_at_least).
    bool always_read_till_end = false;
    /// True once output is finished; remaining chunks are cleared or counted.
    bool done_outputting = false;

    /// Whether the AFTER condition has been met (non-ALL mode).
    bool started = false;
    /// Total rows emitted so far (non-ALL mode).
    UInt64 rows_output = 0;
    /// Total rows seen so far across all chunks (ALL mode, for absolute position tracking).
    UInt64 rows_read = 0;
    /// Absolute row position up to which the current/latest window extends (ALL mode).
    UInt64 repeated_window_end = 0;
    /// An AFTER match with no limit opened an unbounded window (ALL mode, no UNTIL yet).
    bool has_repeated_unbounded_window = false;

    /// Position of the start condition column in the block after executing start_expression.
    size_t start_column_position = 0;
    /// Position of the end condition column in the block after executing end_expression.
    size_t end_column_position = 0;

    /// Stops emitting rows. If always_read_till_end, keeps draining input to preserve row counts.
    void setDone();
};

}
