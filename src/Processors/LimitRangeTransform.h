#pragma once

#include <optional>
#include <vector>

#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/Transforms/ChunkRowRange.h>

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
        bool always_read_till_end_);

    String getName() const override { return "LimitRange"; }

    Status prepare() override;
    void transform(Chunk & chunk) override;

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override
    {
        rows_before_limit_at_least.swap(counter);
    }

private:
    void transformAll(Chunk & chunk, const ColumnPtr & start_col, const ColumnPtr & end_col);

    /// Selects the rows `[begin, end)` of the current chunk for output, extending the last slice when they
    /// continue it; an empty range selects nothing.
    void appendOutputRows(size_t begin, size_t end);

    /// Stops emitting rows. If always_read_till_end, keeps draining input to preserve row counts.
    void setDone();

    /// Expression that evaluates the AFTER condition per row.
    ExpressionActionsPtr start_expression;
    /// Expression that evaluates the UNTIL condition per row.
    ExpressionActionsPtr end_expression;

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
    /// Total rows read so far; in ALL mode also the absolute position of the current chunk's first row.
    UInt64 rows_read = 0;
    /// Absolute row position up to which the current/latest window extends (ALL mode).
    UInt64 repeated_window_end = 0;
    /// An AFTER match with no limit opened an unbounded window (ALL mode, no UNTIL yet).
    bool has_repeated_unbounded_window = false;

    /// Position of the start condition column in the block after executing start_expression.
    size_t start_column_position = 0;
    /// Position of the end condition column in the block after executing end_expression.
    size_t end_column_position = 0;

    /// Rows of the current chunk selected for output; reused across chunks.
    std::vector<ChunkRowRange> output_slices;
};

}
