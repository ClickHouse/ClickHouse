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

    ExpressionActionsPtr start_expression;
    String start_column_name;
    ExpressionActionsPtr end_expression;
    String end_column_name;
    bool start_all = false;
    std::optional<UInt64> limit;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    bool always_read_till_end = false;
    bool done_outputting = false;

    bool started = false;
    UInt64 rows_output = 0;
    UInt64 rows_read = 0;
    UInt64 repeated_window_end = 0;
    bool has_repeated_unbounded_window = false;

    size_t start_column_position = 0;
    size_t end_column_position = 0;

    /// Stops emitting rows. If always_read_till_end, keeps draining input to preserve row counts.
    void setDone();
};

}
