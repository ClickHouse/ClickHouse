#include <Processors/LimitRangeTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <base/arithmeticOverflow.h>
#include <limits>

namespace DB
{

namespace
{

UInt64 saturatingAdd(UInt64 lhs, UInt64 rhs)
{
    UInt64 result = 0;
    if (common::addOverflow(lhs, rhs, result))
        return std::numeric_limits<UInt64>::max();
    return result;
}

/// Per-chunk view of a boundary condition column: the constant verdict or the byte mask is resolved
/// once per chunk via FilterDescription instead of dispatching on the column type for every row.
struct BoundaryColumnView
{
    bool always_false = true;
    bool always_true = false;
    std::optional<FilterDescription> mask;

    explicit BoundaryColumnView(const ColumnPtr & column)
    {
        if (!column)
            return;

        ConstantFilterDescription constant_description(*column);
        if (constant_description.always_false)
            return;

        always_false = false;
        if (constant_description.always_true)
        {
            always_true = true;
            return;
        }

        mask.emplace(*column);
    }

    bool isTrueAt(size_t row_num) const
    {
        if (always_false)
            return false;
        if (always_true)
            return true;
        return (*mask->data)[row_num];
    }

    /// The first row in `[begin, end)` where the condition is true, or `end`.
    size_t findTrue(size_t begin, size_t end) const
    {
        if (always_false)
            return end;
        if (always_true)
            return begin;

        /// Skip zero-filled blocks with the memcmp-based check, then finish byte by byte.
        const UInt8 * data = mask->data->data();
        constexpr size_t block_size = 64;
        while (begin + block_size <= end && memoryIsZero(data, begin, begin + block_size))
            begin += block_size;
        while (begin < end && !data[begin])
            ++begin;
        return begin;
    }
};

}

LimitRangeTransform::LimitRangeTransform(
    SharedHeader header_,
    ExpressionActionsPtr start_expression_,
    const String & start_column_name_,
    ExpressionActionsPtr end_expression_,
    const String & end_column_name_,
    bool start_all_,
    std::optional<UInt64> limit_,
    bool always_read_till_end_)
    : ISimpleTransform(header_, header_, true)
    , start_expression(std::move(start_expression_))
    , end_expression(std::move(end_expression_))
    , start_all(start_all_)
    , limit(limit_)
    , always_read_till_end(always_read_till_end_)
{
    if (limit && *limit == 0)
    {
        setDone();
        return;
    }

    if (start_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        start_expression->execute(block, /*dry_run=*/true);
        start_column_position = block.getPositionByName(start_column_name_);
    }
    if (end_expression)
    {
        Block block = getInputPort().getHeader().cloneEmpty();
        end_expression->execute(block, /*dry_run=*/true);
        end_column_position = block.getPositionByName(end_column_name_);
    }
}

void LimitRangeTransform::appendOutputRows(size_t begin, size_t end)
{
    if (begin >= end)
        return;

    if (!output_slices.empty() && output_slices.back().start + output_slices.back().length == begin)
        output_slices.back().length += end - begin;
    else
        output_slices.push_back({begin, end - begin});
}

void LimitRangeTransform::transformAll(Chunk & chunk, const ColumnPtr & start_col, const ColumnPtr & end_col)
{
    const size_t num_rows = chunk.getNumRows();
    output_slices.clear();

    const BoundaryColumnView start_view(start_col);
    const BoundaryColumnView end_view(end_col);

    /// Rows where neither boundary matches only continue the current window, so they are handled in bulk:
    /// the loop jumps from one matching row to the next and selects the rows in between as one slice.
    size_t row = 0;
    while (row < num_rows)
    {
        const size_t next_start = start_view.findTrue(row, num_rows);
        const size_t next_end = end_view.findTrue(row, std::min(next_start + 1, num_rows));
        const size_t event = std::min(next_start, next_end);

        if (has_repeated_unbounded_window)
            appendOutputRows(row, event);
        else if (limit && rows_read + row < repeated_window_end)
            appendOutputRows(row, static_cast<size_t>(std::min<UInt64>(event, repeated_window_end - rows_read)));

        if (event == num_rows)
            break;

        const UInt64 current_row = rows_read + event;
        const bool end_match = end_view.isTrueAt(event);
        if (end_match)
        {
            has_repeated_unbounded_window = false;
            repeated_window_end = current_row;
        }

        const bool start_match = start_view.isTrueAt(event);
        if (start_match && !end_match)
        {
            if (limit)
                repeated_window_end = std::max(repeated_window_end, saturatingAdd(current_row, *limit));
            else
                has_repeated_unbounded_window = true;
        }

        if (has_repeated_unbounded_window || (limit && current_row < repeated_window_end))
            appendOutputRows(event, event + 1);

        row = event + 1;
    }

    rows_read += num_rows;

    if (output_slices.empty())
    {
        chunk.clear();
        return;
    }

    materializeSlicesIntoChunk(chunk, chunk.detachColumns(), num_rows, output_slices);
}

void LimitRangeTransform::setDone()
{
    if (always_read_till_end)
        done_outputting = true;
    else
        stopReading();
}

IProcessor::Status LimitRangeTransform::prepare()
{
    /// A finished output normally closes the input at once, but `exact_rows_before_limit` promises the
    /// count of all rows before the range, so keep pulling and counting until the input is exhausted, as
    /// `LimitTransform` does. Nothing is pulled while no row was read yet: the sets of the query may not
    /// be built at that point.
    if (output.isFinished() && always_read_till_end && rows_read > 0 && !input.isFinished())
    {
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        auto chunk = input.pull(true);
        if (rows_before_limit_at_least)
            rows_before_limit_at_least->add(chunk.getNumRows());

        input.setNeeded();
        return Status::NeedData;
    }

    return ISimpleTransform::prepare();
}

void LimitRangeTransform::transform(Chunk & chunk)
{
    if (chunk.empty())
        return;

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(chunk.getNumRows());

    if (done_outputting)
    {
        chunk.clear();
        return;
    }

    const size_t num_rows = chunk.getNumRows();

    ColumnPtr start_col;
    if (start_expression && (start_all || !started))
    {
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        /// Pass the real chunk row count by reference (the `size_t & num_rows` overload) with
        /// dry_run = false; otherwise the boundary predicate is evaluated in dry-run mode and a
        /// zero-column block would mistakenly report 0 rows.
        size_t start_rows = num_rows;
        start_expression->execute(block, start_rows, /*dry_run=*/false);
        start_col = block.getByPosition(start_column_position).column;
    }

    ColumnPtr end_col;
    if (end_expression)
    {
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        size_t end_rows = num_rows;
        end_expression->execute(block, end_rows, /*dry_run=*/false);
        end_col = block.getByPosition(end_column_position).column;
    }

    if (start_all)
    {
        transformAll(chunk, start_col, end_col);
        return;
    }

    rows_read += num_rows;

    const BoundaryColumnView start_view(start_col);
    const BoundaryColumnView end_view(end_col);

    size_t output_start = 0;
    size_t output_end = num_rows;
    /// The first `UNTIL` match of the chunk, once it has been looked for.
    std::optional<size_t> first_end;

    if (!started)
    {
        if (start_col)
        {
            const size_t first_start = start_view.findTrue(0, num_rows);

            if (end_col)
            {
                first_end = end_view.findTrue(0, num_rows);
                /// UNTIL fired at or before AFTER (covers: AFTER not found, UNTIL at same row,
                /// UNTIL precedes AFTER).  The window is permanently closed.
                if (*first_end <= first_start)
                {
                    if (*first_end < num_rows)
                        setDone();
                    chunk.clear();
                    return;
                }
            }

            if (first_start >= num_rows)
            {
                chunk.clear();
                return;
            }

            started = true;
            output_start = first_start;
        }
        else
        {
            started = true;
        }
    }

    if (end_col && output_end > output_start)
    {
        if (!first_end)
            first_end = end_view.findTrue(0, num_rows);
        if (*first_end < num_rows)
            output_end = *first_end;
    }

    if (output_end <= output_start)
    {
        if (end_col && output_end < num_rows)
            setDone();

        chunk.clear();
        return;
    }

    if (limit)
    {
        /// rows_output < *limit here: reaching the limit immediately sets done, after which either
        /// done_outputting clears the chunk above or the input is closed and transform is not called.
        UInt64 remaining = *limit - rows_output;
        size_t take = output_end - output_start;
        if (take > remaining)
            output_end = output_start + remaining;
    }

    output_slices.assign(1, ChunkRowRange{output_start, output_end - output_start});
    rows_output += materializeSlicesIntoChunk(chunk, chunk.detachColumns(), num_rows, output_slices);

    if (limit && rows_output >= *limit)
        setDone();
    else if (end_col && output_end < num_rows)
        setDone();
}

}
