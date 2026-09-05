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

/// The columns at `positions` of `columns`, named and typed as in `header`.
Block makeInputBlock(const Block & header, const Columns & columns, const std::vector<size_t> & positions)
{
    Block block;
    for (size_t position : positions)
    {
        const auto & header_column = header.getByPosition(position);
        block.insert(ColumnWithTypeAndName(columns[position], header_column.type, header_column.name));
    }
    return block;
}

}

LimitRangeTransform::BoundaryEvaluation::BoundaryEvaluation(
    const Block & header,
    ActionsDAG conditions,
    const std::optional<String> & start_column_name,
    const std::optional<String> & end_column_name,
    const ExpressionActionsSettings & actions_settings)
    : actions(std::make_shared<ExpressionActions>(std::move(conditions), actions_settings))
{
    for (const auto & column : actions->getRequiredColumnsWithTypes())
        input_positions.push_back(header.getPositionByName(column.name));

    /// A dry run over the empty header columns gives the positions of the boundary columns.
    Block block = makeInputBlock(header, header.getColumns(), input_positions);
    actions->execute(block, /*dry_run=*/true);
    if (start_column_name)
        start_position = block.getPositionByName(*start_column_name);
    if (end_column_name)
        end_position = block.getPositionByName(*end_column_name);
}

void LimitRangeTransform::BoundaryEvaluation::evaluate(
    const Block & header, const Columns & chunk_columns, size_t num_rows, ColumnPtr & start_column, ColumnPtr & end_column) const
{
    Block block = makeInputBlock(header, chunk_columns, input_positions);
    /// The row count is passed explicitly: a condition without input columns evaluates over an empty block.
    size_t rows = num_rows;
    actions->execute(block, rows, /*dry_run=*/false);
    if (start_position)
        start_column = block.getByPosition(*start_position).column;
    if (end_position)
        end_column = block.getByPosition(*end_position).column;
}

LimitRangeTransform::LimitRangeTransform(
    SharedHeader header_,
    const ActionsDAG & conditions,
    const std::optional<String> & start_column_name,
    const std::optional<String> & end_column_name,
    const ExpressionActionsSettings & actions_settings,
    bool start_all_,
    std::optional<UInt64> limit_,
    bool always_read_till_end_)
    : ISimpleTransform(header_, header_, true)
    , start_all(start_all_)
    , limit(limit_)
    , always_read_till_end(always_read_till_end_)
{
    if (limit && *limit == 0)
    {
        setDone();
        return;
    }

    const Block & header = getInputPort().getHeader();
    if (start_column_name || end_column_name)
        boundary_evaluation.emplace(header, conditions.clone(), start_column_name, end_column_name, actions_settings);

    if (!start_all && end_column_name)
    {
        ActionsDAG end_conditions = conditions.clone();
        end_conditions.removeUnusedActions(Names{*end_column_name});
        end_only_evaluation.emplace(header, std::move(end_conditions), std::nullopt, end_column_name, actions_settings);
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
    ColumnPtr end_col;
    /// Once the single range has started only `UNTIL` matters.
    const auto & evaluation = (start_all || !started) ? boundary_evaluation : end_only_evaluation;
    if (evaluation)
        evaluation->evaluate(getInputPort().getHeader(), chunk.getColumns(), num_rows, start_col, end_col);

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
