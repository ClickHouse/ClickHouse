#include <Processors/LimitRangeTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <base/arithmeticOverflow.h>
#include <algorithm>
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

LimitRangeTransform::BoundaryEvaluation::BoundaryEvaluation(
    const Block & header,
    ActionsDAG conditions,
    const std::optional<String> & start_column_name,
    const std::optional<String> & end_column_name,
    const ExpressionActionsSettings & actions_settings)
    : actions(std::make_shared<ExpressionActions>(std::move(conditions), actions_settings))
{
    for (const auto & column : actions->getRequiredColumnsWithTypes())
    {
        size_t position = header.getPositionByName(column.name);
        required_column_positions.push_back(position);
        input_header.insert(header.getByPosition(position));
    }

    action_input_positions = actions->getInputPositions(input_header);
    const auto & output_header = getOutputHeader();
    if (start_column_name)
        start_position = output_header.getPositionByName(*start_column_name);
    if (end_column_name)
        end_position = output_header.getPositionByName(*end_column_name);
}

const Block & LimitRangeTransform::BoundaryEvaluation::getOutputHeader() const
{
    /// Every input is consumed, so the sample header gives the result layout, including shared
    /// intermediates for a later stage, without executing stateful functions during setup.
    return actions->getSampleBlock();
}

Columns LimitRangeTransform::BoundaryEvaluation::evaluate(const Columns & columns, size_t num_rows) const
{
    Columns inputs;
    inputs.reserve(required_column_positions.size());
    for (size_t position : required_column_positions)
        inputs.push_back(columns[position]);

    /// The row count is passed explicitly: a condition without input columns still runs for every row.
    size_t rows = num_rows;
    auto result = actions->executeOnColumns(std::move(inputs), input_header, action_input_positions, rows);
    /// Boundary expressions cannot contain `arrayJoin`, so evaluation preserves the chunk's row count.
    chassert(rows == num_rows);
    chassert(result.size() == getOutputHeader().columns());
    return result;
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
    if (start_all)
    {
        combined_evaluation.emplace(header, conditions.clone(), start_column_name, end_column_name, actions_settings);
        return;
    }

    if (end_column_name)
    {
        ActionsDAG end_conditions = conditions.clone();
        end_conditions.removeUnusedActions(Names{*end_column_name});
        /// Skipping chunks changes the values of stateful and query-scope non-deterministic functions,
        /// including those inside lambdas. Evaluate both boundaries together until the range starts.
        const bool evaluate_end_before_start = start_column_name && end_conditions.hasNonDeterministicOrStatefulFunctions();
        /// `clone` retains the function objects, so the end-only stage continues their state after the start.
        end_only_evaluation.emplace(header, std::move(end_conditions), std::nullopt, end_column_name, actions_settings);
        if (evaluate_end_before_start)
        {
            combined_evaluation.emplace(header, conditions.clone(), start_column_name, end_column_name, actions_settings);
            return;
        }
    }

    if (start_column_name && end_column_name)
    {
        /// Carry the raw inputs through the start stage so `UNTIL` can read its own inputs as well as
        /// reuse shared subexpressions. Only the starting chunk needs to execute both stages.
        const auto * start_node = &conditions.findInOutputs(*start_column_name);
        std::unordered_set<const ActionsDAG::Node *> split_nodes(conditions.getInputs().begin(), conditions.getInputs().end());
        auto split = conditions.splitActionsForFilter(*start_column_name, std::move(split_nodes));
        const auto split_start_name = split.split_nodes_mapping.at(start_node)->result_name;
        start_only_evaluation.emplace(header, std::move(split.first), split_start_name, std::nullopt, actions_settings);
        split.second.removeUnusedActions(Names{*end_column_name});
        end_after_start_evaluation.emplace(
            start_only_evaluation->getOutputHeader(), std::move(split.second), std::nullopt, end_column_name, actions_settings);
    }
    else if (start_column_name)
    {
        start_only_evaluation.emplace(header, conditions.clone(), start_column_name, std::nullopt, actions_settings);
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
    if (combined_evaluation && (start_all || !started))
    {
        auto boundaries = combined_evaluation->evaluate(chunk.getColumns(), num_rows);
        start_col = combined_evaluation->getStartColumn(boundaries);
        end_col = combined_evaluation->getEndColumn(boundaries);
    }

    if (start_all)
    {
        chassert(combined_evaluation);
        transformAll(chunk, start_col, end_col);
        return;
    }

    rows_read += num_rows;

    Columns start_columns;
    if (!started && start_only_evaluation)
    {
        start_columns = start_only_evaluation->evaluate(chunk.getColumns(), num_rows);
        start_col = start_only_evaluation->getStartColumn(start_columns);
    }

    size_t output_start = 0;
    if (!started && start_col)
    {
        output_start = BoundaryColumnView(start_col).findTrue(0, num_rows);
        if (output_start == num_rows)
        {
            chunk.clear();
            return;
        }

        if (end_after_start_evaluation)
        {
            auto end_columns = end_after_start_evaluation->evaluate(start_columns, num_rows);
            end_col = end_after_start_evaluation->getEndColumn(end_columns);
        }
    }
    else if (end_only_evaluation)
    {
        auto end_columns = end_only_evaluation->evaluate(chunk.getColumns(), num_rows);
        end_col = end_only_evaluation->getEndColumn(end_columns);
    }
    started = true;

    /// `UNTIL` closes the range at its first match at or after the starting row.
    /// Matches before the range opens have no effect, including those in earlier chunks.
    size_t output_end = BoundaryColumnView(end_col).findTrue(output_start, num_rows);
    if (output_end == output_start)
    {
        if (output_end < num_rows)
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
