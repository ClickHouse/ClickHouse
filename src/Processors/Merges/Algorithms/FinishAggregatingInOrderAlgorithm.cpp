#include <Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithm.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/SortCursor.h>

#include <common/range.h>

namespace DB
{

FinishAggregatingInOrderAlgorithm::State::State(
    const Chunk & chunk, const SortDescription & desc)
    : num_rows(chunk.getNumRows())
    , all_columns(chunk.getColumns())
{
    if (!chunk)
        return;

    sorting_columns.reserve(desc.size());
    for (const auto & column_desc : desc)
        sorting_columns.emplace_back(all_columns[column_desc.column_number].get());
}

FinishAggregatingInOrderAlgorithm::FinishAggregatingInOrderAlgorithm(
    const Block & header_,
    size_t num_inputs_,
    AggregatingTransformParamsPtr params_,
    SortDescription description_,
    size_t max_block_size_)
    : header(header_)
    , num_inputs(num_inputs_)
    , params(params_)
    , description(std::move(description_))
    , max_block_size(max_block_size_)
{
    /// Replace column names in description to positions.
    for (auto & column_description : description)
    {
        if (!column_description.column_name.empty())
        {
            column_description.column_number = header_.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }
}

void FinishAggregatingInOrderAlgorithm::initialize(Inputs inputs)
{
    current_inputs = std::move(inputs);
    states.reserve(num_inputs);
    for (size_t i = 0; i < num_inputs; ++i)
        states.emplace_back(current_inputs[i].chunk, description);
}

void FinishAggregatingInOrderAlgorithm::consume(Input & input, size_t source_num)
{
    states[source_num] = State{input.chunk, description};
}

IMergingAlgorithm::Status FinishAggregatingInOrderAlgorithm::merge()
{
    if (!inputs_to_update.empty())
    {
        Status status(inputs_to_update.back());
        inputs_to_update.pop_back();
        return status;
    }

    /// Find the input with smallest last row.
    std::optional<size_t> best_input;
    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (!states[i].isValid())
            continue;

        if (!best_input
            || less(states[i].sorting_columns, states[*best_input].sorting_columns,
                    states[i].num_rows - 1, states[*best_input].num_rows - 1, description))
        {
            best_input = i;
        }
    }

    if (!best_input)
        return Status{aggregate(), true};

    /// Chunk at best_input will be aggregated entirely.
    auto & best_state = states[*best_input];
    best_state.to_row = states[*best_input].num_rows;

    /// Find the positions up to which need to aggregate in other chunks.
    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (!states[i].isValid() || i == *best_input)
            continue;

        auto indices = collections::range(states[i].current_row, states[i].num_rows);
        auto it = std::upper_bound(indices.begin(), indices.end(), best_state.num_rows - 1,
            [&](size_t lhs_pos, size_t rhs_pos)
            {
                return less(best_state.sorting_columns, states[i].sorting_columns, lhs_pos, rhs_pos, description);
            });

        states[i].to_row = (it == indices.end() ? states[i].num_rows : *it);
    }

    addToAggregation();

    /// At least one chunk should be fully aggregated.
    assert(!inputs_to_update.empty());
    Status status(inputs_to_update.back());
    inputs_to_update.pop_back();

    /// Do not merge blocks, if there are too few rows.
    if (accumulated_rows >= max_block_size)
        status.chunk = aggregate();

    return status;
}

Chunk FinishAggregatingInOrderAlgorithm::aggregate()
{
    auto aggregated = params->aggregator.mergeBlocks(blocks, false);
    blocks.clear();
    accumulated_rows = 0;
    return {aggregated.getColumns(), aggregated.rows()};
}

void FinishAggregatingInOrderAlgorithm::addToAggregation()
{
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & state = states[i];
        if (!state.isValid() || state.current_row == state.to_row)
            continue;

        if (state.to_row - state.current_row == state.num_rows)
        {
            blocks.emplace_back(header.cloneWithColumns(state.all_columns));
        }
        else
        {
            Columns new_columns;
            new_columns.reserve(state.all_columns.size());
            for (const auto & column : state.all_columns)
                new_columns.emplace_back(column->cut(state.current_row, state.to_row - state.current_row));

            blocks.emplace_back(header.cloneWithColumns(new_columns));
        }

        states[i].current_row = states[i].to_row;
        accumulated_rows += blocks.back().rows();

        if (!states[i].isValid())
            inputs_to_update.push_back(i);
    }
}

}
