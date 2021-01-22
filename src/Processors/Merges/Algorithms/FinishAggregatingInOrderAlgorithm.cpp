#include <Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithm.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Core/SortCursor.h>

#include <ext/range.h>

namespace DB
{

FinishAggregatingInOrderAlgorithm::State::State(
    const Chunk & chunk, const SortDescription & desc)
    : num_rows(chunk.getNumRows())
    , all_columns(chunk.getColumns())
{
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
    : merged_data(header_.cloneEmptyColumns(), false, max_block_size_)
    , header(header_)
    , num_inputs(num_inputs_)
    , params(params_)
    , description(description_)
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
        return Status{merged_data.pull(), true};

    auto & best_state = states[*best_input];
    best_state.to_row = states[*best_input].num_rows;

    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (!states[i].isValid() || i == *best_input)
            continue;

        auto indices = ext::range(states[i].current_row, states[i].num_rows);
        auto it = std::upper_bound(indices.begin(), indices.end(), best_state.num_rows - 1,
            [&](size_t lhs_pos, size_t rhs_pos)
            {
                return less(best_state.sorting_columns, states[i].sorting_columns, lhs_pos, rhs_pos, description);
            });

        states[i].to_row = (it == indices.end() ? states[i].num_rows : *it);
    }

    auto aggregated = aggregate();
    for (size_t i = 0; i < aggregated.rows(); ++i)
        merged_data.insertRow(aggregated.getColumns(), i, aggregated.rows());

    Status status(*best_input);
    if (merged_data.hasEnoughRows())
        status.chunk = merged_data.pull();

    return status;
}

Block FinishAggregatingInOrderAlgorithm::aggregate()
{
    BlocksList blocks;

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & state = states[i];
        if (!state.isValid())
            continue;

        if (state.current_row == 0 && state.to_row == state.num_rows)
        {
            blocks.emplace_back(header.cloneWithColumns(states[i].all_columns));
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
    }

    return params->aggregator.mergeBlocks(blocks, false);
}

}
