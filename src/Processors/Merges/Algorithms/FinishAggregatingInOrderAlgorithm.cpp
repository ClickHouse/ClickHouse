#include <Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithm.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Core/SortCursor.h>

#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FinishAggregatingInOrderAlgorithm::State::State(const Chunk & chunk, const SortDescriptionWithPositions & desc, Int64 total_bytes_)
    : all_columns(chunk.getColumns()), num_rows(chunk.getNumRows()), total_bytes(total_bytes_)
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
    const SortDescription & description_,
    size_t max_block_size_,
    size_t max_block_bytes_)
    : header(header_), num_inputs(num_inputs_), params(params_), max_block_size(max_block_size_), max_block_bytes(max_block_bytes_)
{
    for (const auto & column_description : description_)
        description.emplace_back(column_description, header_.getPositionByName(column_description.column_name));
}

void FinishAggregatingInOrderAlgorithm::initialize(Inputs inputs)
{
    current_inputs = std::move(inputs);
    states.resize(num_inputs);
    for (size_t i = 0; i < num_inputs; ++i)
        consume(current_inputs[i], i);
}

void FinishAggregatingInOrderAlgorithm::consume(Input & input, size_t source_num)
{
    if (!input.chunk.hasRows())
        return;

    const auto & info = input.chunk.getChunkInfo();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in FinishAggregatingInOrderAlgorithm");

    const auto * arenas_info = typeid_cast<const ChunkInfoWithAllocatedBytes *>(info.get());
    if (!arenas_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have ChunkInfoWithAllocatedBytes in FinishAggregatingInOrderAlgorithm");

    states[source_num] = State{input.chunk, description, arenas_info->allocated_bytes};
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
        return Status(prepareToMerge(), true);

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

    /// Do not merge blocks, if there are too few rows or bytes.
    if (accumulated_rows >= max_block_size || accumulated_bytes >= max_block_bytes)
        status.chunk = prepareToMerge();

    return status;
}

Chunk FinishAggregatingInOrderAlgorithm::prepareToMerge()
{
    accumulated_rows = 0;
    accumulated_bytes = 0;

    auto info = std::make_shared<ChunksToMerge>();
    info->chunks = std::make_unique<Chunks>(std::move(chunks));

    Chunk chunk;
    chunk.setChunkInfo(std::move(info));
    return chunk;
}

void FinishAggregatingInOrderAlgorithm::addToAggregation()
{
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & state = states[i];
        if (!state.isValid() || state.current_row == state.to_row)
            continue;

        size_t current_rows = state.to_row - state.current_row;
        if (current_rows == state.num_rows)
        {
            chunks.emplace_back(state.all_columns, current_rows);
        }
        else
        {
            Columns new_columns;
            new_columns.reserve(state.all_columns.size());
            for (const auto & column : state.all_columns)
                new_columns.emplace_back(column->cut(state.current_row, current_rows));

            chunks.emplace_back(std::move(new_columns), current_rows);
        }

        chunks.back().setChunkInfo(std::make_shared<AggregatedChunkInfo>());
        states[i].current_row = states[i].to_row;

        /// We assume that sizes in bytes of rows are almost the same.
        accumulated_bytes += states[i].total_bytes * (static_cast<double>(current_rows) / states[i].num_rows);
        accumulated_rows += current_rows;


        if (!states[i].isValid())
            inputs_to_update.push_back(i);
    }
}

}
