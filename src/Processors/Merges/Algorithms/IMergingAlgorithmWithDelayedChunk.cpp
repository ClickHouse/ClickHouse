#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergeTreePartLevelInfo.h>


namespace DB
{

IMergingAlgorithmWithDelayedChunk::IMergingAlgorithmWithDelayedChunk(Block header_, size_t num_inputs, SortDescription description_)
    : description(std::move(description_))
    , header(std::move(header_))
    , current_inputs(num_inputs)
    , cursors(num_inputs)
    , inputs_origin_merge_tree_part_level(num_inputs)
{
}

void IMergingAlgorithmWithDelayedChunk::initializeQueue(Inputs inputs)
{
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        if (!current_inputs[source_num].chunk)
            continue;

        cursors[source_num] = SortCursorImpl(
            header, current_inputs[source_num].chunk.getColumns(), description, source_num, current_inputs[source_num].permutation);

        inputs_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(current_inputs[source_num].chunk);
    }

    queue = SortingQueue<SortCursor>(cursors);
}

void IMergingAlgorithmWithDelayedChunk::updateCursor(Input & input, size_t source_num)
{
    auto & current_input = current_inputs[source_num];

    /// Extend lifetime of last chunk.
    last_chunk.swap(current_input.chunk);
    last_chunk_sort_columns = std::move(cursors[source_num].sort_columns);

    current_input.swap(input);
    cursors[source_num].reset(current_input.chunk.getColumns(), header, current_input.permutation);

    inputs_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(current_input.chunk);

    queue.push(cursors[source_num]);
}

}
