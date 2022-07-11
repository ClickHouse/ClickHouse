#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>


namespace DB
{

IMergingAlgorithmWithDelayedChunk::IMergingAlgorithmWithDelayedChunk(
    size_t num_inputs,
    SortDescription description_)
    : description(std::move(description_))
    , current_inputs(num_inputs)
    , cursors(num_inputs)
{
}

void IMergingAlgorithmWithDelayedChunk::initializeQueue(Inputs inputs)
{
    current_inputs = std::move(inputs);

    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        if (!current_inputs[source_num].chunk)
            continue;

        cursors[source_num] = SortCursorImpl(current_inputs[source_num].chunk.getColumns(), description, source_num, current_inputs[source_num].permutation);
    }

    queue = SortingHeap<SortCursor>(cursors);
}

void IMergingAlgorithmWithDelayedChunk::updateCursor(Input & input, size_t source_num)
{
    auto & current_input = current_inputs[source_num];

    /// Extend lifetime of last chunk.
    last_chunk.swap(current_input.chunk);
    last_chunk_sort_columns = std::move(cursors[source_num].sort_columns);

    current_input.swap(input);
    cursors[source_num].reset(current_input.chunk.getColumns(), {}, current_input.permutation);

    queue.push(cursors[source_num]);
}

}
