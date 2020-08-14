#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>


namespace DB
{

IMergingAlgorithmWithDelayedChunk::IMergingAlgorithmWithDelayedChunk(
    size_t num_inputs,
    SortDescription description_)
    : description(std::move(description_))
    , source_chunks(num_inputs)
    , cursors(num_inputs)
{
}

void IMergingAlgorithmWithDelayedChunk::initializeQueue(Chunks chunks)
{
    source_chunks = std::move(chunks);

    for (size_t source_num = 0; source_num < source_chunks.size(); ++source_num)
    {
        if (!source_chunks[source_num])
            continue;

        cursors[source_num] = SortCursorImpl(source_chunks[source_num].getColumns(), description, source_num);
    }

    queue = SortingHeap<SortCursor>(cursors);
}

void IMergingAlgorithmWithDelayedChunk::updateCursor(Chunk chunk, size_t source_num)
{
    auto & source_chunk = source_chunks[source_num];

    /// Extend lifetime of last chunk.
    last_chunk = std::move(source_chunk);
    last_chunk_sort_columns = std::move(cursors[source_num].sort_columns);

    source_chunk = std::move(chunk);
    cursors[source_num].reset(source_chunk.getColumns(), {});

    queue.push(cursors[source_num]);
}

}
