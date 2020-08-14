#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>

namespace DB
{

IMergingAlgorithmWithSharedChunks::IMergingAlgorithmWithSharedChunks(
    size_t num_inputs,
    SortDescription description_,
    WriteBuffer * out_row_sources_buf_,
    size_t max_row_refs)
    : description(std::move(description_))
    , chunk_allocator(num_inputs + max_row_refs)
    , cursors(num_inputs)
    , source_chunks(num_inputs)
    , out_row_sources_buf(out_row_sources_buf_)
{
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void IMergingAlgorithmWithSharedChunks::initialize(Chunks chunks)
{
    source_chunks.resize(chunks.size());

    for (size_t source_num = 0; source_num < source_chunks.size(); ++source_num)
    {
        if (!chunks[source_num])
            continue;

        prepareChunk(chunks[source_num]);

        auto & source_chunk = source_chunks[source_num];

        source_chunk = chunk_allocator.alloc(std::move(chunks[source_num]));
        cursors[source_num] = SortCursorImpl(source_chunk->getColumns(), description, source_num);

        source_chunk->all_columns = cursors[source_num].all_columns;
        source_chunk->sort_columns = cursors[source_num].sort_columns;
    }

    queue = SortingHeap<SortCursor>(cursors);
}

void IMergingAlgorithmWithSharedChunks::consume(Chunk chunk, size_t source_num)
{
    prepareChunk(chunk);

    auto & source_chunk = source_chunks[source_num];
    source_chunk = chunk_allocator.alloc(std::move(chunk));
    cursors[source_num].reset(source_chunk->getColumns(), {});

    source_chunk->all_columns = cursors[source_num].all_columns;
    source_chunk->sort_columns = cursors[source_num].sort_columns;

    queue.push(cursors[source_num]);
}

}
