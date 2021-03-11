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
    , sources(num_inputs)
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

void IMergingAlgorithmWithSharedChunks::initialize(Inputs inputs)
{
    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        if (!inputs[source_num].chunk)
            continue;

        prepareChunk(inputs[source_num].chunk);

        auto & source = sources[source_num];

        source.skip_last_row = inputs[source_num].skip_last_row;
        source.chunk = chunk_allocator.alloc(inputs[source_num].chunk);
        cursors[source_num] = SortCursorImpl(source.chunk->getColumns(), description, source_num);

        source.chunk->all_columns = cursors[source_num].all_columns;
        source.chunk->sort_columns = cursors[source_num].sort_columns;
    }

    queue = SortingHeap<SortCursor>(cursors);
}

void IMergingAlgorithmWithSharedChunks::consume(Input & input, size_t source_num)
{
    prepareChunk(input.chunk);

    auto & source = sources[source_num];
    source.skip_last_row = input.skip_last_row;
    source.chunk = chunk_allocator.alloc(input.chunk);
    cursors[source_num].reset(source.chunk->getColumns(), {});

    source.chunk->all_columns = cursors[source_num].all_columns;
    source.chunk->sort_columns = cursors[source_num].sort_columns;

    queue.push(cursors[source_num]);
}

}
