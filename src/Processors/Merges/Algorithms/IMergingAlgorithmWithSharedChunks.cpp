#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergeTreePartLevelInfo.h>

namespace DB
{

IMergingAlgorithmWithSharedChunks::IMergingAlgorithmWithSharedChunks(
    Block header_, size_t num_inputs, SortDescription description_, WriteBuffer * out_row_sources_buf_, size_t max_row_refs, std::unique_ptr<MergedData> merged_data_)
    : header(std::move(header_))
    , description(std::move(description_))
    , chunk_allocator(num_inputs + max_row_refs)
    , cursors(num_inputs)
    , sources(num_inputs)
    , sources_origin_merge_tree_part_level(num_inputs)
    , out_row_sources_buf(out_row_sources_buf_)
    , merged_data(std::move(merged_data_))
{
}

void IMergingAlgorithmWithSharedChunks::initialize(Inputs inputs)
{
    removeConstAndSparse(inputs);
    merged_data->initialize(header, inputs);

    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        if (!inputs[source_num].chunk)
            continue;

        auto & source = sources[source_num];

        source.skip_last_row = inputs[source_num].skip_last_row;
        source.chunk = chunk_allocator.alloc(inputs[source_num].chunk);
        cursors[source_num] = SortCursorImpl(header, source.chunk->getColumns(), description, source_num, inputs[source_num].permutation);

        source.chunk->all_columns = cursors[source_num].all_columns;
        source.chunk->sort_columns = cursors[source_num].sort_columns;

        sources_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(*source.chunk);
    }

    queue = SortingQueue<SortCursor>(cursors);
}

void IMergingAlgorithmWithSharedChunks::consume(Input & input, size_t source_num)
{
    removeConstAndSparse(input);

    auto & source = sources[source_num];
    source.skip_last_row = input.skip_last_row;
    source.chunk = chunk_allocator.alloc(input.chunk);
    cursors[source_num].reset(source.chunk->getColumns(), header, input.permutation);

    source.chunk->all_columns = cursors[source_num].all_columns;
    source.chunk->sort_columns = cursors[source_num].sort_columns;

    sources_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(*source.chunk);

    queue.push(cursors[source_num]);
}

}
