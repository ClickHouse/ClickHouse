#include <Processors/Merges/Algorithms/IMergingAlgorithmWithSharedChunks.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>

namespace DB
{

IMergingAlgorithmWithSharedChunks::IMergingAlgorithmWithSharedChunks(
    SharedHeader header_, size_t num_inputs, SortDescription description_, WriteBuffer * out_row_sources_buf_, size_t max_row_refs, std::unique_ptr<MergedData> merged_data_)
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
    removeReplicatedFromSortingColumns(header, inputs, description);
    removeConstAndSparse(inputs);
    merged_data->initialize(*header, inputs);

    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        if (!inputs[source_num].chunk)
            continue;

        auto & source = sources[source_num];

        source.skip_last_row = inputs[source_num].skip_last_row;
        source.chunk = chunk_allocator.alloc(inputs[source_num].chunk);
        cursors[source_num] = SortCursorImpl(
            *header, source.chunk->getColumns(), source.chunk->getNumRows(), description, source_num, inputs[source_num].permutation);

        source.chunk->all_columns = cursors[source_num].all_columns;
        source.chunk->sort_columns = cursors[source_num].sort_columns;

        sources_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(*source.chunk);
    }

    /// A single-column key without a collation compares cheaply even through the generic
    /// `SortCursor`, so it keeps the heap container (see `sortDescriptionCompareIsExpensive`),
    /// and the batches are detected only when the algorithm can make use of them.
    bool compare_is_expensive = sortDescriptionCompareIsExpensive(description);
    bool detect_batches = compare_is_expensive || (uses_runs_of_equal_keys && anySourceHasRunsOfEqualKeys());
    queue = SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch>(cursors, compare_is_expensive, detect_batches);
}

bool IMergingAlgorithmWithSharedChunks::anySourceHasRunsOfEqualKeys() const
{
    /// Checking the whole chunk would cost about as much as merging it, and the first rows are
    /// representative enough: a part written by a single `INSERT` either has its duplicates spread
    /// all over or none at all.
    static constexpr size_t max_rows_to_check = 8192;

    for (size_t source_num = 0; source_num < cursors.size(); ++source_num)
    {
        const auto & cursor = cursors[source_num];

        /// Parts with a non-zero level are assumed to have no duplicates (see `rowsHaveDifferentSortColumns`),
        /// and rows sorted through a permutation are not physically adjacent.
        if (cursor.empty() || cursor.permutation || sources_origin_merge_tree_part_level[source_num] > 0)
            continue;

        size_t rows_to_check = std::min(cursor.getSize(), max_rows_to_check);
        for (size_t row = 1; row < rows_to_check; ++row)
        {
            bool equal = true;
            for (const auto * column : cursor.sort_columns)
            {
                if (column->compareAt(row, row - 1, *column, /* nan_direction_hint = */ 1) != 0)
                {
                    equal = false;
                    break;
                }
            }

            if (equal)
                return true;
        }
    }

    return false;
}

void IMergingAlgorithmWithSharedChunks::consume(Input & input, size_t source_num)
{
    removeReplicatedFromSortingColumns(header, input, description);
    removeConstAndSparse(input);

    auto & source = sources[source_num];
    source.skip_last_row = input.skip_last_row;
    source.chunk = chunk_allocator.alloc(input.chunk);
    cursors[source_num].reset(source.chunk->getColumns(), *header, source.chunk->getNumRows(), input.permutation);

    source.chunk->all_columns = cursors[source_num].all_columns;
    source.chunk->sort_columns = cursors[source_num].sort_columns;

    sources_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(*source.chunk);

    queue.push(cursors[source_num]);
}

}
