#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>


namespace DB
{

IMergingAlgorithmWithDelayedChunk::IMergingAlgorithmWithDelayedChunk(SharedHeader header_, size_t num_inputs, SortDescription description_)
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
            *header,
            current_inputs[source_num].chunk.getColumns(),
            current_inputs[source_num].chunk.getNumRows(),
            description,
            source_num,
            current_inputs[source_num].permutation);

        inputs_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(current_inputs[source_num].chunk);
    }

    /// A single-column key without a collation compares cheaply even through the generic
    /// `SortCursor`, so it keeps the heap container (see `sortDescriptionCompareIsExpensive`).
    /// The algorithms consume rows one by one, so the batches only pay off by saving
    /// comparisons: with a cheap comparator the detection is skipped, unless the algorithm takes long batches whole.
    bool compare_is_expensive = sortDescriptionCompareIsExpensive(description);
    has_long_batches_of_unique_keys = uses_batches_of_unique_keys && sourcesHaveLongBatchesOfUniqueKeys();
    queue = SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch>(
        cursors, compare_is_expensive, compare_is_expensive || has_long_batches_of_unique_keys);
}

bool IMergingAlgorithmWithDelayedChunk::singleSourceHasMostlyDistinctKeys() const
{
    static constexpr size_t max_rows_to_check = 1024;

    const SortCursorImpl * source = nullptr;
    for (const auto & cursor : cursors)
    {
        if (cursor.empty())
            continue;
        if (source)
            return false;
        source = &cursor;
    }
    if (!source)
        return false;

    const size_t rows_to_check = std::min(source->rowsLeft() / max_share_of_rows_to_check, max_rows_to_check);
    if (rows_to_check < min_batch_size_to_take)
        return false;

    const auto row_at = [&](size_t pos) { return source->permutation ? (*source->permutation)[pos] : pos; };
    const IColumn ** sort_columns = const_cast<const IColumn **>(source->sort_columns.data());
    const size_t begin = source->getSize() - source->rowsLeft();
    const size_t end = begin + rows_to_check;
    size_t distinct = 0;
    for (size_t pos = begin; pos < end; ++pos)
        distinct += !detail::RowRef::checkEquals(source->sort_columns.size(), sort_columns, row_at(pos), sort_columns, row_at(pos + 1));

    return distinct * 2 >= rows_to_check;
}

bool IMergingAlgorithmWithDelayedChunk::sourcesHaveLongBatchesOfUniqueKeys() const
{
    static constexpr size_t max_rows_to_check = 1024;

    bool has_source_with_unique_keys = false;
    for (size_t source_num = 0; source_num < cursors.size(); ++source_num)
    {
        const auto & cursor = cursors[source_num];
        if (!cursor.empty() && !cursor.permutation && sourceHasUniqueKeys(source_num))
            has_source_with_unique_keys = true;
    }
    if (!has_source_with_unique_keys)
        return false;

    size_t total_rows = 0;
    for (const auto & cursor : cursors)
        if (!cursor.empty())
            total_rows += cursor.rowsLeft();

    const size_t rows_to_check = std::min(total_rows / max_share_of_rows_to_check, max_rows_to_check);
    if (rows_to_check < min_batch_size_to_take)
        return false;

    SortCursorImpls sample_cursors = cursors;
    SortingQueueForCursor<SortCursor, SortingQueueStrategy::Batch> sample_queue(
        sample_cursors, /*enable_array_container=*/ false, /*enable_batch_detection=*/ true);

    size_t rows = 0;
    size_t rows_in_long_batches = 0;
    while (sample_queue.isValid() && rows < rows_to_check)
    {
        auto [cursor, batch_size] = sample_queue.current();
        rows += batch_size;
        if (batch_size >= min_batch_size_to_take && !(*cursor)->permutation && sourceHasUniqueKeys((*cursor)->order))
            rows_in_long_batches += batch_size - 2;

        /// The next chunk of the source is not available here; a batch up to the end of the chunk is at least that long.
        if ((*cursor)->isLast(batch_size))
            break;

        sample_queue.next(batch_size);
    }

    return rows_in_long_batches * 2 >= rows && rows_in_long_batches > 0;
}

void IMergingAlgorithmWithDelayedChunk::updateCursor(Input & input, size_t source_num)
{
    auto & current_input = current_inputs[source_num];

    /// Extend lifetime of last chunk.
    last_chunk.swap(current_input.chunk);
    last_chunk_sort_columns = std::move(cursors[source_num].sort_columns);

    current_input.swap(input);
    cursors[source_num].reset(current_input.chunk.getColumns(), *header, current_input.chunk.getNumRows(), current_input.permutation);

    inputs_origin_merge_tree_part_level[source_num] = getPartLevelFromChunk(current_input.chunk);

    queue.push(cursors[source_num]);
}

}
