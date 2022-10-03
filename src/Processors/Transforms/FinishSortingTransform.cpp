#include <Processors/Transforms/FinishSortingTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool isPrefix(const SortDescription & pref_descr, const SortDescription & descr)
{
    if (pref_descr.size() > descr.size())
        return false;

    for (size_t i = 0; i < pref_descr.size(); ++i)
        if (pref_descr[i] != descr[i])
            return false;

    return true;
}

FinishSortingTransform::FinishSortingTransform(
    const Block & header,
    const SortDescription & description_sorted_,
    const SortDescription & description_to_sort_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    bool increase_sort_description_compile_attempts)
    : SortingTransform(header, description_to_sort_, max_merged_block_size_, limit_, increase_sort_description_compile_attempts)
{
    /// Check for sanity non-modified descriptions
    if (!isPrefix(description_sorted_, description_to_sort_))
        throw Exception("Can't finish sorting. SortDescription of already sorted stream is not prefix of "
            "SortDescription needed to sort", ErrorCodes::LOGICAL_ERROR);

    /// The target description is modified in SortingTransform constructor.
    /// To avoid doing the same actions with description_sorted just copy it from prefix of target description.
    for (const auto & column_sort_desc : description_sorted_)
        description_with_positions.emplace_back(column_sort_desc, header_without_constants.getPositionByName(column_sort_desc.column_name));
}

void FinishSortingTransform::consume(Chunk chunk)
{
    generated_prefix = false;

    // If there were only const columns in sort description, then there is no need to sort.
    // Return the chunks as is.
    if (description.empty())
    {
        generated_chunk = std::move(chunk);
        return;
    }

    removeConstColumns(chunk);

    /// Find the position of last already read key in current chunk.
    if (!chunks.empty())
    {
        size_t size = chunk.getNumRows();
        const auto & last_chunk = chunks.back();

        ssize_t low = -1;
        ssize_t high = size;
        while (high - low > 1)
        {
            ssize_t mid = (low + high) / 2;
            if (!less(last_chunk.getColumns(), chunk.getColumns(), last_chunk.getNumRows() - 1, mid, description_with_positions))
                low = mid;
            else
                high = mid;
        }

        size_t tail_pos = high;

        /// We need to save tail of chunk, because next chunk may starts with the same key as in tail
        /// and we should sort these rows in one portion.
        if (tail_pos != size)
        {
            auto source_columns = chunk.detachColumns();
            Columns tail_columns;

            for (auto & source_column : source_columns)
            {
                tail_columns.emplace_back(source_column->cut(tail_pos, size - tail_pos));
                source_column = source_column->cut(0, tail_pos);
            }

            chunks.emplace_back(std::move(source_columns), tail_pos);
            tail_chunk.setColumns(std::move(tail_columns), size - tail_pos);

            stage = Stage::Generate;
            return;
        }
    }

    /// If we reach here, that means that current chunk is first in portion
    /// or it all consists of rows with the same key as tail of a previous chunk.
    chunks.push_back(std::move(chunk));
}

void FinishSortingTransform::generate()
{
    if (!merge_sorter)
    {
        merge_sorter
            = std::make_unique<MergeSorter>(header_without_constants, std::move(chunks), description, max_merged_block_size, limit);
        generated_prefix = true;
    }

    generated_chunk = merge_sorter->read();

    if (!generated_chunk)
    {
        merge_sorter.reset();
        if (tail_chunk)
            chunks.push_back(std::move(tail_chunk));
        stage = Stage::Consume;
    }
    else
        enrichChunkWithConstants(generated_chunk);
}

}
