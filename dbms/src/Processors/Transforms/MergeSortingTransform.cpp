#include <Processors/Transforms/MergeSortingTransform.h>

#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>

#include <queue>


namespace ProfileEvents
{
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
}


namespace DB
{

/** Part of implementation. Merging array of ready (already read from somewhere) chunks.
  * Returns result of merge as stream of chunks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSorter
{
public:
    MergeSorter(Chunks & chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_ = 0);

    Chunk read();

private:
    Chunks & chunks;
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;
    size_t total_merged_rows = 0;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    bool has_collation = false;

    std::priority_queue<SortCursor> queue_without_collation;
    std::priority_queue<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
      *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
      */
    template <typename TSortCursor>
    Chunk mergeImpl(std::priority_queue<TSortCursor> & queue);
};

MergeSorter::MergeSorter(Chunks & chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_)
    : chunks(chunks_), description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    Chunks nonempty_chunks;
    for (auto & chunk : chunks)
    {
        if (chunk.getNumRows() == 0)
            continue;

        cursors.emplace_back(chunk.getColumns(), description);
        has_collation |= cursors.back().has_collation;

        nonempty_chunks.emplace_back(std::move(chunk));
    }

    chunks.swap(nonempty_chunks);

    if (!has_collation)
    {
        for (auto & cursor : cursors)
            queue_without_collation.push(SortCursor(&cursor));
    }
    else
    {
        for (auto & cursor : cursors)
            queue_with_collation.push(SortCursorWithCollation(&cursor));
    }
}


Chunk MergeSorter::read()
{
    if (chunks.empty())
        return Chunk();

    if (chunks.size() == 1)
    {
        auto res = std::move(chunks[0]);
        chunks.clear();
        return res;
    }

    return !has_collation
           ? mergeImpl<SortCursor>(queue_without_collation)
           : mergeImpl<SortCursorWithCollation>(queue_with_collation);
}


template <typename TSortCursor>
Chunk MergeSorter::mergeImpl(std::priority_queue<TSortCursor> & queue)
{
    size_t num_columns = chunks[0].getNumColumns();

    MutableColumns merged_columns = chunks[0].cloneEmptyColumns();
    /// TODO: reserve (in each column)

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        ++total_merged_rows;
        ++merged_rows;

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }

        if (limit && total_merged_rows == limit)
        {
            chunks.clear();
            return Chunk(std::move(merged_columns), merged_rows);
        }

        if (merged_rows == max_merged_block_size)
            return Chunk(std::move(merged_columns), merged_rows);
    }

    if (merged_rows == 0)
        return {};

    return Chunk(std::move(merged_columns), merged_rows);
}


MergeSortingTransform::MergeSortingTransform(
    const Block & header,
    SortDescription & description_,
    size_t max_merged_block_size_, UInt64 limit_,
    size_t max_bytes_before_remerge_,
    size_t max_bytes_before_external_sort_, const std::string & tmp_path_)
    : IAccumulatingTransform(header, header)
    , description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_path(tmp_path_)
{
    auto & sample = getInputPort().getHeader();

    /// Replace column names to column position in sort_description.
    for (auto & column_description : description)
    {
        if (!column_description.column_name.empty())
        {
            column_description.column_number = sample.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }

    /// Remove constants from header and map old indexes to new.
    size_t num_columns = sample.columns();
    ColumnNumbers map(num_columns, num_columns);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column = sample.getByPosition(pos);
        if (!(column.column && column.column->isColumnConst()))
        {
            map[pos] = header_without_constants.columns();
            header_without_constants.insert(column);
        }
    }

    /// Remove constants from column_description and remap positions.
    SortDescription description_without_constants;
    description_without_constants.reserve(description.size());
    for (const auto & column_description : description)
    {
        auto old_pos = column_description.column_number;
        auto new_pos = map[old_pos];
        if (new_pos < num_columns)
        {
            description_without_constants.push_back(column_description);
            description_without_constants.back().column_number = new_pos;
        }
    }

    description.swap(description_without_constants);
}

void MergeSortingTransform::consume(Chunk chunk)
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// If there were only const columns in sort description, then there is no need to sort.
    /// Return the chunk as is.
    if (description.empty())
    {
        setReadyChunk(std::move(chunk));
        return;
    }

    removeConstColumns(chunk);

    sum_rows_in_blocks += chunk.getNumRows();
    sum_bytes_in_blocks += chunk.allocatedBytes();
    chunks.push_back(std::move(chunk));

    /** If significant amount of data was accumulated, perform preliminary merging step.
      */
    if (chunks.size() > 1
        && limit
        && limit * 2 < sum_rows_in_blocks   /// 2 is just a guess.
        && remerge_is_useful
        && max_bytes_before_remerge
        && sum_bytes_in_blocks > max_bytes_before_remerge)
    {
        remerge();
    }

    /** If too many of them and if external sorting is enabled,
      *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
      * NOTE. It's possible to check free space in filesystem.
      */
    if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
    {
        Poco::File(tmp_path).createDirectories();
        temporary_files.emplace_back(std::make_unique<Poco::TemporaryFile>(tmp_path));
        const std::string & path = temporary_files.back()->path();
        WriteBufferFromFile file_buf(path);
        CompressedWriteBuffer compressed_buf(file_buf);
        NativeBlockOutputStream block_out(compressed_buf, 0, header_without_constants);
        MergeSorter merge_sorter(chunks, description, max_merged_block_size, limit);

        LOG_INFO(log, "Sorting and writing part of data into temporary file " + path);
        ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);

        /// NOTE. Possibly limit disk usage.
        /// NOTE. This should be another one processor.
        /// TODO: Rewrite this code when processors could be able to create another processors.
        block_out.writePrefix();
        while (auto next = merge_sorter.read())
        {
            auto block = header_without_constants.cloneWithColumns(next.detachColumns());
            block_out.write(block);
        }
        block_out.writeSuffix();

        LOG_INFO(log, "Done writing part of data into temporary file " + path);

        chunks.clear();
        sum_bytes_in_blocks = 0;
        sum_rows_in_blocks = 0;
    }
}

void MergeSortingTransform::remerge()
{
    LOG_DEBUG(log, "Re-merging intermediate ORDER BY data (" << chunks.size() << " blocks with " << sum_rows_in_blocks << " rows) to save memory consumption");

    /// NOTE Maybe concat all blocks and partial sort will be faster than merge?
    MergeSorter merge_sorter(chunks, description, max_merged_block_size, limit);

    Chunks new_chunks;
    size_t new_sum_rows_in_blocks = 0;
    size_t new_sum_bytes_in_blocks = 0;

    while (auto chunk = merge_sorter.read())
    {
        new_sum_rows_in_blocks += chunk.getNumRows();
        new_sum_bytes_in_blocks += chunk.allocatedBytes();
        new_chunks.emplace_back(chunk);
    }

    LOG_DEBUG(log, "Memory usage is lowered from "
            << formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks) << " to "
            << formatReadableSizeWithBinarySuffix(new_sum_bytes_in_blocks));

    /// If the memory consumption was not lowered enough - we will not perform remerge anymore. 2 is a guess.
    if (new_sum_bytes_in_blocks * 2 > sum_bytes_in_blocks)
        remerge_is_useful = false;

    chunks = std::move(new_chunks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}


void MergeSortingTransform::removeConstColumns(Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception("Block has different number of columns with header: " + toString(num_columns)
                        + " vs " + toString(const_columns_to_remove.size()), ErrorCodes::LOGICAL_ERROR);

    auto columns = chunk.detachColumns();
    Columns column_without_constants;
    column_without_constants.reserve(header_without_constants.columns());

    for (size_t position = 0; position < num_columns; ++position)
    {
        if (!const_columns_to_remove[position])
            column_without_constants.push_back(std::move(columns[position]));
    }

    chunk.setColumns(std::move(column_without_constants), num_rows);
}


}
