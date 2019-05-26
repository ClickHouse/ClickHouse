#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/processConstants.h>
#include <Common/formatReadable.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/sortBlock.h>


namespace ProfileEvents
{
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
}

namespace DB
{

MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const BlockInputStreamPtr & input, SortDescription & description_,
    size_t max_merged_block_size_, UInt64 limit_, size_t max_bytes_before_remerge_,
    size_t max_bytes_before_external_sort_, const std::string & tmp_path_)
    : description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_),
    max_bytes_before_remerge(max_bytes_before_remerge_),
    max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_path(tmp_path_)
{
    children.push_back(input);
    header = children.at(0)->getHeader();
    header_without_constants = header;
    removeConstantsFromBlock(header_without_constants);
    removeConstantsFromSortDescription(header, description);
}


Block MergeSortingBlockInputStream::readImpl()
{
    /** Algorithm:
      * - read to memory blocks from source stream;
      * - if too many of them and if external sorting is enabled,
      *   - merge all blocks to sorted stream and write it to temporary file;
      * - at the end, merge all sorted streams from temporary files and also from rest of blocks in memory.
      */

    /// If has not read source blocks.
    if (!impl)
    {
        while (Block block = children.back()->read())
        {
            /// If there were only const columns in sort description, then there is no need to sort.
            /// Return the blocks as is.
            if (description.empty())
                return block;

            removeConstantsFromBlock(block);

            blocks.push_back(block);
            sum_rows_in_blocks += block.rows();
            sum_bytes_in_blocks += block.allocatedBytes();

            /** If significant amount of data was accumulated, perform preliminary merging step.
              */
            if (blocks.size() > 1
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
                MergeSortingBlocksBlockInputStream block_in(blocks, description, max_merged_block_size, limit);

                LOG_INFO(log, "Sorting and writing part of data into temporary file " + path);
                ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
                copyData(block_in, block_out, &is_cancelled);    /// NOTE. Possibly limit disk usage.
                LOG_INFO(log, "Done writing part of data into temporary file " + path);

                blocks.clear();
                sum_bytes_in_blocks = 0;
                sum_rows_in_blocks = 0;
            }
        }

        if ((blocks.empty() && temporary_files.empty()) || isCancelledOrThrowIfKilled())
            return Block();

        if (temporary_files.empty())
        {
            impl = std::make_unique<MergeSortingBlocksBlockInputStream>(blocks, description, max_merged_block_size, limit);
        }
        else
        {
            /// If there was temporary files.
            ProfileEvents::increment(ProfileEvents::ExternalSortMerge);

            LOG_INFO(log, "There are " << temporary_files.size() << " temporary sorted parts to merge.");

            /// Create sorted streams to merge.
            for (const auto & file : temporary_files)
            {
                temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), header_without_constants));
                inputs_to_merge.emplace_back(temporary_inputs.back()->block_in);
            }

            /// Rest of blocks in memory.
            if (!blocks.empty())
                inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(blocks, description, max_merged_block_size, limit));

            /// Will merge that sorted streams.
            impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, description, max_merged_block_size, limit);
        }
    }

    Block res = impl->read();
    if (res)
        enrichBlockWithConstants(res, header);
    return res;
}


MergeSortingBlocksBlockInputStream::MergeSortingBlocksBlockInputStream(
    Blocks & blocks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_)
    : blocks(blocks_), header(blocks.at(0).cloneEmpty()), description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    Blocks nonempty_blocks;
    for (const auto & block : blocks)
    {
        if (block.rows() == 0)
            continue;

        nonempty_blocks.push_back(block);
        cursors.emplace_back(block, description);
        has_collation |= cursors.back().has_collation;
    }

    blocks.swap(nonempty_blocks);

    if (!has_collation)
    {
        for (size_t i = 0; i < cursors.size(); ++i)
            queue_without_collation.push(SortCursor(&cursors[i]));
    }
    else
    {
        for (size_t i = 0; i < cursors.size(); ++i)
            queue_with_collation.push(SortCursorWithCollation(&cursors[i]));
    }
}


Block MergeSortingBlocksBlockInputStream::readImpl()
{
    if (blocks.empty())
        return Block();

    if (blocks.size() == 1)
    {
        Block res = blocks[0];
        blocks.clear();
        return res;
    }

    return !has_collation
        ? mergeImpl<SortCursor>(queue_without_collation)
        : mergeImpl<SortCursorWithCollation>(queue_with_collation);
}


template <typename TSortCursor>
Block MergeSortingBlocksBlockInputStream::mergeImpl(std::priority_queue<TSortCursor> & queue)
{
    size_t num_columns = blocks[0].columns();

    MutableColumns merged_columns = blocks[0].cloneEmptyColumns();
    /// TODO: reserve (in each column)

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        if (!current->isLast())
        {
            current->next();
            queue.push(current);
        }

        ++total_merged_rows;
        if (limit && total_merged_rows == limit)
        {
            auto res = blocks[0].cloneWithColumns(std::move(merged_columns));
            blocks.clear();
            return res;
        }

        ++merged_rows;
        if (merged_rows == max_merged_block_size)
            return blocks[0].cloneWithColumns(std::move(merged_columns));
    }

    if (merged_rows == 0)
        return {};

    return blocks[0].cloneWithColumns(std::move(merged_columns));
}


void MergeSortingBlockInputStream::remerge()
{
    LOG_DEBUG(log, "Re-merging intermediate ORDER BY data (" << blocks.size() << " blocks with " << sum_rows_in_blocks << " rows) to save memory consumption");

    /// NOTE Maybe concat all blocks and partial sort will be faster than merge?
    MergeSortingBlocksBlockInputStream merger(blocks, description, max_merged_block_size, limit);

    Blocks new_blocks;
    size_t new_sum_rows_in_blocks = 0;
    size_t new_sum_bytes_in_blocks = 0;

    merger.readPrefix();
    while (Block block = merger.read())
    {
        new_sum_rows_in_blocks += block.rows();
        new_sum_bytes_in_blocks += block.allocatedBytes();
        new_blocks.emplace_back(std::move(block));
    }
    merger.readSuffix();

    LOG_DEBUG(log, "Memory usage is lowered from "
        << formatReadableSizeWithBinarySuffix(sum_bytes_in_blocks) << " to "
        << formatReadableSizeWithBinarySuffix(new_sum_bytes_in_blocks));

    /// If the memory consumption was not lowered enough - we will not perform remerge anymore. 2 is a guess.
    if (new_sum_bytes_in_blocks * 2 > sum_bytes_in_blocks)
        remerge_is_useful = false;

    blocks = std::move(new_blocks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}
}
