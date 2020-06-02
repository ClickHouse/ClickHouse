#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/TemporaryFileStream.h>
#include <DataStreams/processConstants.h>
#include <Common/formatReadable.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/sortBlock.h>
#include <Disks/StoragePolicy.h>


namespace ProfileEvents
{
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}

MergeSortingBlockInputStream::MergeSortingBlockInputStream(
    const BlockInputStreamPtr & input, SortDescription & description_,
    size_t max_merged_block_size_, UInt64 limit_, size_t max_bytes_before_remerge_,
    size_t max_bytes_before_external_sort_, VolumePtr tmp_volume_, const String & codec_, size_t min_free_disk_space_)
    : description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_),
    max_bytes_before_remerge(max_bytes_before_remerge_),
    max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_volume(tmp_volume_),
    codec(codec_),
    min_free_disk_space(min_free_disk_space_)
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
                size_t size = sum_bytes_in_blocks + min_free_disk_space;
                auto reservation = tmp_volume->reserve(size);
                if (!reservation)
                    throw Exception("Not enough space for external sort in temporary storage", ErrorCodes::NOT_ENOUGH_SPACE);

                const std::string tmp_path(reservation->getDisk()->getPath());
                temporary_files.emplace_back(createTemporaryFile(tmp_path));

                const std::string & path = temporary_files.back()->path();
                MergeSortingBlocksBlockInputStream block_in(blocks, description, max_merged_block_size, limit);

                LOG_INFO(log, "Sorting and writing part of data into temporary file {}", path);
                ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
                TemporaryFileStream::write(path, header_without_constants, block_in, &is_cancelled, codec); /// NOTE. Possibly limit disk usage.
                LOG_INFO(log, "Done writing part of data into temporary file {}", path);

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

            LOG_INFO(log, "There are {} temporary sorted parts to merge.", temporary_files.size());

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
    Blocks & blocks_, const SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_)
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

    if (has_collation)
        queue_with_collation = SortingHeap<SortCursorWithCollation>(cursors);
    else if (description.size() > 1)
        queue_without_collation = SortingHeap<SortCursor>(cursors);
    else
        queue_simple = SortingHeap<SimpleSortCursor>(cursors);
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

    if (has_collation)
        return mergeImpl(queue_with_collation);
    else if (description.size() > 1)
        return mergeImpl(queue_without_collation);
    else
        return mergeImpl(queue_simple);
}


template <typename TSortingHeap>
Block MergeSortingBlocksBlockInputStream::mergeImpl(TSortingHeap & queue)
{
    size_t num_columns = header.columns();
    MutableColumns merged_columns = header.cloneEmptyColumns();

    /// Reserve
    if (queue.isValid() && !blocks.empty())
    {
        /// The expected size of output block is the same as input block
        size_t size_to_reserve = blocks[0].rows();
        for (auto & column : merged_columns)
            column->reserve(size_to_reserve);
    }

    /// TODO: Optimization when a single block left.

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (queue.isValid())
    {
        auto current = queue.current();

        /// Append a row from queue.
        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        ++total_merged_rows;
        ++merged_rows;

        /// We don't need more rows because of limit has reached.
        if (limit && total_merged_rows == limit)
        {
            blocks.clear();
            break;
        }

        queue.next();

        /// It's enough for current output block but we will continue.
        if (merged_rows == max_merged_block_size)
            break;
    }

    if (!queue.isValid())
        blocks.clear();

    if (merged_rows == 0)
        return {};

    return header.cloneWithColumns(std::move(merged_columns));
}


void MergeSortingBlockInputStream::remerge()
{
    LOG_DEBUG(log, "Re-merging intermediate ORDER BY data ({} blocks with {} rows) to save memory consumption", blocks.size(), sum_rows_in_blocks);

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

    LOG_DEBUG(log, "Memory usage is lowered from {} to {}", ReadableSize(sum_bytes_in_blocks), ReadableSize(new_sum_bytes_in_blocks));

    /// If the memory consumption was not lowered enough - we will not perform remerge anymore. 2 is a guess.
    if (new_sum_bytes_in_blocks * 2 > sum_bytes_in_blocks)
        remerge_is_useful = false;

    blocks = std::move(new_blocks);
    sum_rows_in_blocks = new_sum_rows_in_blocks;
    sum_bytes_in_blocks = new_sum_bytes_in_blocks;
}
}
