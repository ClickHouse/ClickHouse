#pragma once

#include <common/logger_useful.h>

#include <Common/filesystemHelpers.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>

#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>


namespace DB
{

struct TemporaryFileStream;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

namespace ErrorCodes
{
}
/** Merges stream of sorted each-separately blocks to sorted as-a-whole stream of blocks.
  * If data to sort is too much, could use external sorting, with temporary files.
  */

/** Part of implementation. Merging array of ready (already read from somewhere) blocks.
  * Returns result of merge as stream of blocks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSortingBlocksBlockInputStream : public IBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlocksBlockInputStream(Blocks & blocks_, const SortDescription & description_,
        size_t max_merged_block_size_, UInt64 limit_ = 0);

    String getName() const override { return "MergeSortingBlocks"; }

    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Blocks & blocks;
    Block header;
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;
    size_t total_merged_rows = 0;

    SortCursorImpls cursors;

    bool has_collation = false;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SimpleSortCursor> queue_simple;
    SortingHeap<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
     *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
     */
    template <typename TSortingHeap>
    Block mergeImpl(TSortingHeap & queue);
};


class MergeSortingBlockInputStream : public IBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlockInputStream(const BlockInputStreamPtr & input, SortDescription & description_,
        size_t max_merged_block_size_, UInt64 limit_,
        size_t max_bytes_before_remerge_,
        size_t max_bytes_before_external_sort_, VolumePtr tmp_volume_,
        const String & codec_,
        size_t min_free_disk_space_);

    String getName() const override { return "MergeSorting"; }

    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    size_t max_bytes_before_remerge;
    size_t max_bytes_before_external_sort;
    VolumePtr tmp_volume;
    String codec;
    size_t min_free_disk_space;

    Poco::Logger * log = &Poco::Logger::get("MergeSortingBlockInputStream");

    Blocks blocks;
    size_t sum_rows_in_blocks = 0;
    size_t sum_bytes_in_blocks = 0;
    std::unique_ptr<IBlockInputStream> impl;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header;
    Block header_without_constants;

    /// Everything below is for external sorting.
    std::vector<std::unique_ptr<TemporaryFile>> temporary_files;
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    BlockInputStreams inputs_to_merge;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();
    /// If remerge doesn't save memory at least several times, mark it as useless and don't do it anymore.
    bool remerge_is_useful = true;
};
}
