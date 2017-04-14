#pragma once

#include <queue>
#include <Poco/TemporaryFile.h>

#include <common/logger_useful.h>

#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>


namespace DB
{

/** Merges stream of sorted each-separately blocks to sorted as-a-whole stream of blocks.
  * If data to sort is too much, could use external sorting, with temporary files.
  */

/** Part of implementation. Merging array of ready (already read from somewhere) blocks.
  * Returns result of merge as stream of blocks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSortingBlocksBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlocksBlockInputStream(Blocks & blocks_, SortDescription & description_,
        size_t max_merged_block_size_, size_t limit_ = 0);

    String getName() const override { return "MergeSortingBlocks"; }
    String getID() const override { return getName(); }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

protected:
    Block readImpl() override;

private:
    Blocks & blocks;
    SortDescription description;
    size_t max_merged_block_size;
    size_t limit;
    size_t total_merged_rows = 0;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    bool has_collation = false;

    std::priority_queue<SortCursor> queue;
    std::priority_queue<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
     *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
     */
    template <typename TSortCursor>
    Block mergeImpl(std::priority_queue<TSortCursor> & queue);
};


class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_,
        size_t max_merged_block_size_, size_t limit_,
        size_t max_bytes_before_external_sort_, const std::string & tmp_path_)
        : description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_),
        max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_path(tmp_path_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "MergeSorting"; }

    String getID() const override
    {
        std::stringstream res;
        res << "MergeSorting(" << children.back()->getID();

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ")";
        return res.str();
    }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    size_t limit;

    size_t max_bytes_before_external_sort;
    const std::string tmp_path;

    Logger * log = &Logger::get("MergeSortingBlockInputStream");

    Blocks blocks;
    size_t sum_bytes_in_blocks = 0;
    std::unique_ptr<IBlockInputStream> impl;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block sample_block;

    /// Everything below is for external sorting.
    std::vector<std::unique_ptr<Poco::TemporaryFile>> temporary_files;

    /// For reading data from temporary file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path)
            : file_in(path), compressed_in(file_in), block_in(std::make_shared<NativeBlockInputStream>(compressed_in)) {}
    };

    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    BlockInputStreams inputs_to_merge;
};

}
