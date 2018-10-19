#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Takes stream already sorted by `x` and finishes sorting it by (`x`, `y`).
 *  During sorting only blocks with rows that equal by `x` saved in RAM.
 * */
class FinishSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    FinishSortingBlockInputStream(const BlockInputStreamPtr & input, const SortDescription & description_sorted_,
        const SortDescription & description_to_sort_,
        size_t max_merged_block_size_, size_t limit_);

    String getName() const override { return "FinishSorting"; }

    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description_to_sort; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    SortDescription description_sorted;
    SortDescription description_to_sort;
    size_t max_merged_block_size;
    size_t limit;

    Block tail_block;
    Blocks blocks;

    std::unique_ptr<IBlockInputStream> impl;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// to avoid excessive virtual function calls
    /// Save original block structure here.
    Block header;

    bool end_of_stream = false;
    size_t total_rows_processed = 0;
};
}