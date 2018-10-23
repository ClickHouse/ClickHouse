#include <DataStreams/FinishSortingBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/processConstants.h>


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

FinishSortingBlockInputStream::FinishSortingBlockInputStream(
    const BlockInputStreamPtr & input, const SortDescription & description_sorted_,
    const SortDescription & description_to_sort_,
    size_t max_merged_block_size_, size_t limit_)
    : description_sorted(description_sorted_), description_to_sort(description_to_sort_),
    max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    if (!isPrefix(description_sorted, description_to_sort))
        throw Exception("Can`t finish sorting. SortDescription of already sorted stream is not prefix of "
            "SortDescription needed to sort", ErrorCodes::LOGICAL_ERROR);

    children.push_back(input);
    header = children.at(0)->getHeader();
    removeConstantsFromSortDescription(header, description_to_sort);
}


struct Less
{
    const ColumnsWithSortDescriptions & left_columns;
    const ColumnsWithSortDescriptions & right_columns;

    Less(const ColumnsWithSortDescriptions & left_columns_, const ColumnsWithSortDescriptions & right_columns_) :
        left_columns(left_columns_), right_columns(right_columns_) {}

    bool operator() (size_t a, size_t b) const
    {
        for (auto it = left_columns.begin(), jt = right_columns.begin(); it != left_columns.end(); ++it, ++jt)
        {
            int res = it->second.direction * it->first->compareAt(a, b, *jt->first, it->second.nulls_direction);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};

Block FinishSortingBlockInputStream::readImpl()
{
    if (limit && total_rows_processed >= limit)
        return {};

    Block res;
    if (impl)
        res = impl->read();

    /// If res block is empty, we have finished sorting previous chunk of blocks.
    if (!res)
    {
        if (end_of_stream)
            return {};

        blocks.clear();
        if (tail_block)
            blocks.push_back(std::move(tail_block));

        while (true)
        {
            Block block = children.back()->read();

            /// End of input stream, but we can`t return immediatly, we need to merge already read blocks.
            /// Check it later, when get end of stream from impl.
            if (!block)
            {
                end_of_stream = true;
                break;
            }

            // If there were only const columns in sort description, then there is no need to sort.
            // Return the blocks as is.
            if (description_to_sort.empty())
                return block;

            size_t size = block.rows();
            if (size == 0)
                continue;

            /// We need to sort each block separatly before merging.
            sortBlock(block, description_to_sort);

            removeConstantsFromBlock(block);

            /// Find the position of last already read key in current block.
            if (!blocks.empty())
            {
                const Block & last_block = blocks.back();
                auto last_columns = getColumnsWithSortDescription(last_block, description_sorted);
                auto current_columns = getColumnsWithSortDescription(block, description_sorted);

                Less less(last_columns, current_columns);

                IColumn::Permutation perm(size);
                for (size_t i = 0; i < size; ++i)
                    perm[i] = i;

                auto it = std::upper_bound(perm.begin(), perm.end(), last_block.rows() - 1, less);

                /// We need to save tail of block, because next block may starts with the same key as in tail
                /// and we should sort these rows in one chunk.
                if (it != perm.end())
                {
                    size_t tail_pos = it - perm.begin();
                    Block head_block = block.cloneEmpty();
                    tail_block = block.cloneEmpty();

                    for (size_t i = 0; i < block.columns(); ++i)
                    {
                        head_block.getByPosition(i).column = block.getByPosition(i).column->cut(0, tail_pos);
                        tail_block.getByPosition(i).column = block.getByPosition(i).column->cut(tail_pos, block.rows() - tail_pos);
                    }

                    if (head_block.rows())
                        blocks.push_back(head_block);

                    break;
                }
            }

            /// If we reach here, that means that current block is first in chunk
            /// or it all consists of rows with the same key as tail of a previous block.
            blocks.push_back(block);
        }

        impl = std::make_unique<MergeSortingBlocksBlockInputStream>(blocks, description_to_sort, max_merged_block_size, limit);
        res = impl->read();
    }

    if (res)
        enrichBlockWithConstants(res, header);

    total_rows_processed += res.rows();

    return res;
}
}
