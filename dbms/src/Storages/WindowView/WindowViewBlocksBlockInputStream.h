#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/WindowView/WindowViewBlocksMetadata.h>


namespace DB
{
/** A stream of blocks from a shared vector of blocks with metadata
  */
using BlocksListPtr = std::shared_ptr<BlocksList>;
using BlocksListPtrs = std::shared_ptr<std::list<BlocksListPtr>>;

class WindowViewBlocksBlockInputStream : public IBlockInputStream
{
public:
    /// Acquires shared ownership of the blocks vector
    WindowViewBlocksBlockInputStream(BlocksListPtrs blocks_ptr_, Block header_, std::mutex &mutex_)
        : blocks(blocks_ptr_), mutex(mutex_), header(std::move(header_))
    {
        it_blocks = blocks->begin();
        end_blocks = blocks->end();
        if (it_blocks != end_blocks)
        {
            it = (*it_blocks)->begin();
            end = (*it_blocks)->end();
        }
    }

    String getName() const override { return "MetadataBlocks"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        while (it_blocks != end_blocks)
        {
            while (it != end)
            {
                Block &block = *it;
                size_t columns = block.columns();

                //generate filter
                auto & column_status = block.getByName("____fire_status").column;
                auto column_status_mutable = column_status->assumeMutable();
                IColumn::Filter filter(column_status->size(), 0);
                auto & data = static_cast<ColumnUInt8 &>(*column_status_mutable).getData();
                {
                    std::unique_lock lock(mutex);
                    for (size_t i = 0; i < column_status->size(); ++i)
                    {
                        if (data[i] == WINDOW_VIEW_FIRE_STATUS::READY)
                        {
                            filter[i] = 1;
                            data[i] = WINDOW_VIEW_FIRE_STATUS::RETIRED;
                        }
                    }
                }

                //filter block
                /** Let's find out how many rows will be in result.
                 * To do this, we filter out the first non-constant column
                 *  or calculate number of set bytes in the filter.
                 */
                size_t first_non_constant_column = 0;
                for (size_t i = 0; i < columns; ++i)
                {
                    if (!isColumnConst(*block.safeGetByPosition(i).column))
                    {
                        first_non_constant_column = i;
                        break;
                    }
                }

                Block res = block.cloneEmpty();
                size_t filtered_rows = 0;
                {
                    ColumnWithTypeAndName & current_column = block.safeGetByPosition(first_non_constant_column);
                    ColumnWithTypeAndName & filtered_column = res.safeGetByPosition(first_non_constant_column);
                    filtered_column.column = current_column.column->filter(filter, -1);
                    filtered_rows = filtered_column.column->size();
                }

                /// If the current block is completely filtered out, let's move on to the next one.
                if (filtered_rows == 0)
                {
                    ++it;
                    continue;
                }

                /// If all the rows pass through the filter.
                if (filtered_rows == filter.size())
                {
                    ++it;
                    return block;
                }

                /// Filter the rest of the columns.
                for (size_t i = 0; i < columns; ++i)
                {
                    ColumnWithTypeAndName & current_column = block.safeGetByPosition(i);
                    ColumnWithTypeAndName & filtered_column = res.safeGetByPosition(i);

                    if (i == first_non_constant_column)
                        continue;

                    if (isColumnConst(*current_column.column))
                        filtered_column.column = current_column.column->cut(0, filtered_rows);
                    else
                        filtered_column.column = current_column.column->filter(filter, -1);
                }
                ++it;
                return res;
            }
            ++it_blocks;
            if (it_blocks != end_blocks)
            {
                it = (*it_blocks)->begin();
                end = (*it_blocks)->end();
            }
        }
        return Block();
    }

private:
    BlocksListPtrs blocks;
    std::list<BlocksListPtr>::iterator it_blocks;
    std::list<BlocksListPtr>::iterator end_blocks;
    BlocksList::iterator it;
    BlocksList::iterator end;
    std::mutex & mutex;
    Block header;
};
}
