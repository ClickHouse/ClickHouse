#include <algorithm>

#include <DataStreams/LimitBlockInputStream.h>


namespace DB
{


LimitBlockInputStream::LimitBlockInputStream(
    const BlockInputStreamPtr & input, UInt64 limit_, UInt64 offset_, bool always_read_till_end_,
    bool use_limit_as_total_rows_approx, bool with_ties_, const SortDescription & description_)
    : limit(limit_), offset(offset_), always_read_till_end(always_read_till_end_), with_ties(with_ties_)
    , description(description_)
{
    if (use_limit_as_total_rows_approx)
    {
        addTotalRowsApprox(static_cast<size_t>(limit));
    }

    children.push_back(input);
}


Block LimitBlockInputStream::readImpl()
{
    Block res;
    UInt64 rows = 0;

    /// pos >= offset + limit and all rows in previous block were equal to ties_row_ref
    /// so we check current block
    if (with_ties && ties_row_ref.shared_block)
    {
        rows = res.rows();
        pos += rows;
        res = children.back()->read();


        SharedBlockPtr ptr = new detail::SharedBlock(std::move(res));
        ColumnRawPtrs columns = SharedBlockRowRef::getBlockColumns(*ptr, description);
        UInt64 len;

        for (len = 0; len < rows; ++len)
        {
            SharedBlockRowRef current_row;
            SharedBlockRowRef::setSharedBlockRowRef(current_row, ptr, &columns, len);
            if (current_row != ties_row_ref)
            {
                ties_row_ref.reset();
                break;
            }
        }

        if (len < rows - 1)
        {
            for (size_t i = 0; i < ptr->columns(); ++i)
                ptr->safeGetByPosition(i).column = ptr->safeGetByPosition(i).column->cut(0, len);
        }

        return *ptr;
    }

    /// pos - how many lines were read, including the last read block

    if (pos >= offset + limit)
    {
        if (!always_read_till_end)
            return res;
        else
        {
            while (children.back()->read())
                ;
            return res;
        }
    }

    do
    {
        res = children.back()->read();
        if (!res)
            return res;
        rows = res.rows();
        pos += rows;
    } while (pos <= offset);


    /// give away the whole block
    if (pos >= offset + rows && pos <= offset + limit)
        return res;

    /// give away a piece of the block
    UInt64 start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    UInt64 length = std::min(
        static_cast<Int64>(limit), std::min(
        static_cast<Int64>(pos) - static_cast<Int64>(offset),
        static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));

    SharedBlockPtr ptr = new detail::SharedBlock(std::move(res));

    /// check if other rows in current block equals to last one in limit
    if (with_ties)
    {
        ColumnRawPtrs columns = SharedBlockRowRef::getBlockColumns(*ptr, description);
        SharedBlockRowRef::setSharedBlockRowRef(ties_row_ref, ptr, &columns, start + length - 1);

        for (size_t i = ties_row_ref.row_num + 1; i < rows; ++i)
        {
            SharedBlockRowRef current_row;
            SharedBlockRowRef::setSharedBlockRowRef(current_row, ptr, &columns, i);
            if (current_row == ties_row_ref)
                ++length;
            else
            {
                ties_row_ref.reset();
                break;
            }
        }
    }

    for (size_t i = 0; i < ptr->columns(); ++i)
        ptr->safeGetByPosition(i).column = ptr->safeGetByPosition(i).column->cut(start, length);

    // TODO: we should provide feedback to child-block, so it will know how many rows are actually consumed.
    //       It's crucial for streaming engines like Kafka.

    return *ptr;
}


}
