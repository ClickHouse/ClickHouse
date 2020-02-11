#include <algorithm>

#include <DataStreams/LimitBlockInputStream.h>


namespace DB
{

/// gets pointers to all columns of block, which were used for ORDER BY
static ColumnRawPtrs extractSortColumns(const Block & block, const SortDescription & description)
{
    size_t size = description.size();
    ColumnRawPtrs res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
                                    ? block.getByName(description[i].column_name).column.get()
                                    : block.safeGetByPosition(description[i].column_number).column.get();
        res.emplace_back(column);
    }

    return res;
}


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

    /// pos >= offset + limit and all rows in the end of previous block were equal
    ///  to row at 'limit' position. So we check current block.
    if (!ties_row_ref.empty() && pos >= offset + limit)
    {
        res = children.back()->read();
        rows = res.rows();

        if (!res)
            return res;

        SharedBlockPtr ptr = new detail::SharedBlock(std::move(res));
        ptr->sort_columns = extractSortColumns(*ptr, description);

        UInt64 len;
        for (len = 0; len < rows; ++len)
        {
            SharedBlockRowRef current_row;
            current_row.set(ptr, &ptr->sort_columns, len);

            if (current_row != ties_row_ref)
            {
                ties_row_ref.reset();
                break;
            }
        }

        if (len < rows)
        {
            for (size_t i = 0; i < ptr->columns(); ++i)
                ptr->safeGetByPosition(i).column = ptr->safeGetByPosition(i).column->cut(0, len);
        }

        return *ptr;
    }

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

    SharedBlockPtr ptr = new detail::SharedBlock(std::move(res));
    if (with_ties)
        ptr->sort_columns = extractSortColumns(*ptr, description);

    /// give away the whole block
    if (pos >= offset + rows && pos <= offset + limit)
    {
        /// Save rowref for last row, because probalbly next block begins with the same row.
        if (with_ties && pos == offset + limit)
            ties_row_ref.set(ptr, &ptr->sort_columns, rows - 1);
        return *ptr;
    }

    /// give away a piece of the block
    UInt64 start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    UInt64 length = std::min(
        static_cast<Int64>(limit), std::min(
        static_cast<Int64>(pos) - static_cast<Int64>(offset),
        static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));


    /// check if other rows in current block equals to last one in limit
    if (with_ties)
    {
        ties_row_ref.set(ptr, &ptr->sort_columns, start + length - 1);

        for (size_t i = ties_row_ref.row_num + 1; i < rows; ++i)
        {
            SharedBlockRowRef current_row;
            current_row.set(ptr, &ptr->sort_columns, i);
            if (current_row == ties_row_ref)
                ++length;
            else
            {
                ties_row_ref.reset();
                break;
            }
        }
    }

    if (length == rows)
        return *ptr;

    for (size_t i = 0; i < ptr->columns(); ++i)
        ptr->safeGetByPosition(i).column = ptr->safeGetByPosition(i).column->cut(start, length);

    // TODO: we should provide feedback to child-block, so it will know how many rows are actually consumed.
    //       It's crucial for streaming engines like Kafka.

    return *ptr;
}

}
