#include <algorithm>

#include <DataStreams/LimitBlockInputStream.h>


namespace DB
{

LimitBlockInputStream::LimitBlockInputStream(const BlockInputStreamPtr & input, UInt64 limit_, UInt64 offset_, bool always_read_till_end_, bool use_limit_as_total_rows_approx)
    : limit(limit_), offset(offset_), always_read_till_end(always_read_till_end_)
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

    for (size_t i = 0; i < res.columns(); ++i)
        res.safeGetByPosition(i).column = res.safeGetByPosition(i).column->cut(start, length);

    // TODO: we should provide feedback to child-block, so it will know how many rows are actually consumed.
    //       It's crucial for streaming engines like Kafka.

    return res;
}

}
