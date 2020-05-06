#include <algorithm>

#include <DataStreams/OffsetBlockInputStream.h>


namespace DB
{

OffsetBlockInputStream::OffsetBlockInputStream(
    const BlockInputStreamPtr & input, UInt64 offset_, bool always_read_till_end_,
    bool use_limit_as_total_rows_approx, bool with_ties_, const SortDescription & description_)
    : offset(offset_), always_read_till_end(always_read_till_end_), with_ties(with_ties_)
    , description(description_)
{
    if (use_limit_as_total_rows_approx)
    {
        addTotalRowsApprox(static_cast<size_t>(limit));
    }

    children.push_back(input);
}

Block OffsetBlockInputStream::readImpl()
{
    Block res;
    UInt64 rows = 0;

    do
    {
        res = children.back()->read();
        if (!res)
            return res;
        rows = res.rows();
        pos += rows;
    } while (pos <= offset);

    SharedBlockPtr ptr = new detail::SharedBlock(std::move(res));

    /// give away a piece of the block
    UInt64 start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

    for (size_t i = 0; i < ptr->columns(); ++i)
        ptr->safeGetByPosition(i).column = ptr->safeGetByPosition(i).column->cut(start, rows);

    return *ptr;
}

}
