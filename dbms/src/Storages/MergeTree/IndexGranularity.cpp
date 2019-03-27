#include <Storages/MergeTree/IndexGranularity.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IndexGranularity::IndexGranularity(const std::vector<size_t> & marks_to_rows_)
    : marks_to_rows(marks_to_rows_)
{
    for (size_t rows_count : marks_to_rows)
        total_rows += rows_count;
}


IndexGranularity::IndexGranularity(size_t marks_count, size_t fixed_granularity)
    : marks_to_rows(marks_count, fixed_granularity)
    , total_rows(marks_count * fixed_granularity)
{
}

size_t IndexGranularity::getAvgGranularity() const
{
    if (marks_to_rows.empty())
        throw Exception("Trying to compute average index granularity with zero marks", ErrorCodes::LOGICAL_ERROR);

    return total_rows / marks_to_rows.size();
}


size_t IndexGranularity::getMarkStartingRow(size_t mark_index) const
{
    size_t result = 0;
    for (size_t i = 0; i < mark_index; ++i)
        result += marks_to_rows[i];
    return result;
}

size_t IndexGranularity::getMarksCount() const
{
    return marks_to_rows.size();
}

size_t IndexGranularity::getTotalRows() const
{
    return total_rows;
}

void IndexGranularity::appendMark(size_t rows_count)
{
    total_rows += rows_count;
    marks_to_rows.push_back(rows_count);
}

//size_t IndexGranularity::getMarkRows(size_t mark_index) const
//{
//
//    if (mark_index >= marks_to_rows.size())
//        throw Exception("Trying to get mark rows for mark " + toString(mark_index) + " while marks count is " + toString(marks_to_rows.size()), ErrorCodes::LOGICAL_ERROR);
//    return marks_to_rows[mark_index];
//}


size_t IndexGranularity::getRowsCountInRange(const MarkRange & range) const
{
    size_t rows_count = 0;
    for (size_t i = range.begin; i < range.end; ++i)
        rows_count += getMarkRows(i);

    return rows_count;
}
size_t IndexGranularity::getRowsCountInRanges(const MarkRanges & ranges) const
{
    size_t total = 0;
    for (const auto & range : ranges)
        total += getRowsCountInRange(range);

    return total;
}


void IndexGranularity::resizeWithFixedGranularity(size_t size, size_t fixed_granularity)
{
    marks_to_rows.resize(size, fixed_granularity);
    total_rows += size * fixed_granularity;
}

}
