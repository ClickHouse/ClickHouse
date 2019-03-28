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
}


IndexGranularity::IndexGranularity(size_t marks_count, size_t fixed_granularity)
    : marks_to_rows(marks_count, fixed_granularity)
{
}

size_t IndexGranularity::getAvgGranularity() const
{
    if (marks_to_rows.empty())
        throw Exception("Trying to compute average index granularity with zero marks", ErrorCodes::LOGICAL_ERROR);

    return marks_to_rows.back() / marks_to_rows.size();
}


size_t IndexGranularity::getMarkStartingRow(size_t mark_index) const
{
    if (mark_index == 0)
        return 0;
    return marks_to_rows[mark_index - 1];
}

size_t IndexGranularity::getMarksCount() const
{
    return marks_to_rows.size();
}

size_t IndexGranularity::getTotalRows() const
{
    if (marks_to_rows.empty())
        return 0;
    return marks_to_rows.back();
}

void IndexGranularity::appendMark(size_t rows_count)
{
    if (marks_to_rows.empty())
        marks_to_rows.push_back(rows_count);
    else
        marks_to_rows.push_back(marks_to_rows.back() + rows_count);
}


size_t IndexGranularity::getMarkPositionInRows(const size_t mark_index) const
{
    if (mark_index == 0)
        return 0;
    return marks_to_rows[mark_index - 1];
}

size_t IndexGranularity::getRowsCountInRange(const MarkRange & range) const
{
    size_t subtrahend = 0;
    if (range.begin != 0)
        subtrahend = marks_to_rows[range.begin - 1];
    return marks_to_rows[range.end - 1] - subtrahend;
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
    if (size == 0)
        return;

    if (marks_to_rows.empty())
    {
        marks_to_rows.push_back(fixed_granularity);
        --size;
    }

    for (size_t i = 0; i < size; ++i)
        marks_to_rows.push_back(marks_to_rows.back() + fixed_granularity);
}

}
