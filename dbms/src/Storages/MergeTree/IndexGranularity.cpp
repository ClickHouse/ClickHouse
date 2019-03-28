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


size_t IndexGranularity::getRowsCountInRange(size_t begin, size_t end) const
{
    size_t subtrahend = 0;
    if (begin != 0)
        subtrahend = marks_to_rows[begin - 1];
    return marks_to_rows[end - 1] - subtrahend;
}

size_t IndexGranularity::getRowsCountInRange(const MarkRange & range) const
{
    return getRowsCountInRange(range.begin, range.end);
}

size_t IndexGranularity::getRowsCountInRanges(const MarkRanges & ranges) const
{
    size_t total = 0;
    for (const auto & range : ranges)
        total += getRowsCountInRange(range);

    return total;
}


size_t IndexGranularity::countMarksForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + offset_in_rows + number_of_rows;
    auto position = std::upper_bound(marks_to_rows.begin(), marks_to_rows.end(), last_row_pos);
    size_t to_mark;
    if (position == marks_to_rows.end())
        to_mark = marks_to_rows.size();
    else
        to_mark = position - marks_to_rows.begin();

    return getRowsCountInRange(from_mark, std::max(1UL, to_mark)) - offset_in_rows;

}

void IndexGranularity::resizeWithFixedGranularity(size_t size, size_t fixed_granularity)
{
    marks_to_rows.resize(size);

    size_t prev = 0;
    for (size_t i = 0; i < size; ++i)
    {
        marks_to_rows[i] = fixed_granularity + prev;
        prev = marks_to_rows[i];
    }
}


}
