#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranularity::MergeTreeIndexGranularity(const std::vector<size_t> & marks_rows_partial_sums_)
    : marks_rows_partial_sums(marks_rows_partial_sums_)
{
}

/// Rows after mark to next mark
size_t MergeTreeIndexGranularity::getMarkRows(size_t mark_index) const
{
    if (mark_index >= getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get non existing mark {}, while size is {}", mark_index, getMarksCount());
    if (mark_index == 0)
        return marks_rows_partial_sums[0];
    return marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
}

size_t MergeTreeIndexGranularity::getMarkStartingRow(size_t mark_index) const
{
    if (mark_index == 0)
        return 0;
    return marks_rows_partial_sums[mark_index - 1];
}

size_t MergeTreeIndexGranularity::getMarksCount() const
{
    return marks_rows_partial_sums.size();
}

size_t MergeTreeIndexGranularity::getTotalRows() const
{
    if (marks_rows_partial_sums.empty())
        return 0;
    return marks_rows_partial_sums.back();
}

void MergeTreeIndexGranularity::appendMark(size_t rows_count)
{
    if (marks_rows_partial_sums.empty())
        marks_rows_partial_sums.push_back(rows_count);
    else
        marks_rows_partial_sums.push_back(marks_rows_partial_sums.back() + rows_count);
}

void MergeTreeIndexGranularity::addRowsToLastMark(size_t rows_count)
{
    if (marks_rows_partial_sums.empty())
        marks_rows_partial_sums.push_back(rows_count);
    else
        marks_rows_partial_sums.back() += rows_count;
}

void MergeTreeIndexGranularity::popMark()
{
    if (!marks_rows_partial_sums.empty())
        marks_rows_partial_sums.pop_back();
}

size_t MergeTreeIndexGranularity::getRowsCountInRange(size_t begin, size_t end) const
{
    size_t subtrahend = 0;
    if (begin != 0)
        subtrahend = marks_rows_partial_sums[begin - 1];
    return marks_rows_partial_sums[end - 1] - subtrahend;
}

size_t MergeTreeIndexGranularity::getRowsCountInRange(const MarkRange & range) const
{
    return getRowsCountInRange(range.begin, range.end);
}

size_t MergeTreeIndexGranularity::getRowsCountInRanges(const MarkRanges & ranges) const
{
    size_t total = 0;
    for (const auto & range : ranges)
        total += getRowsCountInRange(range);
    return total;
}

size_t MergeTreeIndexGranularity::countMarksForRows(size_t from_mark, size_t number_of_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + number_of_rows;
    auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), last_row_pos);
    size_t to_mark = it - marks_rows_partial_sums.begin();
    return to_mark - from_mark;
}

size_t MergeTreeIndexGranularity::countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows, size_t min_marks_to_read) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + offset_in_rows + number_of_rows;
    auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), last_row_pos);
    size_t to_mark = it - marks_rows_partial_sums.begin();

    /// This is a heuristic to respect min_marks_to_read which is ignored by MergeTreeReadPool in case of remote disk.
    /// See comment in IMergeTreeSelectAlgorithm.
    if (min_marks_to_read)
    {
        // check overflow
        size_t min_marks_to_read_2 = 0;
        bool overflow = common::mulOverflow(min_marks_to_read, 2, min_marks_to_read_2);

        size_t to_mark_overwrite = 0;
        if (!overflow)
            overflow = common::addOverflow(from_mark, min_marks_to_read_2, to_mark_overwrite);

        if (!overflow && to_mark_overwrite < to_mark)
            to_mark = to_mark_overwrite;
    }

    return getRowsCountInRange(from_mark, std::max(1UL, to_mark)) - offset_in_rows;
}

void MergeTreeIndexGranularity::resizeWithFixedGranularity(size_t size, size_t fixed_granularity)
{
    marks_rows_partial_sums.resize(size);

    size_t prev = 0;
    for (size_t i = 0; i < size; ++i)
    {
        marks_rows_partial_sums[i] = fixed_granularity + prev;
        prev = marks_rows_partial_sums[i];
    }
}

std::string MergeTreeIndexGranularity::describe() const
{
    return fmt::format("initialized: {}, marks_rows_partial_sums: [{}]", initialized, fmt::join(marks_rows_partial_sums, ", "));
}
}
