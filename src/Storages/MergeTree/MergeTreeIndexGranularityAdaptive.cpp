#include <Common/Exception.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranularityAdaptive::MergeTreeIndexGranularityAdaptive(const std::vector<size_t> & marks_rows_partial_sums_)
    : marks_rows_partial_sums(marks_rows_partial_sums_)
{
}

/// Rows after mark to next mark
size_t MergeTreeIndexGranularityAdaptive::getMarkRows(size_t mark_index) const
{
    if (mark_index >= getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get non existing mark {}, while size is {}", mark_index, getMarksCount());

    if (mark_index == 0)
        return marks_rows_partial_sums[0];

    return marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
}

MarkRange MergeTreeIndexGranularityAdaptive::getMarkRangeForRowOffset(size_t row_offset) const
{
    auto position = std::lower_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), row_offset + 1);
    size_t begin_mark = position - marks_rows_partial_sums.begin();

    return {begin_mark, begin_mark + 1};
}

bool MergeTreeIndexGranularityAdaptive::hasFinalMark() const
{
    if (marks_rows_partial_sums.empty())
        return false;
    return getLastMarkRows() == 0;
}

size_t MergeTreeIndexGranularityAdaptive::getMarksCount() const
{
    return marks_rows_partial_sums.size();
}

size_t MergeTreeIndexGranularityAdaptive::getTotalRows() const
{
    if (marks_rows_partial_sums.empty())
        return 0;
    return marks_rows_partial_sums.back();
}

void MergeTreeIndexGranularityAdaptive::appendMark(size_t rows_count)
{
    if (hasFinalMark())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot append mark after final");
    }
    else if (marks_rows_partial_sums.empty())
    {
        marks_rows_partial_sums.push_back(rows_count);
    }
    else
    {
        marks_rows_partial_sums.push_back(marks_rows_partial_sums.back() + rows_count);
    }
}

void MergeTreeIndexGranularityAdaptive::adjustLastMark(size_t rows_count)
{
    if (hasFinalMark())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot adjust final mark");
    }
    else if (marks_rows_partial_sums.empty())
    {
        marks_rows_partial_sums.push_back(rows_count);
    }
    else
    {
        marks_rows_partial_sums.pop_back();
        appendMark(rows_count);
    }
}

size_t MergeTreeIndexGranularityAdaptive::getRowsCountInRange(size_t begin, size_t end) const
{
    if (end > getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get marks in range [{}; {}), while size is {}", begin, end, getMarksCount());

    if (end == 0)
        return 0;

    size_t subtrahend = 0;
    if (begin != 0)
        subtrahend = marks_rows_partial_sums[begin - 1];

    return marks_rows_partial_sums[end - 1] - subtrahend;
}

size_t MergeTreeIndexGranularityAdaptive::countMarksForRows(size_t from_mark, size_t number_of_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + number_of_rows;
    auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), last_row_pos);
    size_t to_mark = it - marks_rows_partial_sums.begin();
    return to_mark - from_mark;
}

size_t MergeTreeIndexGranularityAdaptive::countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + offset_in_rows + number_of_rows;
    auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), last_row_pos);
    size_t to_mark = it - marks_rows_partial_sums.begin();

    return getRowsCountInRange(from_mark, std::max(1UL, to_mark)) - offset_in_rows;
}

uint64_t MergeTreeIndexGranularityAdaptive::getBytesSize() const
{
    return marks_rows_partial_sums.size() * sizeof(size_t);
}

uint64_t MergeTreeIndexGranularityAdaptive::getBytesAllocated() const
{
    return marks_rows_partial_sums.capacity() * sizeof(size_t);
}

std::shared_ptr<MergeTreeIndexGranularity> MergeTreeIndexGranularityAdaptive::optimize()
{
    size_t marks_count = getMarksCountWithoutFinal();
    if (marks_count == 0)
        return nullptr;

    size_t first_mark = getMarkRows(0);
    for (size_t i = 1; i < marks_count - 1; ++i)
    {
        if (getMarkRows(i) != first_mark)
        {
            /// We cannot optimize to constant but at least optimize memory usage.
            marks_rows_partial_sums.shrink_to_fit();
            return nullptr;
        }
    }

    size_t last_mark = getMarkRows(marks_count - 1);
    return std::make_shared<MergeTreeIndexGranularityConstant>(first_mark, last_mark, marks_count, hasFinalMark());
}

std::string MergeTreeIndexGranularityAdaptive::describe() const
{
    return fmt::format("Adaptive(marks_rows_partial_sums: [{}])", fmt::join(marks_rows_partial_sums, ", "));
}

}
