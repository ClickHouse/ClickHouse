#include <Common/Exception.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranularityConstant::MergeTreeIndexGranularityConstant(size_t constant_granularity_)
    : constant_granularity(constant_granularity_)
    , last_mark_granularity(constant_granularity_)
{
}

MergeTreeIndexGranularityConstant::MergeTreeIndexGranularityConstant(size_t constant_granularity_, size_t last_mark_granularity_, size_t num_marks_without_final_, bool has_final_mark_)
    : constant_granularity(constant_granularity_)
    , last_mark_granularity(last_mark_granularity_)
    , num_marks_without_final(num_marks_without_final_)
    , has_final_mark(has_final_mark_)
{
}

/// Rows after mark to next mark
size_t MergeTreeIndexGranularityConstant::getMarkRows(size_t mark_index) const
{
    if (mark_index >= getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get non existing mark {}, while size is {}", mark_index, getMarksCount());

    if (mark_index + 1 < num_marks_without_final)
        return constant_granularity;

    if (mark_index + 1 == num_marks_without_final)
        return last_mark_granularity;

    return 0; // Final mark.
}

MarkRange MergeTreeIndexGranularityConstant::getMarkRangeForRowOffset(size_t row_offset) const
{
    size_t mark_index;
    if (row_offset < constant_granularity * (num_marks_without_final - 1))
        mark_index = row_offset / constant_granularity;
    else
        mark_index = num_marks_without_final - 1;

    return {mark_index, mark_index + 1};
}

size_t MergeTreeIndexGranularityConstant::getMarksCount() const
{
    return num_marks_without_final + has_final_mark;
}

size_t MergeTreeIndexGranularityConstant::getTotalRows() const
{
    if (num_marks_without_final == 0)
        return 0;

    return constant_granularity * (num_marks_without_final - 1) + last_mark_granularity;
}

void MergeTreeIndexGranularityConstant::appendMark(size_t rows_count)
{
    if (has_final_mark)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot append mark after final");
    }
    else if (rows_count == 0)
    {
        has_final_mark = true;
    }
    else if (rows_count != constant_granularity)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot append mark with {} rows. Granularity is constant ({})", rows_count, constant_granularity);
    }
    else
    {
        ++num_marks_without_final;
    }
}

void MergeTreeIndexGranularityConstant::adjustLastMark(size_t rows_count)
{
    if (has_final_mark)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot adjust final mark");
    }
    else
    {
        if (num_marks_without_final == 0)
            ++num_marks_without_final;

        last_mark_granularity = rows_count;
    }
}

size_t MergeTreeIndexGranularityConstant::getRowsCountInRange(size_t begin, size_t end) const
{
    if (end > getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get marks in range [{}; {}), while size is {}", begin, end, getMarksCount());

    if (end == 0)
        return 0;

    size_t total_rows = 0;
    if (end >= num_marks_without_final)
    {
        total_rows += last_mark_granularity;
        end = num_marks_without_final - 1;
    }

    total_rows += constant_granularity * (end - begin);
    return total_rows;
}

size_t MergeTreeIndexGranularityConstant::getMarkUpperBoundForRow(size_t row_index) const
{
    size_t num_rows_with_constant_granularity = (num_marks_without_final - 1) * constant_granularity;

    if (row_index >= getTotalRows())
        return getMarksCount();

    if (row_index >= num_rows_with_constant_granularity)
        return num_marks_without_final - 1;

    return row_index / constant_granularity;
}

size_t MergeTreeIndexGranularityConstant::countMarksForRows(size_t from_mark, size_t number_of_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + number_of_rows;

    return getMarkUpperBoundForRow(last_row_pos) - from_mark;
}

size_t MergeTreeIndexGranularityConstant::countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + offset_in_rows + number_of_rows;

    return getRowsCountInRange(from_mark, std::max(1UL, getMarkUpperBoundForRow(last_row_pos))) - offset_in_rows;
}

void MergeTreeIndexGranularityConstant::fixFromRowsCount(size_t rows_count)
{
    if (num_marks_without_final == 0)
        return;

    /// Calculate the expected number of data marks for the given row count.
    size_t expected_data_marks = (rows_count + constant_granularity - 1) / constant_granularity;
    if (rows_count == 0)
        expected_data_marks = 0;

    /// If we have more marks than expected data marks, the extra mark is a final mark.
    if (num_marks_without_final > expected_data_marks && !has_final_mark)
    {
        has_final_mark = true;
        --num_marks_without_final;
    }

    /// Adjust the last mark granularity to match the actual row count.
    if (num_marks_without_final > 0)
    {
        size_t rows_in_complete_granules = (num_marks_without_final - 1) * constant_granularity;
        if (rows_count > rows_in_complete_granules)
            last_mark_granularity = rows_count - rows_in_complete_granules;
        else
            last_mark_granularity = 0;
    }
}

std::shared_ptr<MergeTreeIndexGranularityConstant> MergeTreeIndexGranularityConstant::fixedFromRowsCount(size_t rows_count) const
{
    auto result = std::make_shared<MergeTreeIndexGranularityConstant>(constant_granularity, last_mark_granularity, num_marks_without_final, has_final_mark);
    result->fixFromRowsCount(rows_count);
    return result;
}

std::string MergeTreeIndexGranularityConstant::describe() const
{
    return fmt::format(
        "Constant(constant_granularity: {}, last_mark_granularity: {}, num_marks_without_final: {}, has_final_mark: {})",
        constant_granularity, last_mark_granularity, num_marks_without_final, has_final_mark);
}

}
