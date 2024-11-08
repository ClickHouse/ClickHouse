#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/Exception.h>

#include <Common/logger_useful.h>

#ifndef NDEBUG
/// TODO: Review. The exceptions in non-const members might need to go elsewhere
/// And we might need a better approach to load data from disk that isn't push_back in a loop with conditionals
// just to compress it later
/// Also, the only reason we have 2 vectors after compression is for debugging, so that should be simplified
#    define DEBUG_MARKS 1
#endif

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

    if (compressed)
    {
        size_t value;
        if (mark_index < compressed)
            value = uncompressed_marks_partial_sums[0];
        else
        {
            size_t prev = mark_index == compressed ? compressed * uncompressed_marks_partial_sums[0]
                                                   : uncompressed_marks_partial_sums[mark_index - compressed];
            value = uncompressed_marks_partial_sums[mark_index - compressed + 1] - prev;
        }
#ifdef DEBUG_MARKS
        size_t uncompressed_value
            = mark_index == 0 ? marks_rows_partial_sums[0] : marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
        if (value != uncompressed_value)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "getMarkRows(mark {}): {} vs {} (Compressed {})",
                mark_index,
                value,
                uncompressed_value,
                compressed);
#endif
        return value;
    }

    if (mark_index == 0)
        return marks_rows_partial_sums[0];
    return marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
}

size_t MergeTreeIndexGranularity::getMarkStartingRow(size_t mark_index) const
{
    if (compressed)
    {
        size_t value;
        if (mark_index < compressed)
            value = mark_index * uncompressed_marks_partial_sums[0];
        else
        {
            value = marks_rows_partial_sums[mark_index];
        }
#ifdef DEBUG_MARKS
        size_t uncompressed_value = mark_index == 0 ? 0 : marks_rows_partial_sums[mark_index - 1];
        if (value != uncompressed_value)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "getMarkStartingRow(mark {}): {} vs {} (Compressed {})",
                mark_index,
                value,
                uncompressed_value,
                compressed);
#endif
        return value;
    }

    if (mark_index == 0)
        return 0;
    return marks_rows_partial_sums[mark_index - 1];
}

size_t MergeTreeIndexGranularity::getLastMarkRows() const
{
    size_t last = getMarksCount() - 1;
    return getMarkRows(last);
}

size_t MergeTreeIndexGranularity::getMarksCount() const
{
    if (compressed)
    {
        size_t value = compressed + uncompressed_marks_partial_sums.size() - 1;
#ifdef DEBUG_MARKS
        size_t uncompressed_value = marks_rows_partial_sums.size();
        if (value != uncompressed_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "getMarksCount: {} vs {} (Compressed {})", value, uncompressed_value, compressed);
#endif
        return value;
    }

    return marks_rows_partial_sums.size();
}

size_t MergeTreeIndexGranularity::getTotalRows() const
{
    if (compressed)
    {
        size_t value = uncompressed_marks_partial_sums.back();

#ifdef DEBUG_MARKS
        size_t uncompressed_value = marks_rows_partial_sums.empty() ? 0 : marks_rows_partial_sums.back();
        if (value != uncompressed_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "getTotalRows: {} vs {} (Compressed {})", value, uncompressed_value, compressed);
#endif
        return value;
    }

    if (marks_rows_partial_sums.empty())
        return 0;
    return marks_rows_partial_sums.back();
}

size_t MergeTreeIndexGranularity::getMarksCountWithoutFinal() const
{
    return getMarksCount() - hasFinalMark();
}

size_t MergeTreeIndexGranularity::getLastNonFinalMarkRows() const
{
    size_t last_mark_rows = getLastMarkRows();
    if (last_mark_rows != 0)
        return last_mark_rows;
    return getMarkRows(marks_rows_partial_sums.size() - 2);
}

bool MergeTreeIndexGranularity::hasFinalMark() const
{
    return getLastMarkRows() == 0;
}

bool MergeTreeIndexGranularity::empty() const
{
    return !compressed && marks_rows_partial_sums.empty();
}

bool MergeTreeIndexGranularity::isInitialized() const
{
    return initialized;
}

void MergeTreeIndexGranularity::setInitialized()
{
    initialized = true;
}

void MergeTreeIndexGranularity::appendMark(size_t rows_count)
{
#ifdef DEBUG_MARKS
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "appendMark while compressed");
#endif
    if (marks_rows_partial_sums.empty())
        marks_rows_partial_sums.push_back(rows_count);
    else
        marks_rows_partial_sums.push_back(marks_rows_partial_sums.back() + rows_count);
}

void MergeTreeIndexGranularity::addRowsToLastMark(size_t rows_count)
{
#ifdef DEBUG_MARKS
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "addRowsToLastMark while compressed");
#endif
    if (marks_rows_partial_sums.empty())
        marks_rows_partial_sums.push_back(rows_count);
    else
        marks_rows_partial_sums.back() += rows_count;
}

void MergeTreeIndexGranularity::popMark()
{
#ifdef DEBUG_MARKS
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "popMark while compressed");
#endif
    if (!marks_rows_partial_sums.empty())
        marks_rows_partial_sums.pop_back();
}

size_t MergeTreeIndexGranularity::getRowsCountInRange(size_t begin, size_t end) const
{
    size_t result = getMarkStartingRow(end) - getMarkStartingRow(begin);

#ifdef DEBUG_MARKS
    size_t subtrahend = 0;
    if (begin != 0)
        subtrahend = marks_rows_partial_sums[begin - 1];
    size_t old_result = marks_rows_partial_sums[end - 1] - subtrahend;
    if (result != old_result)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "getRowsCountInRange({} {}): {} vs {} (Compressed {})", begin, end, result, old_result, compressed);
#endif

    return result;
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

size_t MergeTreeIndexGranularity::getMarkForRow(size_t row) const
{
    if (compressed)
    {
        size_t result;
        size_t compressed_granularity = uncompressed_marks_partial_sums[0];
        if (row < compressed * compressed_granularity)
        {
            result = row / compressed_granularity;
        }
        else
        {
            auto it = std::upper_bound(uncompressed_marks_partial_sums.begin() + 1, uncompressed_marks_partial_sums.end(), row);
            result = it - uncompressed_marks_partial_sums.begin() + compressed;
        }

#ifdef DEBUG_MARKS
        auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), row);
        size_t uncompressed_result = it - marks_rows_partial_sums.begin();

        if (result != uncompressed_result)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "getMarkForRow({}): {} vs {} (Compressed {})", row, result, uncompressed_result, compressed);
#endif
        return result;
    }

    auto it = std::upper_bound(marks_rows_partial_sums.begin(), marks_rows_partial_sums.end(), row);
    return it - marks_rows_partial_sums.begin();
}

size_t MergeTreeIndexGranularity::countMarksForRows(size_t from_mark, size_t number_of_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + number_of_rows;
    size_t to_mark = getMarkForRow(last_row_pos);
    return to_mark - from_mark;
}

size_t MergeTreeIndexGranularity::countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const
{
    size_t rows_before_mark = getMarkStartingRow(from_mark);
    size_t last_row_pos = rows_before_mark + offset_in_rows + number_of_rows;
    size_t to_mark = getMarkForRow(last_row_pos);

    return getRowsCountInRange(from_mark, std::max(1UL, to_mark)) - offset_in_rows;
}

void MergeTreeIndexGranularity::resizeWithFixedGranularity(size_t size, size_t fixed_granularity)
{
#ifdef DEBUG_MARKS
    if (compressed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "resizeWithFixedGranularity while compressed");
#endif
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
    return fmt::format(
        "initialized: {}, marks_rows_partial_sums: [{}], compressed: {}, uncompressed_marks_partial_sums: [{}]",
        initialized,
        fmt::join(marks_rows_partial_sums, ", "),
        compressed,
        fmt::join(uncompressed_marks_partial_sums, ", "));
}

void MergeTreeIndexGranularity::shrinkToFitInMemory()
{
    if (!tryCompressInMemory())
        marks_rows_partial_sums.shrink_to_fit();
}

bool MergeTreeIndexGranularity::tryCompressInMemory()
{
    if (compressed || marks_rows_partial_sums.size() <= 2)
        return compressed;

    const size_t fixed_granularity_value = marks_rows_partial_sums[0];
    if (fixed_granularity_value <= 1)
        return compressed;
    const size_t current_size = marks_rows_partial_sums.size();

    size_t i = 1;
    for (; i < current_size; i++)
    {
        if (marks_rows_partial_sums[i] - marks_rows_partial_sums[i - 1] != fixed_granularity_value)
        {
            i--;
            break;
        }
    }

    const size_t uncompressed = current_size - i - 1;
    const size_t compressed_size = 1 + uncompressed; /* Store the fixed value */

    /// Threshold chosen without measurements, just to avoid copying values around for little benefit
    const bool compress = compressed_size < (double(current_size) * 0.4f);
    LOG_TRACE(
        &Poco::Logger::get("MergeTreeIndexGranularity:: tryCompressInMemory"),
        "Original size {}. Compressed size {}. Decision: {}",
        current_size,
        compressed_size,
        compress ? "Compress" : "Not compress");

    if (!compress)
        return false;

    compressed = current_size - uncompressed;
    uncompressed_marks_partial_sums.resize(compressed_size);
    uncompressed_marks_partial_sums[0] = fixed_granularity_value;
    std::memcpy(&uncompressed_marks_partial_sums[1], &marks_rows_partial_sums[compressed], uncompressed * sizeof(size_t));

#ifndef DEBUG_MARKS
    marks_rows_partial_sums.clear();
#endif

    return true;
}
}
