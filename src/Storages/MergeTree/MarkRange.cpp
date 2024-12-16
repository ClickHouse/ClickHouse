#include "MarkRange.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

size_t MarkRange::getNumberOfMarks() const
{
    return end - begin;
}

bool MarkRange::operator==(const MarkRange & rhs) const
{
    return begin == rhs.begin && end == rhs.end;
}

bool MarkRange::operator<(const MarkRange & rhs) const
{
    /// We allow only consecutive non-intersecting ranges
    /// Here we check whether a beginning of one range lies inside another range
    /// (ranges are intersect)
    if (this != &rhs)
    {
        const bool is_intersection = (begin <= rhs.begin && rhs.begin < end) ||
            (rhs.begin <= begin && begin < rhs.end);

        if (is_intersection)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Intersecting mark ranges are not allowed, it is a bug! "
                "First range ({}, {}), second range ({}, {})",
                begin, end, rhs.begin, rhs.end);
    }

    return begin < rhs.begin && end <= rhs.begin;
}

size_t getLastMark(const MarkRanges & ranges)
{
    size_t current_task_last_mark = 0;
    for (const auto & mark_range : ranges)
        current_task_last_mark = std::max(current_task_last_mark, mark_range.end);
    return current_task_last_mark;
}

std::string toString(const MarkRanges & ranges)
{
    std::string result;
    for (const auto & mark_range : ranges)
    {
        if (!result.empty())
            result += ", ";
        result += "(" + std::to_string(mark_range.begin) + ", " + std::to_string(mark_range.end) + ")";
    }
    return result;
}

void assertSortedAndNonIntersecting(const MarkRanges & ranges)
{
    MarkRanges ranges_copy(ranges.begin(), ranges.end());
    /// Should also throw an exception if interseting range is found during comparison.
    std::sort(ranges_copy.begin(), ranges_copy.end());
    if (ranges_copy != ranges)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Expected sorted and non intersecting ranges. Ranges: {}",
            toString(ranges));
}

size_t MarkRanges::getNumberOfMarks() const
{
    size_t result = 0;
    for (const auto & mark : *this)
        result += mark.getNumberOfMarks();
    return result;
}

bool MarkRanges::isOneRangeForWholePart(size_t num_marks_in_part) const
{
    return size() == 1 && front().begin == 0 && front().end == num_marks_in_part;
}

void MarkRanges::serialize(WriteBuffer & out) const
{
    writeBinaryLittleEndian(this->size(), out);

    for (const auto & [begin, end] : *this)
    {
        writeBinaryLittleEndian(begin, out);
        writeBinaryLittleEndian(end, out);
    }
}

String MarkRanges::describe() const
{
    return fmt::format("Size: {}, Data: {}", this->size(), fmt::join(*this, ","));
}

void MarkRanges::deserialize(ReadBuffer & in)
{
    size_t size = 0;
    readBinaryLittleEndian(size, in);

    this->resize(size);
    for (size_t i = 0; i < size; ++i)
    {
        readBinaryLittleEndian((*this)[i].begin, in);
        readBinaryLittleEndian((*this)[i].end, in);
    }
}

}
