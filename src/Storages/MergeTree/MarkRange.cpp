#include <Storages/MergeTree/MarkRange.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/defines.h>
#include <fmt/ranges.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MarkRange::MarkRange(size_t begin_, size_t end_) : begin(begin_), end(end_)
{
    chassert(begin <= end);
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
    if (*this != rhs)
    {
        const bool is_intersection = (begin <= rhs.begin && rhs.begin < end)
            || (rhs.begin <= begin && begin < rhs.end);

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

MarkRangesInfo::MarkRangesInfo(UUID table_uuid_, const String & part_name_, size_t marks_count_, bool has_final_mark_, MarkRanges mark_ranges_)
    : table_uuid(table_uuid_)
    , part_name(part_name_)
    , marks_count(marks_count_)
    , has_final_mark(has_final_mark_)
    , mark_ranges(mark_ranges_)
{}
void MarkRangesInfo::appendMarkRanges(const MarkRanges & mark_ranges_)
{
    mark_ranges.insert(mark_ranges.end(), mark_ranges_.begin(), mark_ranges_.end());
}

size_t MarkRangeHash::operator()(const MarkRange & range) const
{
    SipHash hash;
    hash.update(range.begin);
    hash.update(range.end);
    return hash.get64();
}

MarkRanges intersectMarkRanges(const MarkRanges & a, const MarkRanges & b, size_t min_marks_for_seek)
{
    /// Two-pointer merge assumes both sides are sorted and non-overlapping. The
    /// min-gap coalescing below relies on that invariant to guarantee
    /// `begin - result.back().end` cannot underflow. We use a cheap single-pass
    /// check rather than `assertSortedAndNonIntersecting` (which sort-copies the
    /// input) and gate it on debug/sanitizer builds so release isn't paying for
    /// the scan on the hot path.
#ifdef DEBUG_OR_SANITIZER_BUILD
    auto debug_assert_sorted = [](const MarkRanges & ranges)
    {
        for (size_t i = 1; i < ranges.size(); ++i)
            chassert(ranges[i - 1].end <= ranges[i].begin);
    };
    debug_assert_sorted(a);
    debug_assert_sorted(b);
#endif

    MarkRanges result;
    const auto * it_a = a.begin();
    const auto * it_b = b.begin();

    while (it_a != a.end() && it_b != b.end())
    {
        size_t begin = std::max(it_a->begin, it_b->begin);
        size_t end = std::min(it_a->end, it_b->end);

        if (begin < end)
        {
            if (min_marks_for_seek > 0 && !result.empty()
                && begin - result.back().end <= min_marks_for_seek)
                result.back().end = end;
            else
                result.emplace_back(begin, end);
        }

        if (it_a->end < it_b->end)
            ++it_a;
        else
            ++it_b;
    }

    return result;
}

}
