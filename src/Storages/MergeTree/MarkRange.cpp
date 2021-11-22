#include "MarkRange.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool MarkRange::operator==(const MarkRange & rhs) const
{
    return begin == rhs.begin && end == rhs.end;
}

bool MarkRange::operator<(const MarkRange & rhs) const
{
    /// We allow only consecutive non-intersecting ranges
    if (rhs.begin > begin && rhs.begin < end)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad marks");
    return begin < rhs.begin && end <= rhs.begin;
}

size_t getLastMark(const MarkRanges & ranges)
{
    size_t current_task_last_mark = 0;
    for (const auto & mark_range : ranges)
        current_task_last_mark = std::max(current_task_last_mark, mark_range.end);
    return current_task_last_mark;
}

}
