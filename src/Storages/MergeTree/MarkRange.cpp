#include "MarkRange.h"

namespace DB
{

size_t getLastMark(const MarkRanges & ranges)
{
    size_t current_task_last_mark = 0;
    for (const auto & mark_range : ranges)
        current_task_last_mark = std::max(current_task_last_mark, mark_range.end);
    return current_task_last_mark;
}

}
