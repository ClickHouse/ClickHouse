#include <Storages/MergeTree/MarkRangeChunking.h>

namespace DB
{

std::vector<MarkRanges> splitMarkRanges(const MarkRanges & ranges, size_t target_marks_per_chunk)
{
    std::vector<MarkRanges> chunks;

    if (ranges.getNumberOfMarks() == 0)
        return chunks;

    if (target_marks_per_chunk == 0)
    {
        chunks.emplace_back(ranges);
        return chunks;
    }

    MarkRanges current;
    size_t current_marks = 0;

    for (const auto & range : ranges)
    {
        size_t begin = range.begin;
        while (begin < range.end)
        {
            size_t take = std::min(target_marks_per_chunk - current_marks, range.end - begin);
            current.push_back(MarkRange(begin, begin + take));
            current_marks += take;
            begin += take;

            if (current_marks == target_marks_per_chunk)
            {
                chunks.push_back(std::move(current));
                current.clear();
                current_marks = 0;
            }
        }
    }

    if (current_marks > 0)
        chunks.push_back(std::move(current));

    return chunks;
}

}
