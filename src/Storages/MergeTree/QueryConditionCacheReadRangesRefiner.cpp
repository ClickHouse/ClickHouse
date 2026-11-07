#include <Storages/MergeTree/QueryConditionCacheReadRangesRefiner.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

#include <algorithm>

namespace DB
{

QueryConditionCacheReadRangesPtr QueryConditionCacheReadRanges::create(
    const RangesInDataParts & before, const RangesInDataParts & after, bool dropped_marks_are_counted)
{
    std::unordered_map<const IMergeTreeDataPart *, const RangesInDataPart *> after_by_part;
    for (const auto & part : after)
        after_by_part.emplace(part.data_part.get(), &part);

    auto result = std::make_shared<QueryConditionCacheReadRanges>();
    for (const auto & part : before)
    {
        auto it = after_by_part.find(part.data_part.get());
        if (it == after_by_part.end())
        {
            result->parts.emplace(part.data_part.get(), Entry{part.data_part, {}});
            continue;
        }

        if (it->second->getMarksCount() == part.getMarksCount())
            continue;

        MarkRanges ranges = it->second->ranges;
        std::sort(ranges.begin(), ranges.end(), [](const MarkRange & lhs, const MarkRange & rhs) { return lhs.begin < rhs.begin; });
        result->parts.emplace(part.data_part.get(), Entry{part.data_part, std::move(ranges)});
    }

    if (result->parts.empty())
        return nullptr;
    result->dropped_marks_are_counted = dropped_marks_are_counted;
    return result;
}

QueryConditionCacheReadRangesRefiner::QueryConditionCacheReadRangesRefiner(
    QueryConditionCacheReadRangesPtr read_ranges_, MergeTreeIndexBuildContextPtr index_build_context_, MergeTreeReadRangesRefinerPtr inner_)
    : read_ranges(std::move(read_ranges_))
    , index_build_context(std::move(index_build_context_))
    , inner(std::move(inner_))
{
    chassert(read_ranges);
}

MarkRanges QueryConditionCacheReadRangesRefiner::refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const
{
    auto part = info.data_part_info->getDataPart();
    auto it = part ? read_ranges->parts.find(part.get()) : read_ranges->parts.end();
    if (it != read_ranges->parts.end())
    {
        const auto & allowed = it->second.ranges;
        MarkRanges result;
        for (const auto & range : ranges)
        {
            /// The first allowed range that ends after the beginning of this one.
            const auto * allowed_it = std::upper_bound(
                allowed.begin(), allowed.end(), range.begin, [](size_t mark, const MarkRange & r) { return mark < r.end; });
            for (; allowed_it != allowed.end() && allowed_it->begin < range.end; ++allowed_it)
            {
                size_t begin = std::max(range.begin, allowed_it->begin);
                size_t end = std::min(range.end, allowed_it->end);
                if (!result.empty() && result.back().end == begin)
                    result.back().end = end;
                else
                    result.emplace_back(begin, end);
            }
        }

        /// Like `IndexReadRangesRefiner`: the dropped marks never reach `MergeTreeIndexBuildContext::getPreparedIndexReadResult`.
        size_t dropped_marks = ranges.getNumberOfMarks() - result.getNumberOfMarks();
        if (dropped_marks != 0 && index_build_context && read_ranges->dropped_marks_are_counted)
        {
            auto & remaining_marks = index_build_context->part_remaining_marks.at(info.part_index_in_query).value;
            if (remaining_marks.fetch_sub(dropped_marks, std::memory_order_acq_rel) == dropped_marks)
                index_build_context->index_reader_pool->clear(info.part_index_in_query);
        }

        ranges = std::move(result);
    }

    if (inner && !ranges.empty())
        return inner->refine(info, std::move(ranges));
    return ranges;
}

}
