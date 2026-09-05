#include <Storages/MergeTree/IndexReadRangesRefiner.h>

#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{

IndexReadRangesRefiner::IndexReadRangesRefiner(
    MergeTreeIndexBuildContextPtr index_build_context_, StorageMetadataPtr metadata_snapshot_)
    : index_build_context(std::move(index_build_context_))
    , metadata_snapshot(std::move(metadata_snapshot_))
{
    chassert(index_build_context);
}

MarkRanges IndexReadRangesRefiner::refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const
{
    auto projection_it = index_build_context->projection_read_ranges.find(info.part_index_in_query);
    bool has_projection_ranges = projection_it != index_build_context->projection_read_ranges.end();

    /// No index read result can exist for this part, do not touch the shared registry.
    if (!has_projection_ranges && !index_build_context->index_reader_pool->hasSkipIndexReader())
        return ranges;

    const auto & skip_index_input = index_build_context->read_ranges.at(info.part_index_in_query);
    const auto & all_updated_columns = info.alter_conversions->getAllUpdatedColumns();

    /// Built once per part; concurrent callers wait on a shared future inside the pool.
    /// The same cached result is later reused by MergeTreeReaderIndex for granule- and row-level filtering.
    auto index_read_result = index_build_context->index_reader_pool->getOrBuildIndexReadResult(
        info.part_index_in_query,
        info.data_part_info,
        skip_index_input,
        has_projection_ranges ? projection_it->second : RangesInDataParts{},
        metadata_snapshot,
        all_updated_columns);

    /// The read may have been cancelled; refinement is an optimization, so just do nothing.
    if (!index_read_result || (!index_read_result->skip_index_read_result && !index_read_result->projection_index_read_result))
        return ranges;

    const auto & index_granularity = info.data_part_info->getIndexGranularity();
    MarkRanges result;
    size_t dropped_marks = 0;

    for (const auto & range : ranges)
    {
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            if (index_read_result->canSkipMark(mark, index_granularity))
            {
                ++dropped_marks;
                continue;
            }

            if (!result.empty() && result.back().end == mark)
                result.back().end = mark + 1;
            else
                result.emplace_back(mark, mark + 1);
        }
    }

    /// Marks dropped here never pass through MergeTreeIndexBuildContext::getPreparedIndexReadResult,
    /// so account for them now; otherwise the cached per-part index result would never be released.
    if (dropped_marks != 0)
    {
        auto & remaining_marks = index_build_context->part_remaining_marks.at(info.part_index_in_query).value;
        bool part_fully_processed = remaining_marks.fetch_sub(dropped_marks, std::memory_order_acq_rel) == dropped_marks;

        if (part_fully_processed)
            index_build_context->index_reader_pool->clear(info.part_index_in_query);
    }

    return result;
}

}
