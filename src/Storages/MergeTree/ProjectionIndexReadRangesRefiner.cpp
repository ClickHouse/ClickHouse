#include <Storages/MergeTree/ProjectionIndexReadRangesRefiner.h>

#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{

ProjectionIndexReadRangesRefiner::ProjectionIndexReadRangesRefiner(
    MergeTreeIndexBuildContextPtr index_build_context_, StorageMetadataPtr metadata_snapshot_)
    : index_build_context(std::move(index_build_context_))
    , metadata_snapshot(std::move(metadata_snapshot_))
{
    chassert(index_build_context);
}

MarkRanges ProjectionIndexReadRangesRefiner::refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const
{
    auto projection_it = index_build_context->projection_read_ranges.find(info.part_index_in_query);
    if (projection_it == index_build_context->projection_read_ranges.end())
        return ranges;

    const auto & part_ranges = index_build_context->read_ranges.at(info.part_index_in_query);
    const auto & all_updated_columns = info.alter_conversions->getAllUpdatedColumns();

    /// Built once per part; concurrent callers wait on a shared future inside the pool.
    /// The same cached result is later reused by MergeTreeReaderIndex for row-level filtering.
    auto index_read_result = index_build_context->index_reader_pool->getOrBuildIndexReadResult(
        part_ranges, projection_it->second, metadata_snapshot, all_updated_columns);

    /// The read may have been cancelled; refinement is an optimization, so just do nothing.
    if (!index_read_result || !index_read_result->projection_index_read_result)
        return ranges;

    const auto & bitmap = *index_read_result->projection_index_read_result;
    const auto & index_granularity = info.data_part_info->getIndexGranularity();

    /// Same predicate as the projection branch of MergeTreeReaderIndex::canSkipMark,
    /// applied before the ranges become a read task.
    MarkRanges result;
    size_t dropped_marks = 0;
    for (const auto & range : ranges)
    {
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            size_t rows_begin = index_granularity.getMarkStartingRow(mark);
            size_t rows_end = rows_begin + index_granularity.getMarkRows(mark);

            if (bitmap.rangeAllZero(rows_begin, rows_end))
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
            index_build_context->index_reader_pool->clear(info.data_part_info->getDataPart());
    }

    return result;
}

}
