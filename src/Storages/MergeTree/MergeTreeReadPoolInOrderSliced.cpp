#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <algorithm>
#include <limits>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// A segment is long enough to amortize one range request on remote storage over several slices.
constexpr size_t slices_per_segment = 8;

/// Takes up to max_marks marks from the front of the ranges. Does not continue across a gap that ends
/// below no_gaps_below: such a gap holds marks that were cut into slices earlier (they came back to the
/// lane from a segment taken away from an idle source), and a slice spanning it would deliver its rows
/// before those slices although it is ordered by its first mark.
MarkRanges cutMarks(MarkRanges & from, size_t max_marks, size_t no_gaps_below)
{
    MarkRanges result;
    while (max_marks > 0 && !from.empty())
    {
        auto & range = from.front();
        if (!result.empty() && range.begin != result.back().end && result.back().end < no_gaps_below)
            break;

        const size_t marks = std::min(range.end - range.begin, max_marks);
        result.emplace_back(range.begin, range.begin + marks);
        range.begin += marks;
        max_marks -= marks;
        if (range.begin == range.end)
            from.pop_front();
    }
    return result;
}

/// Lanes without a known boundary go last.
int compareBoundaries(const Block & lhs, const Block & rhs)
{
    if (lhs.columns() == 0 || rhs.columns() == 0)
        return static_cast<int>(lhs.columns() == 0) - static_cast<int>(rhs.columns() == 0);

    for (size_t i = 0; i < lhs.columns(); ++i)
    {
        int result = lhs.getByPosition(i).column->compareAt(0, 0, *rhs.getByPosition(i).column, 1);
        if (result != 0)
            return result;
    }
    return 0;
}

}

MergeTreeReadPoolInOrderSliced::MergeTreeReadPoolInOrderSliced(
    RangesInDataParts parts_,
    MutationsSnapshotPtr mutations_snapshot_,
    VirtualFields shared_virtual_fields_,
    const IndexReadTasks & index_read_tasks_,
    const StorageSnapshotPtr & storage_snapshot_,
    const FilterDAGInfoPtr & row_level_filter_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const MergeTreeReadTask::BlockSizeParams & params_,
    const ContextPtr & context_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
    size_t num_sources_,
    const Block & primary_key_header_)
    : MergeTreeReadPoolBase(
        std::move(parts_),
        std::move(mutations_snapshot_),
        std::move(shared_virtual_fields_),
        index_read_tasks_,
        storage_snapshot_,
        row_level_filter_,
        prewhere_info_,
        actions_settings_,
        reader_settings_,
        column_names_,
        settings_,
        params_,
        context_)
    , updater(std::move(updater_))
    , num_sources(num_sources_)
    , max_slice_marks(std::max<size_t>(1, pool_settings.min_marks_for_concurrent_read))
    , segment_marks(max_slice_marks * slices_per_segment)
    , segments(num_sources_)
    , pending(num_sources_)
{
    lanes.reserve(parts_ranges.size());
    boundaries.reserve(parts_ranges.size());
    for (size_t lane = 0; lane < parts_ranges.size(); ++lane)
    {
        lanes.push_back(Lane{.unread = parts_ranges[lane].ranges});
        boundaries.push_back(buildBoundary(lane, primary_key_header_));
    }

    lanes_by_boundary.resize(lanes.size());
    std::iota(lanes_by_boundary.begin(), lanes_by_boundary.end(), 0);
    std::stable_sort(lanes_by_boundary.begin(), lanes_by_boundary.end(), [this](size_t lhs, size_t rhs)
    {
        return compareBoundaries(boundaries[lhs], boundaries[rhs]) < 0;
    });
}

Block MergeTreeReadPoolInOrderSliced::buildBoundary(size_t lane, const Block & primary_key_header) const
{
    const auto & part = parts_ranges[lane];
    if (primary_key_header.columns() == 0 || part.ranges.empty())
        return {};

    const size_t mark = part.ranges.front().begin;
    const auto & index = per_part_infos[lane]->data_part_info->getIndexPtr();
    if (index->size() < primary_key_header.columns())
        return {};

    for (size_t i = 0; i < primary_key_header.columns(); ++i)
        if ((*index)[i]->size() <= mark)
            return {};

    auto columns = primary_key_header.cloneEmptyColumns();
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i]->insert((*(*index)[i])[mark]);

    return primary_key_header.cloneWithColumns(std::move(columns));
}

bool MergeTreeReadPoolInOrderSliced::laneHasUnreadMarks(size_t lane) const
{
    return laneFirstUnreadMark(lane) != std::numeric_limits<size_t>::max();
}

bool MergeTreeReadPoolInOrderSliced::laneHasMarksOutsideSegments(size_t lane) const
{
    std::lock_guard lock(mutex);
    return !lanes[lane].unread.empty();
}

size_t MergeTreeReadPoolInOrderSliced::laneFirstUnreadMark(size_t lane) const
{
    std::lock_guard lock(mutex);
    size_t first_mark = std::numeric_limits<size_t>::max();

    if (!lanes[lane].unread.empty())
        first_mark = lanes[lane].unread.front().begin;

    for (const auto & segment : segments)
        if (segment && segment->lane == lane && !segment->unread.empty())
            first_mark = std::min(first_mark, segment->unread.front().begin);

    return first_mark;
}

size_t MergeTreeReadPoolInOrderSliced::laneFirstMarkOutsideSegments(size_t lane) const
{
    std::lock_guard lock(mutex);
    if (lanes[lane].unread.empty())
        return std::numeric_limits<size_t>::max();
    return lanes[lane].unread.front().begin;
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::segmentLane(size_t source) const
{
    std::lock_guard lock(mutex);
    if (!segments[source])
        return std::nullopt;
    return segments[source]->lane;
}

bool MergeTreeReadPoolInOrderSliced::segmentHasUnreadMarks(size_t source) const
{
    std::lock_guard lock(mutex);
    return segments[source] && !segments[source]->unread.empty();
}

bool MergeTreeReadPoolInOrderSliced::hasPendingSlice(size_t source) const
{
    std::lock_guard lock(mutex);
    return pending[source].has_value();
}

size_t MergeTreeReadPoolInOrderSliced::segmentFirstUnreadMark(size_t source) const
{
    std::lock_guard lock(mutex);
    if (!segments[source] || segments[source]->unread.empty())
        return std::numeric_limits<size_t>::max();
    return segments[source]->unread.front().begin;
}

void MergeTreeReadPoolInOrderSliced::returnSegmentToLane(size_t source)
{
    auto & segment = segments[source];
    if (!segment)
        return;

    /// Ranges of a lane are disjoint, so merging by the first mark keeps them sorted.
    auto & unread = lanes[segment->lane].unread;
    MarkRanges merged;
    std::merge(
        unread.begin(), unread.end(),
        segment->unread.begin(), segment->unread.end(),
        std::back_inserter(merged),
        [](const MarkRange & lhs, const MarkRange & rhs) { return lhs.begin < rhs.begin; });
    unread = std::move(merged);

    segment.reset();
}

void MergeTreeReadPoolInOrderSliced::openSegment(size_t source, size_t lane)
{
    std::lock_guard lock(mutex);

    if (pending[source])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} has a slice assigned and cannot be bound to a new segment", source);

    returnSegmentToLane(source);

    auto & lane_state = lanes[lane];
    if (lane_state.unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} has no marks left for a new segment", lane);

    /// A segment may span marks read by other segments; its slices may not (see assignSlice).
    MarkRanges extent = cutMarks(lane_state.unread, segment_marks, /*no_gaps_below=*/ 0);
    segments[source] = Segment{.lane = lane, .extent = extent, .unread = extent, .has_readers = false};
}

MergeTreeReadPoolInOrderSliced::SliceDescription MergeTreeReadPoolInOrderSliced::assignSlice(size_t source)
{
    std::lock_guard lock(mutex);

    auto & segment = segments[source];
    if (!segment || segment->unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} has no segment with unread marks", source);
    if (pending[source])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} already has a slice assigned", source);

    auto & lane_state = lanes[segment->lane];
    const size_t ramp_marks = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    MarkRanges ranges = cutMarks(segment->unread, std::min(max_slice_marks, ramp_marks), lane_state.max_cut_mark);
    ++lane_state.slices_cut;
    lane_state.max_cut_mark = std::max(lane_state.max_cut_mark, ranges.back().end);

    SliceDescription description{
        .first_mark = ranges.front().begin,
        .rows = per_part_infos[segment->lane]->data_part_info->getIndexGranularity().getRowsCountInRanges(ranges),
    };

    pending[source] = PendingSlice{.ranges = std::move(ranges)};
    return description;
}

MergeTreeReadTaskPtr MergeTreeReadPoolInOrderSliced::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    MergeTreeReadTaskInfoPtr info;
    MarkRanges ranges;
    MarkRanges extent;
    bool reuse_readers = false;

    {
        std::lock_guard lock(mutex);

        auto & slice = pending[task_idx];
        if (!slice)
            return nullptr;

        const auto & segment = segments[task_idx];
        if (!segment)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} has a slice assigned but no segment", task_idx);

        info = per_part_infos[segment->lane];
        ranges = std::move(slice->ranges);
        extent = segment->extent;
        reuse_readers = segment->has_readers;
        slice.reset();
    }

    /// May block, so it runs outside of the mutex.
    ranges = refineReadRanges(*info, std::move(ranges));
    if (ranges.empty())
        return nullptr;

    const auto & data_part = info->data_part_info->getDataPart();
    auto patches_ranges = ranges_in_patch_parts.getRanges(data_part, info->patch_parts, ranges);

    MergeTreeReadTask::Readers readers;
    if (reuse_readers && previous_task && &previous_task->getInfo() == info.get())
    {
        readers = previous_task->releaseReaders();
    }
    else
    {
        /// Readers span the whole segment, so the following slices continue the same streams.
        auto extras = getExtras();
        if (previous_task)
            extras.value_size_map = previous_task->getMainReader().getAvgValueSizeHints();

        auto extent_patches_ranges = ranges_in_patch_parts.getRanges(data_part, info->patch_parts, extent);
        readers = MergeTreeReadTask::createReaders(info, extras, extent, extent_patches_ranges);
    }

    {
        std::lock_guard lock(mutex);
        segments[task_idx]->has_readers = true;
    }

    return createTask(info, std::move(readers), std::move(ranges), std::move(patches_ranges), updater);
}

}
