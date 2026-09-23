#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Takes up to max_marks marks from the front of the ranges.
MarkRanges cutMarks(MarkRanges & from, size_t max_marks)
{
    MarkRanges result;
    while (max_marks > 0 && !from.empty())
    {
        auto & range = from.front();
        const size_t marks = std::min(range.end - range.begin, max_marks);
        result.emplace_back(range.begin, range.begin + marks);
        range.begin += marks;
        max_marks -= marks;
        if (range.begin == range.end)
            from.pop_front();
    }
    return result;
}

/// Reader sets kept per lane for sources that come back to it. Every set holds read buffers for all
/// columns, so only a few are kept.
constexpr size_t max_parked_readers_per_lane = 2;

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
    , bound_lane(num_sources_)
    , last_task_lane(num_sources_)
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

MarkRanges MergeTreeReadPoolInOrderSliced::readerExtent(size_t lane, size_t first_mark) const
{
    MarkRanges extent;
    for (const auto & range : parts_ranges[lane].ranges)
        if (range.end > first_mark)
            extent.emplace_back(std::max(range.begin, first_mark), range.end);
    return extent;
}

bool MergeTreeReadPoolInOrderSliced::laneHasUnreadMarks(size_t lane) const
{
    std::lock_guard lock(mutex);
    return !lanes[lane].unread.empty();
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::sourceLane(size_t source) const
{
    std::lock_guard lock(mutex);
    return bound_lane[source];
}

bool MergeTreeReadPoolInOrderSliced::hasPendingSlice(size_t source) const
{
    std::lock_guard lock(mutex);
    return pending[source].has_value();
}

void MergeTreeReadPoolInOrderSliced::releaseLaneReaders(size_t lane)
{
    std::lock_guard lock(mutex);
    lanes[lane].parked_readers.clear();
}

void MergeTreeReadPoolInOrderSliced::bindSource(size_t source, size_t lane)
{
    std::lock_guard lock(mutex);

    if (pending[source])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} has a slice assigned and cannot be bound to another lane", source);

    bound_lane[source] = lane;
}

MergeTreeReadPoolInOrderSliced::SliceDescription MergeTreeReadPoolInOrderSliced::assignSlice(size_t source)
{
    std::lock_guard lock(mutex);

    const auto & lane = bound_lane[source];
    if (!lane)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} is not bound to a lane", source);
    if (pending[source])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} already has a slice assigned", source);

    auto & lane_state = lanes[*lane];
    if (lane_state.unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} has no marks left", *lane);

    const size_t ramp_marks = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    MarkRanges ranges = cutMarks(lane_state.unread, std::min(max_slice_marks, ramp_marks));
    ++lane_state.slices_cut;

    SliceDescription description{
        .first_mark = ranges.front().begin,
        .rows = per_part_infos[*lane]->data_part_info->getIndexGranularity().getRowsCountInRanges(ranges),
    };

    pending[source] = PendingSlice{.ranges = std::move(ranges)};
    return description;
}

MergeTreeReadTaskPtr MergeTreeReadPoolInOrderSliced::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    MergeTreeReadTaskInfoPtr info;
    MarkRanges ranges;
    size_t lane = 0;

    {
        std::lock_guard lock(mutex);

        auto & slice = pending[task_idx];
        if (!slice)
            return nullptr;

        if (!bound_lane[task_idx])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} has a slice assigned but is not bound to a lane", task_idx);

        lane = *bound_lane[task_idx];
        info = per_part_infos[lane];
        ranges = std::move(slice->ranges);
        slice.reset();
    }

    /// May block, so it runs outside of the mutex.
    ranges = refineReadRanges(*info, std::move(ranges));
    if (ranges.empty())
        return nullptr;

    const auto & data_part = info->data_part_info->getDataPart();
    auto patches_ranges = ranges_in_patch_parts.getRanges(data_part, info->patch_parts, ranges);

    /// Readers follow the lane, not the source: a source that switches lanes leaves its readers in the
    /// lane it read before and takes the readers another source left in the new lane, if there are any.
    /// The size hints are taken before the readers may be given away.
    auto extras = getExtras();
    if (previous_task)
        extras.value_size_map = previous_task->getMainReader().getAvgValueSizeHints();

    MergeTreeReadTask::Readers readers;
    bool has_readers = false;
    {
        std::lock_guard lock(mutex);

        auto & last_lane = last_task_lane[task_idx];
        if (previous_task && last_lane == lane)
        {
            readers = previous_task->releaseReaders();
            has_readers = true;
        }
        else
        {
            if (previous_task && last_lane)
            {
                auto & previous = lanes[*last_lane];
                if (!previous.unread.empty() && previous.parked_readers.size() < max_parked_readers_per_lane)
                    previous.parked_readers.push_back(previous_task->releaseReaders());
            }

            auto & parked = lanes[lane].parked_readers;
            if (!parked.empty())
            {
                readers = std::move(parked.back());
                parked.pop_back();
                has_readers = true;
            }
        }
        last_lane = lane;
    }

    if (!has_readers)
    {
        MarkRanges extent = readerExtent(lane, ranges.front().begin);
        auto extent_patches_ranges = ranges_in_patch_parts.getRanges(data_part, info->patch_parts, extent);
        readers = MergeTreeReadTask::createReaders(info, extras, extent, extent_patches_ranges);
    }

    return createTask(info, std::move(readers), std::move(ranges), std::move(patches_ranges), updater);
}

}
