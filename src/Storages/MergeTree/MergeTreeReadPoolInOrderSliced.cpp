#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <algorithm>

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

/// Readers created for a slice of this many marks read a slice of the given size well enough: their
/// buffers are at most half too small for it. Smaller ones are replaced.
bool readersFit(size_t readers_marks, size_t slice_marks)
{
    return slice_marks <= 2 * readers_marks;
}

/// Keys that are not known (empty blocks) go last.
int compareKeys(const Block & lhs, const Block & rhs)
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

bool MergeTreeReadPoolInOrderSliced::QueuedLaneLess::operator()(const QueuedLane & lhs, const QueuedLane & rhs) const
{
    const int result = compareKeys(lhs.key, rhs.key);
    return result != 0 ? result < 0 : lhs.lane < rhs.lane;
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
    , primary_key_header(primary_key_header_)
    , last_task_lane(num_sources_)
    , last_readers_marks(num_sources_)
    , pending(num_sources_)
{
    std::lock_guard lock(mutex);

    const size_t num_lanes = parts_ranges.size();
    lanes.reserve(num_lanes);
    boundaries.reserve(num_lanes);
    queue_position.resize(num_lanes);
    for (size_t lane = 0; lane < num_lanes; ++lane)
    {
        lanes.push_back(Lane{.unread = parts_ranges[lane].ranges});
        boundaries.push_back(lanes.back().unread.empty() ? Block{} : keyAtMark(lane, lanes.back().unread.front().begin));
        enqueueLane(lane);
    }
}

Block MergeTreeReadPoolInOrderSliced::keyAtMark(size_t lane, size_t mark) const
{
    if (primary_key_header.columns() == 0)
        return {};

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

size_t MergeTreeReadPoolInOrderSliced::nextSliceMarksUnlocked(size_t lane) const
{
    const auto & lane_state = lanes[lane];
    const size_t ramp_marks = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    return std::min({max_slice_marks, ramp_marks, lane_state.unread.getNumberOfMarks()});
}

size_t MergeTreeReadPoolInOrderSliced::nextSliceMarks(size_t lane) const
{
    std::lock_guard lock(mutex);
    return nextSliceMarksUnlocked(lane);
}

void MergeTreeReadPoolInOrderSliced::enqueueLane(size_t lane)
{
    const auto & unread = lanes[lane].unread;
    if (unread.empty())
        return;

    auto [it, inserted] = queue.insert(QueuedLane{.key = keyAtMark(lane, unread.front().begin), .lane = lane});
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} is already queued", lane);
    queue_position[lane] = it;
}

void MergeTreeReadPoolInOrderSliced::dequeueLane(size_t lane)
{
    if (auto & position = queue_position[lane])
    {
        queue.erase(*position);
        position.reset();
    }
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextLane() const
{
    std::lock_guard lock(mutex);
    if (queue.empty())
        return std::nullopt;
    return queue.begin()->lane;
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextLaneBefore(size_t lane) const
{
    std::lock_guard lock(mutex);
    const auto & position = queue_position[lane];
    if (!position || queue.empty())
        return std::nullopt;

    const auto & head = *queue.begin();
    if (head.lane == lane || compareKeys(head.key, (*position)->key) >= 0)
        return std::nullopt;
    return head.lane;
}

bool MergeTreeReadPoolInOrderSliced::laneHasUnreadMarks(size_t lane) const
{
    std::lock_guard lock(mutex);
    return !lanes[lane].unread.empty();
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::lastTaskLane(size_t source) const
{
    std::lock_guard lock(mutex);
    return last_task_lane[source];
}

bool MergeTreeReadPoolInOrderSliced::hasPendingSlice(size_t source) const
{
    std::lock_guard lock(mutex);
    return pending[source].has_value();
}

void MergeTreeReadPoolInOrderSliced::finishLane(size_t lane)
{
    std::lock_guard lock(mutex);
    dequeueLane(lane);
    lanes[lane].unread.clear();
    lanes[lane].parked_readers.clear();
}

void MergeTreeReadPoolInOrderSliced::finish()
{
    std::lock_guard lock(mutex);
    finished = true;
}

bool MergeTreeReadPoolInOrderSliced::isFinished() const
{
    std::lock_guard lock(mutex);
    return finished;
}

MergeTreeReadPoolInOrderSliced::SliceDescription MergeTreeReadPoolInOrderSliced::assignSlice(size_t source, size_t lane)
{
    std::lock_guard lock(mutex);

    if (finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot assign a slice to source {}: the pool is finished", source);
    if (pending[source])
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source {} already has a slice assigned", source);

    auto & lane_state = lanes[lane];
    if (lane_state.unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} has no marks left", lane);

    dequeueLane(lane);
    const size_t ramp_marks = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    MarkRanges ranges = cutMarks(lane_state.unread, std::min(max_slice_marks, ramp_marks));
    ++lane_state.slices_cut;
    enqueueLane(lane);

    SliceDescription description{
        .lane = lane,
        .first_mark = ranges.front().begin,
        .marks = ranges.getNumberOfMarks(),
        .rows = per_part_infos[lane]->data_part_info->getIndexGranularity().getRowsCountInRanges(ranges),
    };

    pending[source] = PendingSlice{.lane = lane, .ranges = std::move(ranges)};
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

        lane = slice->lane;
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
    /// Readers made for a much smaller slice are not reused: their buffers would read the slice in many
    /// small pieces. The size hints are taken before the readers may be given away.
    auto extras = getExtras();
    if (previous_task)
        extras.value_size_map = previous_task->getMainReader().getAvgValueSizeHints();

    const size_t slice_marks = ranges.getNumberOfMarks();
    MergeTreeReadTask::Readers readers;
    size_t readers_marks = 0;
    bool has_readers = false;
    {
        std::lock_guard lock(mutex);

        auto & last_lane = last_task_lane[task_idx];
        auto & last_marks = last_readers_marks[task_idx];
        if (previous_task && last_lane == lane && readersFit(last_marks, slice_marks))
        {
            readers = previous_task->releaseReaders();
            readers_marks = last_marks;
            has_readers = true;
        }
        else
        {
            if (previous_task && last_lane)
            {
                auto & previous = lanes[*last_lane];
                if (!previous.unread.empty() && previous.parked_readers.size() < max_parked_readers_per_lane
                    && readersFit(last_marks, nextSliceMarksUnlocked(*last_lane)))
                    previous.parked_readers.push_back(SizedReaders{.readers = previous_task->releaseReaders(), .marks = last_marks});
            }

            auto & parked = lanes[lane].parked_readers;
            auto fitting = std::find_if(parked.begin(), parked.end(), [&](const SizedReaders & set) { return readersFit(set.marks, slice_marks); });
            if (fitting != parked.end())
            {
                readers = std::move(fitting->readers);
                readers_marks = fitting->marks;
                has_readers = true;
                parked.erase(fitting);
            }
        }
        last_lane = lane;
        last_marks = has_readers ? readers_marks : slice_marks;
    }

    if (has_readers)
        readers.updateAllMarkRanges(ranges, patches_ranges);
    else
        readers = MergeTreeReadTask::createReaders(info, extras, ranges, patches_ranges);

    return createTask(info, std::move(readers), std::move(ranges), std::move(patches_ranges), updater);
}

}
