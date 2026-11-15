#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <utility>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 min_compress_block_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Takes up to max_marks marks from the front of the ranges, or from their back when reading in reverse
/// order; the result is in mark order either way.
MarkRanges cutMarks(MarkRanges & from, size_t max_marks, bool from_back)
{
    MarkRanges result;
    while (max_marks > 0 && !from.empty())
    {
        auto & range = from_back ? from.back() : from.front();
        const size_t marks = std::min(range.end - range.begin, max_marks);
        if (from_back)
        {
            result.emplace_front(range.end - marks, range.end);
            range.end -= marks;
        }
        else
        {
            result.emplace_back(range.begin, range.begin + marks);
            range.begin += marks;
        }
        max_marks -= marks;
        if (range.begin == range.end)
        {
            if (from_back)
                from.pop_back();
            else
                from.pop_front();
        }
    }
    return result;
}

/// Both inputs are in mark order and disjoint; the result is in mark order.
MarkRanges unionRanges(const MarkRanges & lhs, const MarkRanges & rhs)
{
    MarkRanges result;
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size() || j < rhs.size())
    {
        if (j == rhs.size() || (i < lhs.size() && lhs[i].begin < rhs[j].begin))
            result.push_back(lhs[i++]);
        else
            result.push_back(rhs[j++]);
    }
    return result;
}

/// Both inputs are in mark order; the result is in mark order.
MarkRanges intersectRanges(const MarkRanges & lhs, const MarkRanges & rhs)
{
    MarkRanges result;
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size() && j < rhs.size())
    {
        const size_t begin = std::max(lhs[i].begin, rhs[j].begin);
        const size_t end = std::min(lhs[i].end, rhs[j].end);
        if (begin < end)
            result.emplace_back(begin, end);
        if (lhs[i].end < rhs[j].end)
            ++i;
        else
            ++j;
    }
    return result;
}

/// Reader sets kept per lane for sources that come back to it. Every set holds read buffers for all
/// columns, so only a few are kept.
constexpr size_t max_parked_readers_per_lane = 2;

}

int MergeTreeReadPoolInOrderSliced::compareKeys(const Block & lhs, const Block & rhs, const std::vector<bool> & reverse_flags, bool reverse)
{
    if (lhs.columns() == 0 || rhs.columns() == 0)
        return static_cast<int>(lhs.columns() == 0) - static_cast<int>(rhs.columns() == 0);

    for (size_t i = 0; i < lhs.columns(); ++i)
    {
        int result = lhs.getByPosition(i).column->compareAt(0, 0, *rhs.getByPosition(i).column, 1);
        if (i < reverse_flags.size() && reverse_flags[i])
            result = -result;
        if (result != 0)
            return reverse ? -result : result;
    }
    return 0;
}

bool MergeTreeReadPoolInOrderSliced::QueuedLaneLess::operator()(const QueuedLane & lhs, const QueuedLane & rhs) const
{
    const int result = compareKeys(lhs.key, rhs.key, *reverse_flags, reverse);
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
    const Block & primary_key_header_,
    std::vector<bool> sorting_key_reverse_flags_,
    bool read_in_reverse_order_)
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
    , num_lanes(parts_ranges.size())
    , max_slice_marks(std::max<size_t>(1, pool_settings.min_marks_for_concurrent_read))
    , min_slice_bytes_to_share(context_->getSettingsRef()[Setting::min_compress_block_size])
    , primary_key_header(primary_key_header_)
    , sorting_key_reverse_flags(std::move(sorting_key_reverse_flags_))
    , reverse(read_in_reverse_order_)
    , queue(QueuedLaneLess{.reverse_flags = &sorting_key_reverse_flags, .reverse = read_in_reverse_order_})
    , sources(num_sources_)
{
    std::lock_guard lock(mutex);

    lanes.reserve(num_lanes);
    queue_position.resize(num_lanes);
    for (size_t lane = 0; lane < num_lanes; ++lane)
    {
        lanes.push_back(Lane{.unread = parts_ranges[lane].ranges});
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

size_t MergeTreeReadPoolInOrderSliced::nextSliceMarks(size_t lane) const
{
    const auto & lane_state = lanes[lane];
    const size_t ramp = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    return std::min({max_slice_marks, ramp, lane_state.unread.getNumberOfMarks()});
}

bool MergeTreeReadPoolInOrderSliced::canShare(size_t lane, const Demand & demand) const
{
    if (!demand.lane_in_flight[lane] || !is_part_on_remote_disk[lane] || read_marks == 0)
        return true;
    /// On remote storage every slice read by another source is a request. A slice that reads less than
    /// a compressed block is not worth one: the source reading the lane gets it for nothing from the
    /// block it has already fetched.
    return read_bytes / read_marks * nextSliceMarks(lane) >= min_slice_bytes_to_share;
}

void MergeTreeReadPoolInOrderSliced::enqueueLane(size_t lane)
{
    const auto & unread = lanes[lane].unread;
    if (unread.empty())
        return;

    const size_t next_mark = reverse ? unread.back().end : unread.front().begin;
    auto [it, inserted] = queue.insert(QueuedLane{.key = keyAtMark(lane, next_mark), .lane = lane});
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

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextLaneBefore(size_t lane) const
{
    const auto & position = queue_position[lane];
    if (!position || queue.empty())
        return std::nullopt;

    const auto & head = *queue.begin();
    if (head.lane == lane || compareKeys(head.key, (*position)->key) >= 0)
        return std::nullopt;
    return head.lane;
}

bool MergeTreeReadPoolInOrderSliced::inFlightAfter(LaneQueue::const_iterator position, const Demand & demand) const
{
    for (auto it = std::next(position); it != queue.end(); ++it)
        if (demand.lane_in_flight[it->lane])
            return true;
    return false;
}

MergeTreeReadPoolInOrderSliced::Cut MergeTreeReadPoolInOrderSliced::cutSlice(size_t lane)
{
    auto & lane_state = lanes[lane];
    if (lane_state.unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} has no marks left", lane);

    dequeueLane(lane);
    MarkRanges ranges = cutMarks(lane_state.unread, nextSliceMarks(lane), reverse);
    enqueueLane(lane);

    Cut cut{
        .slice = MergeTreeSliceTag{.lane = lane, .seq = lane_state.slices_cut},
        .marks = ranges.getNumberOfMarks(),
        .boundary_mark = reverse ? ranges.back().end : ranges.front().begin};
    ++lane_state.slices_cut;
    fifo.push_back(QueuedSlice{.slice = cut.slice, .ranges = std::move(ranges)});
    return cut;
}

void MergeTreeReadPoolInOrderSliced::finishLaneUnlocked(size_t lane)
{
    auto & lane_state = lanes[lane];
    if (lane_state.finished)
        return;

    lane_state.finished = true;
    std::erase_if(fifo, [lane](const QueuedSlice & queued) { return queued.slice.lane == lane; });
    dequeueLane(lane);
    lane_state.unread.clear();
    lane_state.parked_readers.clear();
}

std::optional<MergeTreeReadPoolInOrderSliced::QueuedSlice>
MergeTreeReadPoolInOrderSliced::takeSlice(size_t task_idx, std::optional<MarkRanges> & lane_to_refine)
{
    std::unique_lock lock(mutex);
    while (true)
    {
        if (fifo.empty())
            return std::nullopt;

        /// A slice of the lane the source read last continues with the readers the source holds.
        const auto & source = sources[task_idx];
        auto pick = fifo.end();
        for (auto it = fifo.begin(); it != fifo.end(); ++it)
        {
            if (lanes[it->slice.lane].refining)
                continue;
            if (pick == fifo.end())
                pick = it;
            if (source.lane == it->slice.lane)
            {
                pick = it;
                break;
            }
        }

        if (pick == fifo.end())
        {
            /// Every queued slice belongs to a lane being refined: their ranges are about to change.
            refined_cv.wait(lock);
            continue;
        }

        QueuedSlice taken = std::move(*pick);
        fifo.erase(pick);

        auto & lane_state = lanes[taken.slice.lane];
        /// The router does not wait for the slices of a lane it finished.
        if (lane_state.finished)
            continue;

        /// The first slice of a lane refines the whole lane: the index result is built once per part
        /// anyway, and the slices cut afterwards skip the granules the index drops instead of being
        /// read to find nothing. Every mark of the lane not yet handed to a reader is refined, once:
        /// the slices of the lane still in the FIFO wait for the result (see above), so that no mark
        /// is accounted by the refiner twice.
        if (ranges_refiner && !lane_state.refined)
        {
            lane_state.refining = true;
            lane_to_refine = unionRanges(taken.ranges, lane_state.unread);
            for (const auto & queued : fifo)
                if (queued.slice.lane == taken.slice.lane)
                    lane_to_refine = unionRanges(*lane_to_refine, queued.ranges);
        }
        return taken;
    }
}

MergeTreeReadTaskPtr MergeTreeReadPoolInOrderSliced::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    MergeTreeReadTaskInfoPtr info;
    MarkRanges ranges;
    MergeTreeSliceTag slice;

    {
        std::lock_guard lock(mutex);
        auto & source = sources[task_idx];

        /// The task comes back read: what it cost is the evidence for canShare. A source that found
        /// nothing hands the same task back again when it is woken up.
        if (previous_task && source.reading && previous_task->getSlice() == source.reading)
        {
            source.reading.reset();
            read_bytes += previous_task->getNumReadBytes();
            read_marks += source.reading_marks;
        }
    }

    while (true)
    {
        std::optional<MarkRanges> lane_to_refine;
        auto taken = takeSlice(task_idx, lane_to_refine);
        if (!taken)
            return nullptr;

        slice = taken->slice;
        ranges = std::move(taken->ranges);
        info = per_part_infos[slice.lane];
        if (!lane_to_refine)
            break;

        /// The refinement may block, so it runs outside of the mutex.
        MarkRanges refined;
        try
        {
            refined = refineReadRanges(*info, std::move(*lane_to_refine));
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            lanes[slice.lane].refining = false;
            refined_cv.notify_all();
            throw;
        }

        std::lock_guard lock(mutex);
        auto & lane_state = lanes[slice.lane];
        lane_state.refining = false;
        lane_state.refined = true;
        refined_cv.notify_all();

        if (!lane_state.finished)
        {
            dequeueLane(slice.lane);
            lane_state.unread = intersectRanges(lane_state.unread, refined);
            enqueueLane(slice.lane);

            /// A queued slice the index leaves nothing of is not read: the router drops it.
            for (auto it = fifo.begin(); it != fifo.end();)
            {
                if (it->slice.lane != slice.lane)
                {
                    ++it;
                    continue;
                }
                const size_t marks_before = it->ranges.getNumberOfMarks();
                it->ranges = intersectRanges(it->ranges, refined);
                if (it->ranges.getNumberOfMarks() != marks_before)
                    refined_slices.push_back(SliceRefinement{.slice = it->slice, .marks = it->ranges.getNumberOfMarks()});
                if (it->ranges.empty())
                    it = fifo.erase(it);
                else
                    ++it;
            }
        }

        const size_t marks_before = ranges.getNumberOfMarks();
        ranges = intersectRanges(ranges, refined);
        if (ranges.getNumberOfMarks() != marks_before)
            refined_slices.push_back(SliceRefinement{.slice = slice, .marks = ranges.getNumberOfMarks()});
        if (!ranges.empty())
            break;
        /// Nothing left of this slice either: the source takes the next one.
    }

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
        auto & source = sources[task_idx];

        if (previous_task && source.lane == slice.lane)
        {
            readers = previous_task->releaseReaders();
            has_readers = true;
        }
        else
        {
            if (previous_task && source.lane)
            {
                auto & previous = lanes[*source.lane];
                if (!previous.unread.empty() && previous.parked_readers.size() < max_parked_readers_per_lane)
                    previous.parked_readers.push_back(previous_task->releaseReaders());
            }

            auto & parked = lanes[slice.lane].parked_readers;
            if (!parked.empty())
            {
                readers = std::move(parked.back());
                parked.pop_back();
                has_readers = true;
            }
        }

        source.lane = slice.lane;
        source.reading = slice;
        source.reading_marks = ranges.getNumberOfMarks();
    }

    if (has_readers)
        readers.updateAllMarkRanges(ranges, patches_ranges);
    else
        readers = MergeTreeReadTask::createReaders(info, extras, ranges, patches_ranges);

    auto task = createTask(info, std::move(readers), std::move(ranges), std::move(patches_ranges), updater);
    task->setSlice(slice);
    return task;
}

bool MergeTreeReadPoolInOrderSliced::mayHaveMoreTasks() const
{
    std::lock_guard lock(mutex);
    return !finished;
}

MergeTreeReadPoolInOrderSliced::Scheduled
MergeTreeReadPoolInOrderSliced::schedule(const Demand & demand, const std::vector<size_t> & parked_sources)
{
    std::lock_guard lock(mutex);

    Scheduled result;
    size_t marks_cut = 0;

    /// Slices are cut for the sources that can take them now; a source still reading takes the next one
    /// itself when it is done.
    auto can_cut = [&]() TSA_REQUIRES(mutex) { return fifo.size() < parked_sources.size(); };
    auto cut = [&](size_t lane) TSA_REQUIRES(mutex)
    {
        result.cut.push_back(cutSlice(lane));
        marks_cut += result.cut.back().marks;
    };

    /// The lanes the merge is blocked on are read whatever the read-ahead: those rows are never waste.
    for (size_t lane : demand.blocked_lanes)
    {
        if (lanes[lane].unread.empty())
            continue;
        if (!can_cut())
            break;
        cut(lane);

        /// Lanes whose next key lies within the slice just cut are consumed before that slice is done:
        /// reading them now costs no more rows than waiting for the merge to ask for each of them in turn.
        while (auto before = nextLaneBefore(lane))
        {
            if (!can_cut() || !canShare(*before, demand))
                break;
            cut(*before);
        }
    }

    /// Read ahead in the order the merge is going to need the data, never past the allowance. A lane that
    /// is being read and is too short to share is left to its reader; the next lane in key order is read
    /// ahead instead. A lane with nothing in flight that lies after everything in flight is left alone
    /// until the query spans lanes: the lane the merge is on gets its next slice when the merge asks for
    /// it, so a LIMIT met by that slice reads nothing else, and lanes the merge has not reached wait.
    /// Lanes whose key lies before data in flight are read: the merge needs them before that data.
    for (auto it = queue.begin(); it != queue.end() && can_cut();)
    {
        const size_t lane = it->lane;
        if (!canShare(lane, demand))
        {
            ++it;
            continue;
        }
        if (!demand.spans_lanes && !demand.lane_in_flight[lane] && !inFlightAfter(it, demand))
            break;
        if (marks_cut + nextSliceMarks(lane) > demand.read_ahead_marks)
            break;
        cut(lane);
        it = queue.begin();
    }

    std::vector<size_t> rest = parked_sources;
    for (const auto & queued : fifo)
    {
        if (rest.empty())
            break;
        auto pick = rest.begin();
        for (auto it = rest.begin(); it != rest.end(); ++it)
            if (sources[*it].lane == queued.slice.lane)
                pick = it;
        result.sources_to_wake.push_back(*pick);
        rest.erase(pick);
    }
    return result;
}

size_t MergeTreeReadPoolInOrderSliced::fifoSize() const
{
    std::lock_guard lock(mutex);
    return fifo.size();
}

std::vector<MergeTreeReadPoolInOrderSliced::SliceRefinement> MergeTreeReadPoolInOrderSliced::takeRefinedSlices()
{
    std::lock_guard lock(mutex);
    return std::exchange(refined_slices, {});
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextUnreadMark(size_t lane) const
{
    std::lock_guard lock(mutex);
    const auto & unread = lanes[lane].unread;
    if (unread.empty())
        return std::nullopt;
    return reverse ? unread.back().end : unread.front().begin;
}

void MergeTreeReadPoolInOrderSliced::finishLane(size_t lane)
{
    std::lock_guard lock(mutex);
    finishLaneUnlocked(lane);
}

void MergeTreeReadPoolInOrderSliced::finish()
{
    std::lock_guard lock(mutex);
    finished = true;
}

}
