#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
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

int MergeTreeReadPoolInOrderSliced::compareKeys(const Block & lhs, const Block & rhs, bool reverse)
{
    if (lhs.columns() == 0 || rhs.columns() == 0)
        return static_cast<int>(lhs.columns() == 0) - static_cast<int>(rhs.columns() == 0);

    for (size_t i = 0; i < lhs.columns(); ++i)
    {
        int result = lhs.getByPosition(i).column->compareAt(0, 0, *rhs.getByPosition(i).column, 1);
        if (result != 0)
            return reverse ? -result : result;
    }
    return 0;
}

bool MergeTreeReadPoolInOrderSliced::QueuedLaneLess::operator()(const QueuedLane & lhs, const QueuedLane & rhs) const
{
    const int result = compareKeys(lhs.key, rhs.key, reverse);
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
    ExpressionActionsPtr virtual_row_conversions_,
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
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , reverse(read_in_reverse_order_)
    , queue(QueuedLaneLess{.reverse = read_in_reverse_order_})
    , last_task_lane(num_sources_)
{
    std::lock_guard lock(mutex);

    lanes.reserve(num_lanes);
    queue_position.resize(num_lanes);
    for (size_t lane = 0; lane < num_lanes; ++lane)
    {
        lanes.push_back(Lane{.unread = parts_ranges[lane].ranges});
        enqueueLane(lane);

        const size_t part_index = per_part_infos[lane]->part_index_in_query;
        if (!lane_by_part_index.emplace(part_index, lane).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part index {} is read by two lanes of the sliced pool", part_index);
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

size_t MergeTreeReadPoolInOrderSliced::laneOf(const MergeTreeSliceTag & tag) const
{
    auto it = lane_by_part_index.find(tag.part_index_in_query);
    if (it == lane_by_part_index.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part index {} is not read by the sliced pool", tag.part_index_in_query);
    return it->second;
}

size_t MergeTreeReadPoolInOrderSliced::nextSliceMarks(size_t lane) const
{
    const auto & lane_state = lanes[lane];
    const size_t ramp = size_t(1) << std::min<size_t>(lane_state.slices_cut, 16);
    return std::min({max_slice_marks, ramp, lane_state.unread.getNumberOfMarks()});
}

bool MergeTreeReadPoolInOrderSliced::canShare(size_t lane) const
{
    const auto & lane_state = lanes[lane];
    if (lane_state.slices.empty() || !is_part_on_remote_disk[lane] || read_marks == 0)
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

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextLane() const
{
    if (queue.empty())
        return std::nullopt;
    return queue.begin()->lane;
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextLaneBefore(size_t lane) const
{
    const auto & position = queue_position[lane];
    if (!position || queue.empty())
        return std::nullopt;

    const auto & head = *queue.begin();
    if (head.lane == lane || compareKeys(head.key, (*position)->key, reverse) >= 0)
        return std::nullopt;
    return head.lane;
}

bool MergeTreeReadPoolInOrderSliced::inFlightAfter(LaneQueue::const_iterator position) const
{
    for (auto it = std::next(position); it != queue.end(); ++it)
        if (!lanes[it->lane].slices.empty())
            return true;
    return false;
}

std::optional<size_t> MergeTreeReadPoolInOrderSliced::nextUnreadMark(const Lane & lane) const
{
    if (lane.unread.empty())
        return std::nullopt;
    return reverse ? lane.unread.back().end : lane.unread.front().begin;
}

MergeTreeReadPoolInOrderSliced::Slices::iterator MergeTreeReadPoolInOrderSliced::headSlice(Lane & lane) const
{
    if (lane.slices.empty())
        return lane.slices.end();
    return reverse ? std::prev(lane.slices.end()) : lane.slices.begin();
}

void MergeTreeReadPoolInOrderSliced::cutSlice(size_t lane)
{
    auto & lane_state = lanes[lane];
    if (lane_state.unread.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lane {} has no marks left", lane);

    dequeueLane(lane);
    MarkRanges ranges = cutMarks(lane_state.unread, nextSliceMarks(lane), reverse);
    ++lane_state.slices_cut;
    enqueueLane(lane);

    const size_t first_mark = ranges.front().begin;
    const size_t boundary_mark = reverse ? ranges.back().end : first_mark;
    const size_t marks = ranges.getNumberOfMarks();
    Slice slice{.ranges = std::move(ranges), .marks = marks, .boundary_mark = boundary_mark};

    issued_marks += marks;
    if (!lane_state.slices.try_emplace(first_mark, std::move(slice)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} was cut twice", first_mark, lane);
    fifo.push_back(QueuedSlice{.lane = lane, .first_mark = first_mark});
}

void MergeTreeReadPoolInOrderSliced::completeSlice(const MergeTreeSliceTag & tag, size_t bytes_read)
{
    const size_t lane = laneOf(tag);
    auto & lane_state = lanes[lane];
    if (lane_state.finished)
        return;

    auto it = lane_state.slices.find(tag.first_mark);
    if (it == lane_state.slices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of part {} is not issued", tag.first_mark, tag.part_index_in_query);

    auto & slice = it->second;
    slice.complete = true;
    read_bytes += bytes_read;
    read_marks += slice.marks;
    if (slice.chunks.empty())
        dropSlice(lane, it);
}

void MergeTreeReadPoolInOrderSliced::dropSlice(size_t lane, Slices::iterator slice)
{
    issued_marks -= slice->second.marks;
    if (slice->second.rows_received == 0)
        empty_marks += slice->second.marks;
    else
        lanes[lane].taken_marks += slice->second.marks;
    lanes[lane].slices.erase(slice);
}

void MergeTreeReadPoolInOrderSliced::finishLaneUnlocked(size_t lane)
{
    auto & lane_state = lanes[lane];
    if (lane_state.finished)
        return;

    lane_state.finished = true;
    for (const auto & [first_mark, slice] : lane_state.slices)
        issued_marks -= slice.marks;
    lane_state.slices.clear();
    std::erase_if(fifo, [lane](const QueuedSlice & queued) { return queued.lane == lane; });
    dequeueLane(lane);
    lane_state.unread.clear();
    lane_state.parked_readers.clear();
}

Chunk MergeTreeReadPoolInOrderSliced::announce(size_t lane, size_t mark, const Block & output_header)
{
    if (!virtual_row_conversions)
        return {};

    auto & lane_state = lanes[lane];
    Block key = keyAtMark(lane, mark);
    if (key.columns() == 0 || compareKeys(key, lane_state.announced_key) == 0)
        return {};

    Chunk chunk(output_header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(/*part_level=*/ 0, key, virtual_row_conversions));
    lane_state.announced_key = std::move(key);
    return chunk;
}

size_t MergeTreeReadPoolInOrderSliced::readAheadMarks() const
{
    /// Nothing before the merge has gone through a slice and asked for more: a query answered by its
    /// first slices reads nothing else. From there the depth follows the evidence. A slice the merge went
    /// through buys four times its marks: the depth follows the merge's progress, which a LIMIT may end
    /// any moment. A slice that came back without any rows buys sixteen times its marks: the query is
    /// scanning for rows that are far away or not there, and on object storage every round of slices is
    /// a round trip, so the rest of a lane's ramp is read in one round and every source is reading after
    /// a few. At most a full slice per source.
    return std::min(num_sources * max_slice_marks, 4 * consumed_marks + 16 * empty_marks);
}

MergeTreeReadTaskPtr MergeTreeReadPoolInOrderSliced::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    while (true)
    {
        MergeTreeReadTaskInfoPtr info;
        MarkRanges ranges;
        size_t lane = 0;
        size_t first_mark = 0;
        std::optional<MarkRanges> lane_to_refine;

        {
            std::lock_guard lock(mutex);
            if (fifo.empty())
                return nullptr;

            /// A slice of the lane the source read last continues with the readers the source holds.
            auto queued = fifo.begin();
            if (const auto & last_lane = last_task_lane[task_idx])
            {
                auto own = std::find_if(fifo.begin(), fifo.end(), [&](const QueuedSlice & slice) { return slice.lane == *last_lane; });
                if (own != fifo.end())
                    queued = own;
            }
            lane = queued->lane;
            first_mark = queued->first_mark;
            fifo.erase(queued);

            auto & lane_state = lanes[lane];
            ranges = std::move(lane_state.slices.at(first_mark).ranges);
            info = per_part_infos[lane];
            if (ranges_refiner && !lane_state.refined)
            {
                /// Everything the lane still has to read: its unread marks and the slices cut from them
                /// that no source has taken yet. A slice left out here would be emptied below.
                lane_to_refine = lane_state.unread;
                for (const auto & [_, other] : lane_state.slices)
                    *lane_to_refine = unionRanges(*lane_to_refine, other.ranges);
            }
        }

        if (lane_to_refine)
        {
            /// The first slice of a lane refines the whole lane: the index result is built once per part
            /// anyway, and the slices cut afterwards skip the granules the index drops instead of being
            /// read to find nothing. May block, so it runs outside of the mutex.
            MarkRanges refined = refineReadRanges(*info, unionRanges(ranges, *lane_to_refine));

            std::lock_guard lock(mutex);
            auto & lane_state = lanes[lane];
            if (!lane_state.refined && !lane_state.finished)
            {
                lane_state.refined = true;
                dequeueLane(lane);
                lane_state.unread = intersectRanges(lane_state.unread, refined);
                enqueueLane(lane);
                /// Slices still waiting in the FIFO hold their ranges; taken ones hold none.
                for (auto & [_, other] : lane_state.slices)
                    other.ranges = intersectRanges(other.ranges, refined);
            }
            ranges = intersectRanges(ranges, refined);
        }

        {
            std::lock_guard lock(mutex);
            auto & lane_state = lanes[lane];
            if (lane_state.finished)
                continue;

            /// Nothing left to read: the slice is done without a task. The source's next marker runs the
            /// router, which then serves the lane.
            if (ranges.empty())
            {
                dropSlice(lane, lane_state.slices.find(first_mark));
                continue;
            }

            /// Refinement may have dropped the first marks: the slice is identified by the first mark it
            /// actually reads, and announced at its boundary.
            const size_t read_first_mark = ranges.front().begin;
            if (read_first_mark != first_mark)
            {
                auto node = lane_state.slices.extract(first_mark);
                node.key() = read_first_mark;
                lane_state.slices.insert(std::move(node));
                first_mark = read_first_mark;
            }
            lane_state.slices.at(first_mark).boundary_mark = reverse ? ranges.back().end : first_mark;
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

        if (has_readers)
            readers.updateAllMarkRanges(ranges, patches_ranges);
        else
            readers = MergeTreeReadTask::createReaders(info, extras, ranges, patches_ranges);

        return createTask(info, std::move(readers), std::move(ranges), std::move(patches_ranges), updater);
    }
}

bool MergeTreeReadPoolInOrderSliced::mayHaveMoreTasks() const
{
    std::lock_guard lock(mutex);
    return !finished;
}

void MergeTreeReadPoolInOrderSliced::receive(Chunk chunk)
{
    auto info = chunk.getChunkInfos().extract<MergeTreeSliceInfo>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got a chunk without slice info from a source of the sliced pool");

    std::lock_guard lock(mutex);

    if (info->ended)
        completeSlice(*info->ended, info->ended_bytes);

    if (!info->slice)
        return;

    /// The merge finished the lane while the slice was being read: nothing waits for its rows.
    auto & lane_state = lanes[laneOf(*info->slice)];
    if (lane_state.finished)
        return;

    auto it = lane_state.slices.find(info->slice->first_mark);
    if (it == lane_state.slices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of part {} is not issued", info->slice->first_mark, info->slice->part_index_in_query);

    auto & slice = it->second;
    slice.rows_received += chunk.getNumRows();
    /// A chunk without rows is progress, not data, unless it is a virtual row of the source.
    if (chunk.getNumRows() == 0 && !isVirtualRow(chunk))
        return;
    slice.chunks.push_back(std::move(chunk));
}

MergeTreeReadPoolInOrderSliced::Served MergeTreeReadPoolInOrderSliced::serve(size_t lane, const Block & output_header)
{
    std::lock_guard lock(mutex);

    auto & lane_state = lanes[lane];
    const bool was_waiting = lane_state.wants_data;
    lane_state.wants_data = false;
    if (lane_state.finished)
        return Served{.finished = true};

    /// The merge asks for the lane: whatever it took in full before this, it went through and wants more.
    /// A merge that stops after a chunk (a LIMIT) never comes back, and its slices never count.
    consumed_marks += std::exchange(lane_state.taken_marks, 0);

    /// Rows leave a lane through its first issued slice only, so the lane stays in reading order.
    auto head = headSlice(lane_state);
    if (head != lane_state.slices.end() && !head->second.chunks.empty())
    {
        Chunk chunk = std::move(head->second.chunks.front());
        head->second.chunks.pop_front();
        if (head->second.complete && head->second.chunks.empty())
            dropSlice(lane, head);
        merge_asked |= !was_waiting;
        return Served{.chunk = std::move(chunk)};
    }

    /// Nothing is ready: the rows of the slice in flight start at its boundary mark, else the lane's rows
    /// start at its next unread mark.
    std::optional<size_t> next_mark;
    if (head != lane_state.slices.end())
        next_mark = head->second.boundary_mark;
    else
        next_mark = nextUnreadMark(lane_state);

    if (!next_mark)
    {
        query_spans_lanes = true;
        finishLaneUnlocked(lane);
        return Served{.finished = true};
    }

    Chunk announcement = announce(lane, *next_mark, output_header);
    if (!announcement)
        lane_state.wants_data = true;
    merge_asked |= !was_waiting;
    return Served{.chunk = std::move(announcement)};
}

std::vector<size_t> MergeTreeReadPoolInOrderSliced::schedule(const std::vector<size_t> & parked_sources)
{
    std::lock_guard lock(mutex);

    const bool asked = std::exchange(merge_asked, false);

    /// Slices are cut for the sources that can take them now; a source still reading takes the next one
    /// itself when it is done.
    auto can_cut = [&]() TSA_REQUIRES(mutex) { return fifo.size() < parked_sources.size(); };

    /// The parked sources to wake, one per slice in the FIFO: the one holding the lane's readers if parked.
    auto to_wake = [&]() TSA_REQUIRES(mutex)
    {
        std::vector<size_t> chosen;
        std::vector<size_t> rest = parked_sources;
        for (const auto & queued : fifo)
        {
            if (rest.empty())
                break;
            auto pick = rest.begin();
            for (auto it = rest.begin(); it != rest.end(); ++it)
                if (last_task_lane[*it] == queued.lane)
                    pick = it;
            chosen.push_back(*pick);
            rest.erase(pick);
        }
        return chosen;
    };

    /// The lane the merge is blocked on is read whatever the read-ahead depth: those rows are never waste.
    bool merge_waits = false;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
    {
        if (!lanes[lane].wants_data)
            continue;

        merge_waits = true;
        if (!lanes[lane].slices.empty() || lanes[lane].unread.empty())
            continue;

        if (!can_cut())
            return to_wake();
        cutSlice(lane);

        /// Lanes whose next key lies within the slice just cut are consumed before that slice is done:
        /// reading them now costs no more rows than waiting for the merge to ask for each of them in turn.
        while (auto before = nextLaneBefore(lane))
        {
            if (!can_cut() || !canShare(*before))
                break;
            cutSlice(*before);
        }
    }

    /// Read-ahead only on the merge's demand: a merge that stops after the chunk it just took (a LIMIT)
    /// must not trigger reads it never needs.
    if (asked || merge_waits)
    {
        /// Read ahead in the order the merge is going to need the data, never past the budget. A lane that
        /// is being read and is too short to share is left to its reader; the next lane in key order is
        /// read ahead instead. Lanes the merge has not started and that lie after everything in flight are
        /// left alone until it went through a lane to its end: a query that ends within the lanes it is on,
        /// as a LIMIT met in the first part does, reads nothing else; one that spans lanes reads them all.
        const size_t budget = readAheadMarks();
        for (auto it = queue.begin(); it != queue.end() && can_cut();)
        {
            const size_t lane = it->lane;
            if (!canShare(lane))
            {
                ++it;
                continue;
            }
            if (!query_spans_lanes && lanes[lane].slices.empty() && !inFlightAfter(it))
                break;
            if (issued_marks + nextSliceMarks(lane) > budget)
                break;
            cutSlice(lane);
            it = queue.begin();
        }
    }

    return to_wake();
}

size_t MergeTreeReadPoolInOrderSliced::fifoSize() const
{
    std::lock_guard lock(mutex);
    return fifo.size();
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
