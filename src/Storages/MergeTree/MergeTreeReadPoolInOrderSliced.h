#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeSliceInfo.h>

#include <deque>
#include <mutex>
#include <optional>
#include <set>

namespace DB
{

/// Read pool for reading in the order of the primary key with more parallelism than one thread per part.
///
/// Every part is a lane. Slices are cut from the front of the lane's unread marks (the back in reverse
/// order), and a slice is one MergeTreeReadTask. Lanes with unread marks are queued by the primary key at
/// their next unread mark, so the head of the queue is the slice the merge needs next, whichever part it
/// belongs to. The slices of a lane start with one granule and double up to `min_marks_for_concurrent_read`:
/// the first rows of a part arrive after one granule, and a lane the merge stays on is read in large
/// slices.
///
/// The pool knows marks and keys, not rows. What the merge does with the rows is the business of
/// MergeTreeInOrderSliceRouter, which sees the merge's demand and tells the pool what to cut (`Demand`):
/// the lanes the merge is blocked on, and how many marks to read ahead of it. `schedule` cuts the slices
/// into a FIFO in the order the merge is going to need them, returns them to the router, which puts the
/// slices of a lane back in mark order when their rows come, and names the parked sources to wake for
/// them. Sources take the slices in `getTask`, read them, and tag every chunk with the slice
/// (MergeTreeSliceInfo). A source hands the task it read back with its next `getTask`: that is how the
/// pool learns that a slice is read and what it cost.
///
/// Several sources read one lane at a time when its slices carry enough data to be worth a request each:
/// that is the parallelism within a part the pool exists for. How much a slice carries is what the
/// slices read so far cost: a filter may drop every row at its first, cheap column and never read the
/// rest, or let every row through. Slices below one compressed block are read by one source, slice
/// after slice, through the readers it holds: readers follow the lane, not the source, `getTask` prefers
/// a queued slice of the lane the source read last, and `schedule` wakes the parked source that holds a
/// lane's readers. Readers are created for the marks of one slice, like the readers of the other pools
/// are created for one task, so their buffers are sized by the slice.
class MergeTreeReadPoolInOrderSliced : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolInOrderSliced(
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
        bool read_in_reverse_order_);

    String getName() const override { return "ReadPoolInOrderSliced"; }
    bool preservesOrderOfRanges() const override { return true; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    size_t numSources() const { return num_sources; }
    size_t numLanes() const { return num_lanes; }
    size_t maxSliceMarks() const { return max_slice_marks; }

    /// Source side.

    /// The next slice from the FIFO, preferring one of the lane the source read last. Nothing if the FIFO
    /// is empty: the source then reports idle and is parked until the router has a slice for it.
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    bool mayHaveMoreTasks() const override;

    /// Router side.

    /// What the merge needs, as the router sees it.
    struct Demand
    {
        /// Lanes the merge waits for with no slice cut for them. Each gets a slice whatever the read-ahead.
        std::vector<size_t> blocked_lanes;
        /// Per lane, whether slices cut for it are still in flight: cut and not yet consumed by the merge.
        std::vector<bool> lane_in_flight;
        /// Marks to cut beyond that, in the order the merge is going to need them.
        size_t read_ahead_marks = 0;
        /// The merge went through a lane to its end, so the query is not answered within the lanes it
        /// started on: lanes it has not started are read ahead too.
        bool spans_lanes = false;
    };

    /// A slice cut into the FIFO.
    struct Cut
    {
        MergeTreeSliceTag slice;
        size_t marks = 0;
        /// The mark the rows of the slice start at in reading order.
        size_t boundary_mark = 0;
    };

    struct Scheduled
    {
        std::vector<Cut> cut;
        /// Parked sources to wake, one per slice waiting in the FIFO: the one that read the slice's lane
        /// last if it is among them, since it holds the lane's readers.
        std::vector<size_t> sources_to_wake;
    };

    /// Cuts slices for the demand (see the rules in the .cpp), at most as many as there are parked
    /// sources to take them.
    Scheduled schedule(const Demand & demand, const std::vector<size_t> & parked_sources);
    size_t fifoSize() const;

    /// The mark the lane's rows start at once every slice cut so far is read; nothing when the lane is
    /// cut to its end.
    std::optional<size_t> nextUnreadMark(size_t lane) const;
    /// Primary key values at the mark of the lane, one row; empty if the index has no value there.
    Block keyAtMark(size_t lane, size_t mark) const;
    /// Keys that are not known (empty blocks) go last: the queue is the read-ahead order, and the pool does not
    /// speculate on a lane it cannot place. The merge's demand for such a lane is served regardless.
    static int compareKeys(const Block & lhs, const Block & rhs, bool reverse = false);

    /// The lane is not going to be read anymore: its unread marks, queued slices and parked readers go away.
    void finishLane(size_t lane);
    /// No slice is going to be cut anymore; a source that finds no task ends its stream.
    void finish();

private:
    struct Lane
    {
        MarkRanges unread;
        /// Slices cut so far: the number of the next one, and the ramp.
        size_t slices_cut = 0;
        /// The ranges refiner was applied to the whole lane.
        bool refined = false;
        /// Readers of sources that moved on to other lanes.
        std::vector<MergeTreeReadTask::Readers> parked_readers = {};
        bool finished = false;
    };

    /// A lane with unread marks, ordered by the primary key at its next unread mark in reading order.
    struct QueuedLane
    {
        Block key;
        size_t lane = 0;
    };

    struct QueuedLaneLess
    {
        bool reverse;
        bool operator()(const QueuedLane & lhs, const QueuedLane & rhs) const;
    };

    using LaneQueue = std::set<QueuedLane, QueuedLaneLess>;

    /// A slice cut and not yet taken by a source.
    struct QueuedSlice
    {
        MergeTreeSliceTag slice;
        MarkRanges ranges;
    };

    struct Source
    {
        /// The lane of the last task the source got, i.e. the lane its current readers belong to.
        std::optional<size_t> lane;
        /// The slice the source is reading, until it hands the task back with its next getTask, and its
        /// marks as read: what a slice costs, for canShare.
        std::optional<MergeTreeSliceTag> reading;
        size_t reading_marks = 0;
    };

    size_t nextSliceMarks(size_t lane) const TSA_REQUIRES(mutex);
    /// Whether the lane may get a slice while it has slices in flight: the slice must be worth a request
    /// of its own.
    bool canShare(size_t lane, const Demand & demand) const TSA_REQUIRES(mutex);
    void enqueueLane(size_t lane) TSA_REQUIRES(mutex);
    void dequeueLane(size_t lane) TSA_REQUIRES(mutex);
    std::optional<size_t> nextLaneBefore(size_t lane) const TSA_REQUIRES(mutex);
    /// A lane with slices in flight lies after the position in key order.
    bool inFlightAfter(LaneQueue::const_iterator position, const Demand & demand) const TSA_REQUIRES(mutex);
    Cut cutSlice(size_t lane) TSA_REQUIRES(mutex);
    void finishLaneUnlocked(size_t lane) TSA_REQUIRES(mutex);

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t num_lanes;
    const size_t max_slice_marks;
    /// Bytes a slice reads below which it is not worth a request of its own.
    const size_t min_slice_bytes_to_share;
    const Block primary_key_header;
    const bool reverse;

    mutable std::mutex mutex;
    std::vector<Lane> lanes TSA_GUARDED_BY(mutex);
    LaneQueue queue TSA_GUARDED_BY(mutex);
    /// Where each lane with unread marks sits in the queue.
    std::vector<std::optional<LaneQueue::iterator>> queue_position TSA_GUARDED_BY(mutex);
    /// Slices cut and not yet taken by a source, in the order they were cut.
    std::deque<QueuedSlice> fifo TSA_GUARDED_BY(mutex);
    std::vector<Source> sources TSA_GUARDED_BY(mutex);
    /// Bytes the slices read so far cost and their marks: what a mark costs to read in this query, see canShare.
    size_t read_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t read_marks TSA_GUARDED_BY(mutex) = 0;
    bool finished TSA_GUARDED_BY(mutex) = false;
};

}
