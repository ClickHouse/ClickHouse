#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <mutex>
#include <optional>
#include <set>

namespace DB
{

/// Read pool for reading in the order of the primary key with more parallelism than one thread per part.
///
/// Every part is a lane. Slices are cut from the front of the lane's unread marks, and a slice is one
/// MergeTreeReadTask. Lanes with unread marks are queued by the primary key at their next unread mark, so
/// the head of the queue is the slice the merge is going to need next, whichever part it belongs to. The
/// slices of a lane start with one mark and double up to `min_marks_for_concurrent_read`: the first rows
/// of a part arrive after one granule, and a lane the merge stays on is read in large slices.
///
/// The pool does no scheduling of its own. MergeTreeInOrderSliceRouter decides in its prepare which
/// lanes are read and how far ahead of the merge, and calls assignSlice for an idle source; getTask then
/// hands the slice to that source, and the router reassembles the slices of a lane in mark order. Readers
/// follow the lane, not the source: a source that switches lanes leaves its readers parked in the lane
/// for whichever source reads it next. Readers are created for the marks of one slice, like the readers
/// of the other pools are created for one task: their read buffers are sized from those marks, so the
/// first granule of a lane does not fetch whole buffers of every column from remote storage. When the
/// slices of a lane have grown well past the size its readers were made for, new readers replace them.
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
        const Block & primary_key_header_);

    String getName() const override { return "ReadPoolInOrderSliced"; }
    bool preservesOrderOfRanges() const override { return false; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    struct SliceDescription
    {
        size_t lane;
        size_t first_mark;
        size_t marks;
        /// Rows in the slice before any filtering, to tell a slice whose rows were mostly filtered out.
        size_t rows;
    };

    size_t numSources() const { return num_sources; }
    size_t numLanes() const { return boundaries.size(); }
    /// Marks in a slice of a lane that has been read for a while; the first slices of a lane are smaller.
    size_t maxSliceMarks() const { return max_slice_marks; }

    /// Marks of the next slice of the lane, if it were cut now.
    size_t nextSliceMarks(size_t lane) const;

    /// Primary key values at the first mark of the lane, one row; empty if the index has no value there.
    const Block & laneBoundary(size_t lane) const { return boundaries[lane]; }

    /// The lane whose next unread mark has the smallest primary key: the lane the merge needs next among
    /// the lanes that still have unread marks.
    std::optional<size_t> nextLane() const;

    /// The lane at the head of the queue if its next key is strictly smaller than the next key of the given
    /// lane, i.e. a lane the merge needs before it gets to the given lane's next slice.
    std::optional<size_t> nextLaneBefore(size_t lane) const;

    /// Marks of the lane not yet cut into a slice.
    bool laneHasUnreadMarks(size_t lane) const;

    /// The lane of the last task the source got, i.e. the lane its current readers belong to.
    std::optional<size_t> lastTaskLane(size_t source) const;

    /// Cuts the next slice of the lane and hands it to the source; getTask of that source returns it.
    SliceDescription assignSlice(size_t source, size_t lane);

    /// True between assignSlice and the getTask call that takes the slice.
    bool hasPendingSlice(size_t source) const;

    /// The lane is not going to be read anymore: its unread marks leave the queue and its parked readers
    /// are dropped.
    void finishLane(size_t lane);

    /// No slice is going to be assigned anymore. A source that finds no task after this ends its stream
    /// instead of waiting for the router.
    void finish();
    bool isFinished() const;

private:
    /// Readers and the number of marks of the slice they were created for, which sized their buffers.
    struct SizedReaders
    {
        MergeTreeReadTask::Readers readers;
        size_t marks;
    };

    struct Lane
    {
        MarkRanges unread;
        /// Slices of a lane start small and grow, so the first rows of a part arrive quickly.
        size_t slices_cut = 0;
        /// Readers of sources that moved on to other lanes.
        std::vector<SizedReaders> parked_readers = {};
    };

    struct PendingSlice
    {
        size_t lane;
        MarkRanges ranges;
    };

    /// A lane with unread marks, ordered by the primary key at its next unread mark.
    struct QueuedLane
    {
        Block key;
        size_t lane;
    };

    struct QueuedLaneLess
    {
        bool operator()(const QueuedLane & lhs, const QueuedLane & rhs) const;
    };

    using LaneQueue = std::set<QueuedLane, QueuedLaneLess>;

    /// Primary key values at the mark of the lane, one row; empty if the index has no value there.
    Block keyAtMark(size_t lane, size_t mark) const;
    size_t nextSliceMarksUnlocked(size_t lane) const TSA_REQUIRES(mutex);
    void enqueueLane(size_t lane) TSA_REQUIRES(mutex);
    void dequeueLane(size_t lane) TSA_REQUIRES(mutex);

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t max_slice_marks;
    const Block primary_key_header;

    /// Immutable after construction.
    std::vector<Block> boundaries;

    mutable std::mutex mutex;
    std::vector<Lane> lanes TSA_GUARDED_BY(mutex);
    LaneQueue queue TSA_GUARDED_BY(mutex);
    /// Where each lane with unread marks sits in the queue.
    std::vector<std::optional<LaneQueue::iterator>> queue_position TSA_GUARDED_BY(mutex);
    /// The lane of the last task each source got, i.e. the lane its current readers belong to, and the
    /// marks those readers were created for.
    std::vector<std::optional<size_t>> last_task_lane TSA_GUARDED_BY(mutex);
    std::vector<size_t> last_readers_marks TSA_GUARDED_BY(mutex);
    std::vector<std::optional<PendingSlice>> pending TSA_GUARDED_BY(mutex);
    bool finished TSA_GUARDED_BY(mutex) = false;
};

}
