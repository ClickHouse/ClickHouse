#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <mutex>
#include <optional>

namespace DB
{

/// Read pool for reading in the order of the primary key with more parallelism than one thread per part.
///
/// Every part is a lane. Slices are cut from the front of the lane's unread marks, and a slice is one
/// MergeTreeReadTask. A source is bound to a lane and takes its next slice whenever it finishes one, so a
/// lane read by a single source is read sequentially with one set of readers and one range request on
/// remote storage. Several sources bound to the same lane take its slices in turn, and
/// MergeTreeInOrderSliceRouter reassembles them in mark order.
///
/// The pool does no scheduling of its own. The router decides which lane a source reads next and
/// calls bindSource / assignSlice from its prepare; getTask then hands the assigned slice to the source.
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
        size_t first_mark;
        /// Rows in the slice before any filtering, to tell a slice whose rows were mostly filtered out.
        size_t rows;
    };

    size_t numSources() const { return num_sources; }
    size_t numLanes() const { return boundaries.size(); }

    /// Lane indexes in the order the merge will need them: by the primary key value at the first mark.
    const std::vector<size_t> & lanesByBoundary() const { return lanes_by_boundary; }

    /// Primary key values at the first mark of the lane, one row; empty if the index has no value there.
    const Block & laneBoundary(size_t lane) const { return boundaries[lane]; }

    /// Marks of the lane not yet cut into a slice.
    bool laneHasUnreadMarks(size_t lane) const;

    /// The lane the source takes its slices from.
    std::optional<size_t> sourceLane(size_t source) const;

    /// Makes the source take its next slices from the lane. The readers a source used for another lane
    /// are parked in that lane and picked up by whichever source reads it next.
    void bindSource(size_t source, size_t lane);

    /// Cuts the next slice of the source's lane; getTask of that source returns it.
    SliceDescription assignSlice(size_t source);

    /// True between assignSlice and the getTask call that takes the slice.
    bool hasPendingSlice(size_t source) const;

    /// Drops the readers parked in a lane that is not going to be read anymore.
    void releaseLaneReaders(size_t lane);

private:
    struct Lane
    {
        MarkRanges unread;
        /// Slices of a lane start small and grow, so the first rows of a part arrive quickly.
        size_t slices_cut = 0;
        /// Readers of sources that moved on to other lanes; their extent reaches the end of the lane.
        std::vector<MergeTreeReadTask::Readers> parked_readers = {};
    };

    struct PendingSlice
    {
        MarkRanges ranges;
    };

    Block buildBoundary(size_t lane, const Block & primary_key_header) const;
    /// Marks of the lane from first_mark to its end: the extent of readers created for a slice, so that
    /// the same readers can continue with the following slices of the lane.
    MarkRanges readerExtent(size_t lane, size_t first_mark) const;

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t max_slice_marks;

    /// Immutable after construction.
    std::vector<Block> boundaries;
    std::vector<size_t> lanes_by_boundary;

    mutable std::mutex mutex;
    std::vector<Lane> lanes TSA_GUARDED_BY(mutex);
    /// The lane each source takes its slices from.
    std::vector<std::optional<size_t>> bound_lane TSA_GUARDED_BY(mutex);
    /// The lane of the last task each source got, i.e. the lane its current readers belong to.
    std::vector<std::optional<size_t>> last_task_lane TSA_GUARDED_BY(mutex);
    std::vector<std::optional<PendingSlice>> pending TSA_GUARDED_BY(mutex);
};

}
