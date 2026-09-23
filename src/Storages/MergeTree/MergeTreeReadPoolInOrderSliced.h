#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <mutex>
#include <optional>

namespace DB
{

/// Read pool for reading in the order of the primary key with more parallelism than one thread per part.
///
/// Every part is a lane. A lane is cut into segments of a few hundred marks, and a segment into slices;
/// a slice is one MergeTreeReadTask. A segment is bound to one source and read by it sequentially with
/// one set of readers, so consecutive slices continue the same streams and, on remote storage, the same
/// range request. Several segments of one lane may be read by several sources at once, and
/// MergeTreeInOrderSliceRouter reassembles their slices in mark order.
///
/// The pool does no scheduling of its own. The router decides which lane a source reads next and
/// calls openSegment / assignSlice from its prepare; getTask then hands the assigned slice to the source.
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

    /// Marks of the lane not yet cut into a slice, in the lane itself or in one of its segments.
    bool laneHasUnreadMarks(size_t lane) const;
    /// Marks of the lane not yet taken by any segment.
    bool laneHasMarksOutsideSegments(size_t lane) const;
    /// Smallest mark of the lane not yet cut into a slice, or SIZE_MAX if there is none.
    size_t laneFirstUnreadMark(size_t lane) const;
    /// Smallest mark of the lane not taken by any segment, or SIZE_MAX if there is none.
    size_t laneFirstMarkOutsideSegments(size_t lane) const;

    std::optional<size_t> segmentLane(size_t source) const;
    bool segmentHasUnreadMarks(size_t source) const;
    /// Smallest mark of the segment bound to the source not yet cut into a slice, or SIZE_MAX if there is none.
    size_t segmentFirstUnreadMark(size_t source) const;

    /// Binds the next marks of the lane to the source as a new segment. Marks of the segment previously
    /// bound to the source that were not read yet return to their lane.
    void openSegment(size_t source, size_t lane);

    /// Cuts the next slice from the segment bound to the source; getTask of that source returns it.
    SliceDescription assignSlice(size_t source);
    /// True between assignSlice and the getTask call that takes the slice.
    bool hasPendingSlice(size_t source) const;

private:
    struct Lane
    {
        MarkRanges unread;
        /// Slices of a lane start small and grow, so the first rows of a part arrive quickly.
        size_t slices_cut = 0;
        /// End of the furthest slice cut so far; marks below it that are still unread came back from a
        /// segment taken away from an idle source.
        size_t max_cut_mark = 0;
    };

    struct Segment
    {
        size_t lane;
        /// All marks of the segment, the extent of its readers.
        MarkRanges extent;
        MarkRanges unread;
        bool has_readers = false;
    };

    struct PendingSlice
    {
        MarkRanges ranges;
    };

    Block buildBoundary(size_t lane, const Block & primary_key_header) const;
    void returnSegmentToLane(size_t source) TSA_REQUIRES(mutex);

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t max_slice_marks;
    const size_t segment_marks;

    /// Immutable after construction.
    std::vector<Block> boundaries;
    std::vector<size_t> lanes_by_boundary;

    mutable std::mutex mutex;
    std::vector<Lane> lanes TSA_GUARDED_BY(mutex);
    std::vector<std::optional<Segment>> segments TSA_GUARDED_BY(mutex);
    std::vector<std::optional<PendingSlice>> pending TSA_GUARDED_BY(mutex);
};

}
