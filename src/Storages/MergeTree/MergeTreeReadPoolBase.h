#pragma once
#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class UncompressedCache;
using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

class MergeTreeReadPoolBase : public IMergeTreeReadPool, protected WithContext
{
public:
    using MutationsSnapshotPtr = MergeTreeData::MutationsSnapshotPtr;

    struct PoolSettings
    {
        size_t threads = 0;
        size_t sum_marks = 0;
        size_t min_marks_for_concurrent_read = 0;
        size_t preferred_block_size_bytes = 0;

        bool use_uncompressed_cache = false;
        bool do_not_steal_tasks = false;
        bool use_const_size_tasks_for_remote_reading = false;

        // Not the same as the similar field in `ParallelReadingExtension`. Accounts for `max_parallel_replicas`.
        const size_t total_query_nodes{};
    };

    MergeTreeReadPoolBase(
        RangesInDataParts && parts_,
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
        const ContextPtr & context_);

    /// Simplified c'tor for MergeTreeReadPoolProjectionIndex
    MergeTreeReadPoolBase(
        MutationsSnapshotPtr mutations_snapshot_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & pool_settings_,
        const MergeTreeReadTask::BlockSizeParams & block_size_params_,
        const ContextPtr & context_);

    Block getHeader() const override { return header; }

    /// Build the descriptions list for the initial parallel-replicas announcement: same as
    /// `parts_ranges.getDescriptions()` but with per-part `min_marks_per_task` filled in from
    /// `per_part_infos`. The caller (ReadFromMergeTree) sends the announcement via the
    /// `ParallelReadingExtension` it constructed before passing into the pool.
    virtual RangesInDataPartsDescription buildAnnouncementDescriptions() const;

    /// Must be called before the pipeline starts to call getTask. Not every pool applies the
    /// refiner: see refineReadRanges calls in getTask of the concrete pools.
    void setReadRangesRefiner(MergeTreeReadRangesRefinerPtr refiner) { ranges_refiner = std::move(refiner); }

protected:
    /// Initialized in constructor
    const StorageSnapshotPtr storage_snapshot;
    const RangesInDataParts parts_ranges;
    const MutationsSnapshotPtr mutations_snapshot;
    const VirtualFields shared_virtual_fields;
    const IndexReadTasks index_read_tasks;
    const FilterDAGInfoPtr row_level_filter;
    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const MergeTreeReaderSettings reader_settings;
    const Names column_names;
    const PoolSettings pool_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;
    const MarkCachePtr owned_mark_cache;
    const UncompressedCachePtr owned_uncompressed_cache;
    const PatchJoinCachePtr patch_join_cache;
    const Block header;

    MergeTreeReadTaskInfo buildReadTaskInfo(const RangesInDataPart & part_with_ranges, const Settings & settings) const;

    void fillPerPartInfos(const Settings & settings);
    std::vector<size_t> getPerPartSumMarks() const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MergeTreeReadTask::Readers task_readers,
        MarkRanges ranges,
        std::vector<MarkRanges> patches_ranges,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater = nullptr) const;

    /// `read_request_map` narrows the part's map, e.g. to the assignment of parallel replicas.
    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MarkRanges ranges,
        std::vector<MarkRanges> patches_ranges,
        MergeTreeReadTask * previous_task,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater = nullptr,
        const MarkRangesPtr & read_request_map = nullptr) const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MarkRanges ranges,
        MergeTreeReadTask * previous_task,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater = nullptr,
        const MarkRangesPtr & read_request_map = nullptr) const;

    MergeTreeReadTask::Extras getExtras() const;

    /// Applies the refiner (if any) to ranges cut from a part right before creating a read task.
    /// May block (see IMergeTreeReadRangesRefiner), do not call under the pool scheduling mutex.
    MarkRanges refineReadRanges(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const;

    /// The initial map without the ranges that the refiner has dropped so far. The initial map is `replica_map`
    /// for a parallel replica's task, or the part's map from the index analysis when `replica_map` is null.
    MarkRangesPtr getActualReadRequestMap(const MergeTreeReadTaskInfo & info, const MarkRangesPtr & replica_map) const;

    /// The read request maps of the patch parts for `actual_map`. Empty when `actual_map` is the part's map
    /// from the index analysis, because the task info already holds the patch maps for it.
    std::vector<MarkRangesPtr> getActualPatchReadRequestMaps(const MergeTreeReadTaskInfo & info, const MarkRangesPtr & actual_map) const;

    MergeTreeReadRangesRefinerPtr ranges_refiner;

    std::vector<MergeTreeReadTaskInfoPtr> per_part_infos;
    RangesInPatchParts ranges_in_patch_parts;
    std::vector<bool> is_part_on_remote_disk;

    ReadBufferFromFileBase::ProfileCallback profile_callback;

private:
    /// Cached narrowed maps of a part, so that its tasks share one map until it changes.
    struct PartReadRequestMaps
    {
        /// Held, not only compared, so that a new assignment cannot reuse its address.
        MarkRangesPtr initial_map;
        /// Appended as the refiner drops them; each new batch is sorted in place when it is applied.
        MarkRanges dropped;
        /// `actual_map` is `initial_map` without the first `dropped_in_map` entries of `dropped`.
        size_t dropped_in_map = 0;
        MarkRangesPtr actual_map;
        /// `actual_patch_maps` come from `patch_maps_source`.
        MarkRangesPtr patch_maps_source;
        std::vector<MarkRangesPtr> actual_patch_maps;
    };

    void recordDroppedRanges(const MergeTreeReadTaskInfo & info, MarkRanges cut, MarkRanges refined) const;

    mutable std::mutex part_read_request_maps_mutex;
    mutable std::unordered_map<const MergeTreeReadTaskInfo *, PartReadRequestMaps> part_read_request_maps TSA_GUARDED_BY(part_read_request_maps_mutex);
};

}
