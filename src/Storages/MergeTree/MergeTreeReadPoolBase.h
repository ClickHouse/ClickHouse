#pragma once
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
        const size_t total_query_nodes;
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

protected:
    /// Initialized in constructor
    const RangesInDataParts parts_ranges;
    const MutationsSnapshotPtr mutations_snapshot;
    const VirtualFields shared_virtual_fields;
    const IndexReadTasks index_read_tasks;
    const StorageSnapshotPtr storage_snapshot;
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
        std::vector<MarkRanges> patches_ranges) const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MarkRanges ranges,
        std::vector<MarkRanges> patches_ranges,
        MergeTreeReadTask * previous_task) const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MarkRanges ranges,
        MergeTreeReadTask * previous_task) const;

    MergeTreeReadTask::Extras getExtras() const;

    std::vector<MergeTreeReadTaskInfoPtr> per_part_infos;
    RangesInPatchParts ranges_in_patch_parts;
    std::vector<bool> is_part_on_remote_disk;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
};

}
