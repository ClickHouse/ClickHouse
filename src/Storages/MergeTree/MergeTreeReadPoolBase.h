#pragma once
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>

namespace DB
{

class MergeTreeReadPartsRanges
{
public:
    explicit MergeTreeReadPartsRanges(RangesInDataParts parts_ranges_) : parts_ranges(std::move(parts_ranges_)) {}

    struct LockedPartRannges
    {
        std::lock_guard<std::mutex> lock;
        RangesInDataParts & parts_ranges;
    };

    LockedPartRannges get() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return {std::lock_guard(mutex), parts_ranges};
    }

private:
    RangesInDataParts parts_ranges;
    std::mutex mutex;
};

using MergeTreeReadPartsRangesPtr = std::shared_ptr<MergeTreeReadPartsRanges>;

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
    };

    MergeTreeReadPoolBase(
        RangesInDataParts && parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    Block getHeader() const override { return header; }

    MergeTreeReadPartsRangesPtr getPartsWithRanges() { return parts_ranges_ptr; }

protected:
    /// Initialized in constructor
    const MergeTreeReadPartsRangesPtr parts_ranges_ptr;
    const MutationsSnapshotPtr mutations_snapshot;
    const VirtualFields shared_virtual_fields;
    const StorageSnapshotPtr storage_snapshot;
    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const MergeTreeReaderSettings reader_settings;
    const Names column_names;
    const PoolSettings pool_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;
    const MarkCachePtr owned_mark_cache;
    const UncompressedCachePtr owned_uncompressed_cache;
    const Block header;
    const bool merge_tree_determine_task_size_by_prewhere_columns;
    const UInt64 merge_tree_min_bytes_per_task_for_remote_reading;
    const UInt64 merge_tree_min_read_task_size;

    void fillPerPartInfos(const RangesInDataParts & parts_ranges);
    static std::vector<size_t> getPerPartSumMarks(const RangesInDataParts & parts_ranges);

    MergeTreeReadTaskPtr createTask(MergeTreeReadTaskInfoPtr read_info, MergeTreeReadTask::Readers task_readers, MarkRanges ranges) const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTaskInfoPtr read_info,
        MarkRanges ranges,
        MergeTreeReadTask * previous_task) const;

    MergeTreeReadTask::Extras getExtras() const;

    std::once_flag init_flag;
    std::vector<MergeTreeReadTaskInfoPtr> per_part_infos;
    std::vector<bool> is_part_on_remote_disk;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
};

}
