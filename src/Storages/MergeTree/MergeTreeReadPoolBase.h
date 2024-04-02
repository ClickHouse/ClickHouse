#pragma once
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>

namespace DB
{

class MergeTreeReadPoolBase : public IMergeTreeReadPool
{
public:
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
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        const PoolSettings & settings_,
        const ContextPtr & context_);

    Block getHeader() const override { return header; }

protected:
    /// Initialized in constructor
    const RangesInDataParts parts_ranges;
    const StorageSnapshotPtr storage_snapshot;
    const PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const MergeTreeReaderSettings reader_settings;
    const Names column_names;
    const Names virtual_column_names;
    const PoolSettings pool_settings;
    const MarkCachePtr owned_mark_cache;
    const UncompressedCachePtr owned_uncompressed_cache;
    const Block header;

    void fillPerPartInfos();
    std::vector<size_t> getPerPartSumMarks() const;

    MergeTreeReadTaskPtr createTask(
        MergeTreeReadTask::InfoPtr read_info,
        MarkRanges ranges,
        MergeTreeReadTask * previous_task) const;

    MergeTreeReadTask::Extras getExtras() const;

    std::vector<MergeTreeReadTask::InfoPtr> per_part_infos;
    std::vector<bool> is_part_on_remote_disk;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
};

}
