#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <mutex>

namespace DB
{

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

class MergeTreeReadPoolInOrder : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolInOrder(
        bool has_limit_below_one_block_,
        MergeTreeReadType read_type_,
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
        const MergeTreeIndexBuildContextPtr & index_build_context_ = {});

    String getName() const override { return "ReadPoolInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    const bool has_limit_below_one_block;
    const MergeTreeReadType read_type;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    MergeTreeIndexBuildContextPtr index_build_context;

    struct PartIndexResultCacheEntry
    {
        std::once_flag flag;
        MergeTreeIndexReadResultPtr result;
    };

    std::vector<std::unique_ptr<PartIndexResultCacheEntry>> per_part_index_result_cache;

    MergeTreeIndexReadResultPtr getCachedPartIndexResult(size_t part_idx);

    std::vector<MarkRanges> per_part_mark_ranges;
};

}
