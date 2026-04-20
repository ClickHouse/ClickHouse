#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool projection_index_narrow_marks;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolInOrder::MergeTreeReadPoolInOrder(
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
    const MergeTreeIndexBuildContextPtr & index_build_context_)
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
    , has_limit_below_one_block(has_limit_below_one_block_)
    , read_type(read_type_)
    , updater(std::move(updater_))
    , index_build_context(context_->getSettingsRef()[Setting::projection_index_narrow_marks] ? index_build_context_ : nullptr)
{
    if (index_build_context)
    {
        per_part_index_result_cache.resize(per_part_infos.size());
        for (auto & slot : per_part_index_result_cache)
            slot = std::make_unique<PartIndexResultCacheEntry>();
    }

    per_part_mark_ranges.reserve(parts_ranges.size());
    for (const auto & part_with_ranges : parts_ranges)
        per_part_mark_ranges.push_back(part_with_ranges.ranges);
}

MergeTreeReadTaskPtr MergeTreeReadPoolInOrder::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    if (task_idx >= per_part_infos.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Requested task with idx {}, but there are only {} parts",
            task_idx, per_part_infos.size());

    auto & all_mark_ranges = per_part_mark_ranges[task_idx];

    /// When the projection index narrows a popped range to empty, loop back to pop the
    /// next range rather than returning nullptr -- nullptr means "no more work for this
    /// part" and would prematurely terminate reading of remaining ranges. The InOrder
    /// path has a consume-all branch that leaves `all_mark_ranges` moved-from; when
    /// that branch narrows to empty there is nothing left to pop, so we return nullptr
    /// immediately rather than re-entering the loop with a moved-from container.
    while (true)
    {
        if (all_mark_ranges.empty())
            return nullptr;

        MarkRanges mark_ranges_for_task;
        bool consumed_all_ranges = false;
        if (read_type == MergeTreeReadType::InReverseOrder)
        {
            /// Read ranges from right to left.
            mark_ranges_for_task.emplace_back(std::move(all_mark_ranges.back()));
            all_mark_ranges.pop_back();
        }
        else if (has_limit_below_one_block)
        {
            /// If we need to read few rows, set one range per task to reduce number of read data.
            mark_ranges_for_task.emplace_back(std::move(all_mark_ranges.front()));
            all_mark_ranges.pop_front();
        }
        else
        {
            mark_ranges_for_task = std::move(all_mark_ranges);
            consumed_all_ranges = true;
        }

        if (index_build_context)
        {
            const auto & data_part = per_part_infos[task_idx]->data_part;
            const size_t min_marks_for_seek = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
                reader_settings.merge_tree_min_rows_for_seek,
                reader_settings.merge_tree_min_bytes_for_seek,
                data_part->index_granularity_info.fixed_index_granularity,
                data_part->index_granularity_info.index_granularity_bytes);

            mark_ranges_for_task = narrowMarkRangesByProjectionIndex(
                getCachedPartIndexResult(task_idx),
                *data_part->index_granularity,
                std::move(mark_ranges_for_task),
                min_marks_for_seek);

            if (mark_ranges_for_task.empty())
            {
                if (consumed_all_ranges)
                    return nullptr;
                continue;
            }
        }

        return createTask(per_part_infos[task_idx], std::move(mark_ranges_for_task), previous_task, updater);
    }
}

MergeTreeIndexReadResultPtr MergeTreeReadPoolInOrder::getCachedPartIndexResult(size_t part_idx)
{
    auto & entry = *per_part_index_result_cache[part_idx];
    std::call_once(entry.flag, [&]
    {
        entry.result = lookupProjectionIndexResult(
            *index_build_context,
            per_part_infos[part_idx]->part_index_in_query);
    });
    return entry.result;
}

}
