#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>

namespace DB
{

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
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const MergeTreeReadTask::BlockSizeParams & params_,
    const ContextPtr & context_)
    : MergeTreeReadPoolBase(
        std::move(parts_),
        std::move(mutations_snapshot_),
        std::move(shared_virtual_fields_),
        storage_snapshot_,
        prewhere_info_,
        actions_settings_,
        reader_settings_,
        column_names_,
        settings_,
        params_,
        context_)
    , has_limit_below_one_block(has_limit_below_one_block_)
    , read_type(read_type_)
{
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
    if (all_mark_ranges.empty())
        return nullptr;

    MarkRanges mark_ranges_for_task;
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
    }

    return createTask(per_part_infos[task_idx], std::move(mark_ranges_for_task), previous_task);
}

}
