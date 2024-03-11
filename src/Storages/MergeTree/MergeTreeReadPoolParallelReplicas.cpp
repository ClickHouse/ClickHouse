#include <iterator>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolParallelReplicas::MergeTreeReadPoolParallelReplicas(
    ParallelReadingExtension extension_,
    RangesInDataParts && parts_,
    VirtualFields shared_virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const ContextPtr & context_)
    : MergeTreeReadPoolBase(
        std::move(parts_),
        std::move(shared_virtual_fields_),
        storage_snapshot_,
        prewhere_info_,
        actions_settings_,
        reader_settings_,
        column_names_,
        settings_,
        context_)
    , extension(std::move(extension_))
    , coordination_mode(CoordinationMode::Default)
{
    extension.all_callback(
        InitialAllRangesAnnouncement(coordination_mode, parts_ranges.getDescriptions(), extension.number_of_current_replica));
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t /*task_idx*/, MergeTreeReadTask * previous_task)
{
    std::lock_guard lock(mutex);

    if (no_more_tasks_available)
        return nullptr;

    if (buffered_ranges.empty())
    {
        auto result = extension.callback(ParallelReadRequest(
            coordination_mode,
            extension.number_of_current_replica,
            pool_settings.min_marks_for_concurrent_read * pool_settings.threads,
            /// For Default coordination mode we don't need to pass part names.
            RangesInDataPartsDescription{}));

        if (!result || result->finish)
        {
            no_more_tasks_available = true;
            return nullptr;
        }

        buffered_ranges = std::move(result->description);
    }

    if (buffered_ranges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No tasks to read. This is a bug");

    auto & current_task = buffered_ranges.front();

    auto part_it
        = std::ranges::find_if(per_part_infos, [&current_task](const auto & part) { return part->data_part->info == current_task.info; });
    if (part_it == per_part_infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Assignment contains an unknown part (current_task: {})", current_task.describe());
    const size_t part_idx = std::distance(per_part_infos.begin(), part_it);

    MarkRanges ranges_to_read;
    size_t current_sum_marks = 0;
    while (current_sum_marks < pool_settings.min_marks_for_concurrent_read && !current_task.ranges.empty())
    {
        auto diff = pool_settings.min_marks_for_concurrent_read - current_sum_marks;
        auto range = current_task.ranges.front();
        if (range.getNumberOfMarks() > diff)
        {
            auto new_range = range;
            new_range.end = range.begin + diff;
            range.begin += diff;

            current_task.ranges.front() = range;
            ranges_to_read.push_back(new_range);
            current_sum_marks += new_range.getNumberOfMarks();
            continue;
        }

        ranges_to_read.push_back(range);
        current_sum_marks += range.getNumberOfMarks();
        current_task.ranges.pop_front();
    }

    if (current_task.ranges.empty())
        buffered_ranges.pop_front();

    return createTask(per_part_infos[part_idx], std::move(ranges_to_read), previous_task);
}

}
