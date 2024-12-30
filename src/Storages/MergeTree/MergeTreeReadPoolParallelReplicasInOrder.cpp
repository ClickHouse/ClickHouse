#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicasInOrder.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace ProfileEvents
{
extern const Event ParallelReplicasReadMarks;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsUInt64 index_granularity;
}

MergeTreeReadPoolParallelReplicasInOrder::MergeTreeReadPoolParallelReplicasInOrder(
    ParallelReadingExtension extension_,
    CoordinationMode mode_,
    RangesInDataParts parts_,
    MutationsSnapshotPtr mutations_snapshot_,
    VirtualFields shared_virtual_fields_,
    bool has_limit_below_one_block_,
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
    , extension(std::move(extension_))
    , mode(mode_)
    , has_limit_below_one_block(has_limit_below_one_block_)
    , min_marks_per_task(pool_settings.min_marks_for_concurrent_read)
{
    for (const auto & info : per_part_infos)
        min_marks_per_task = std::max(min_marks_per_task, info->min_marks_per_task);

    if (min_marks_per_task == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero (likely because of weird interference of settings)");

    for (const auto & part : parts_ranges)
        request.push_back({part.data_part->info, MarkRanges{}});

    for (const auto & part : parts_ranges)
        buffered_tasks.push_back({part.data_part->info, MarkRanges{}});

    extension.sendInitialRequest(mode, parts_ranges, /*mark_segment_size_=*/0);

    per_part_marks_in_range.resize(per_part_infos.size(), 1);
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicasInOrder::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    std::lock_guard lock(mutex);

    if (task_idx >= per_part_infos.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Requested task with idx {}, but there are only {} parts", task_idx, per_part_infos.size());

    const auto & part_info = per_part_infos[task_idx]->data_part->info;
    const auto & data_settings = per_part_infos[task_idx]->data_part->storage.getSettings();
    auto & marks_in_range = per_part_marks_in_range[task_idx];
    auto get_from_buffer = [&,
                            rows_granularity = (*data_settings)[MergeTreeSetting::index_granularity],
                            my_max_block_size = this->block_size_params.max_block_size_rows]() -> std::optional<MarkRanges>
    {
        const size_t max_marks_in_range = (my_max_block_size + rows_granularity - 1) / rows_granularity;
        for (auto & desc : buffered_tasks)
        {
            if (desc.info == part_info && !desc.ranges.empty())
            {
                if (mode == CoordinationMode::WithOrder)
                {
                    /// if already splited, just return desc.ranges
                    if (marks_in_range > max_marks_in_range)
                    {
                        auto result = std::move(desc.ranges);
                        desc.ranges = MarkRanges{};
                        ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, result.getNumberOfMarks());
                        return result;
                    }

                    /// for asc limit, just return one range
                    if (has_limit_below_one_block)
                    {
                        MarkRanges result;
                        auto & range = desc.ranges.front();
                        if (range.begin + marks_in_range < range.end)
                        {
                            result.emplace_front(range.begin, range.begin + marks_in_range);
                            range.begin += marks_in_range;
                            marks_in_range *= 2;
                        }
                        else
                        {
                            result.emplace_front(range.begin, range.end);
                            desc.ranges.pop_front();
                        }

                        chassert(result.size() == 1);
                        ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, result.getNumberOfMarks());
                        return result;
                    }

                    /// else split range but still return all MarkRanges
                    MarkRanges result;
                    for (auto range : desc.ranges)
                    {
                        while (marks_in_range <= max_marks_in_range && range.begin + marks_in_range < range.end)
                        {
                            result.emplace_back(range.begin, range.begin + marks_in_range);
                            range.begin += marks_in_range;
                            marks_in_range *= 2;
                        }
                        result.emplace_back(range.begin, range.end);
                    }
                    chassert(!result.empty());
                    desc.ranges = MarkRanges{};
                    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, result.getNumberOfMarks());
                    return result;
                }
                else
                {
                    /// for reverse order just return one range
                    MarkRanges result;
                    auto & range = desc.ranges.back();
                    if (range.begin + marks_in_range < range.end)
                    {
                        result.emplace_front(range.end - marks_in_range, range.end);
                        range.end -= marks_in_range;
                        marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                    }
                    else
                    {
                        result.emplace_front(range.begin, range.end);
                        desc.ranges.pop_back();
                    }

                    chassert(result.size() == 1);
                    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, result.getNumberOfMarks());
                    return result;
                }
            }
        }
        return std::nullopt;
    };

    if (auto result = get_from_buffer())
        return createTask(per_part_infos[task_idx], std::move(*result), previous_task);

    if (no_more_tasks)
        return nullptr;

    auto response = extension.sendReadRequest(mode, min_marks_per_task * request.size(), request);

    if (!response || response->description.empty() || response->finish)
    {
        no_more_tasks = true;
        return nullptr;
    }

    /// Fill the buffer
    for (size_t i = 0; i < request.size(); ++i)
    {
        auto & new_ranges = response->description[i].ranges;
        auto & old_ranges = buffered_tasks[i].ranges;
        std::move(new_ranges.begin(), new_ranges.end(), std::back_inserter(old_ranges));
    }

    if (auto result = get_from_buffer())
        return createTask(per_part_infos[task_idx], std::move(*result), previous_task);

    return nullptr;
}

}
