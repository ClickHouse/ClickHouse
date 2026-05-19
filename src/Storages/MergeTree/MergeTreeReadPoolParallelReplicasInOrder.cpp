#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicasInOrder.h>

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

MergeTreeReadPoolParallelReplicasInOrder::MergeTreeReadPoolParallelReplicasInOrder(
    ParallelReadingExtension extension_,
    CoordinationMode mode_,
    RangesInDataParts parts_,
    MutationsSnapshotPtr mutations_snapshot_,
    VirtualFields shared_virtual_fields_,
    const IndexReadTasks & index_read_tasks_,
    bool has_hard_limit_below_one_block_,
    bool has_soft_limit_below_one_block_,
    const StorageSnapshotPtr & storage_snapshot_,
    const FilterDAGInfoPtr & row_level_filter_,
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
    , extension(std::move(extension_))
    , mode(mode_)
    , has_hard_limit_below_one_block(has_hard_limit_below_one_block_)
    , has_soft_limit_below_one_block(has_soft_limit_below_one_block_)
    , min_marks_per_task(pool_settings.min_marks_for_concurrent_read)
{
    for (const auto & info : per_part_infos)
        min_marks_per_task = std::max(min_marks_per_task, info->min_marks_per_task);

    if (min_marks_per_task == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero (likely because of weird interference of settings)");

    min_marks_per_request = min_marks_per_task * pool_settings.threads;

    for (const auto & part : parts_ranges)
    {
        bool is_projection = part.data_part->isProjectionPart();
        chassert(!is_projection || part.parent_part);

        auto info = is_projection ? part.parent_part->info : part.data_part->info;
        auto projection_name = is_projection ? part.data_part->name : "";

        request.push_back({.info = info, .ranges = MarkRanges{}, .projection_name = projection_name});
        buffered_tasks.push_back({.info = std::move(info), .ranges = MarkRanges{}, .projection_name = std::move(projection_name)});
    }

    auto descriptions = parts_ranges.getDescriptions();
    chassert(descriptions.size() == per_part_infos.size());
    for (size_t i = 0; i < descriptions.size(); ++i)
        descriptions[i].min_marks_per_task = per_part_infos[i]->min_marks_per_task;

    auto response = extension.sendInitialRequest(mode, std::move(descriptions), /*mark_segment_size=*/0, min_marks_per_request);

    /// Build the authoritative parts set from the coordinator's response. Consumers for parts
    /// outside this set finish immediately (no phantom getTask spinning), and read requests
    /// only carry parts the coordinator's stream actually contains. `std::nullopt` means the
    /// initiator is on an older protocol and didn't send a response — leave the flag unset and
    /// fall back to the pre-pruning behavior. An engaged optional with empty `parts` is a
    /// valid response that means the stream owns nothing (over-announced split) — every
    /// consumer of this pool should finish.
    if (response)
    {
        authoritative_parts_received = true;
        for (const auto & part : response->parts)
            authoritative_parts.emplace(part.info, part.projection_name);
    }

    per_part_marks_in_range.resize(per_part_infos.size(), 1);
}

bool MergeTreeReadPoolParallelReplicasInOrder::wasSelectedByInitiator(const MergeTreePartInfo & info, const String & projection_name) const
{
    return authoritative_parts_received && authoritative_parts.contains({info, projection_name});
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicasInOrder::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    std::lock_guard lock(mutex);

    if (task_idx >= per_part_infos.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Requested task with idx {}, but there are only {} parts", task_idx, per_part_infos.size());

    bool is_projection = per_part_infos[task_idx]->data_part->isProjectionPart();
    chassert(!is_projection || per_part_infos[task_idx]->parent_part);

    const auto & part_info = is_projection ? per_part_infos[task_idx]->parent_part->info : per_part_infos[task_idx]->data_part->info;
    const auto & projection_name = is_projection ? per_part_infos[task_idx]->data_part->name : "";

    /// Phantom consumers: this consumer's (part, projection_name) isn't in the coordinator's stream
    /// — finish it immediately rather than spinning on getTask. We only filter when the coordinator
    /// actually reported its authoritative set; if it didn't (older initiator), fall back to the
    /// pre-existing behavior (no pruning). An empty authoritative set with the flag set means the
    /// stream doesn't exist on the coordinator (over-announced split) — every consumer of this
    /// pool should finish.
    if (authoritative_parts_received && !authoritative_parts.contains({part_info, projection_name}))
        return nullptr;

    auto & marks_in_range = per_part_marks_in_range[task_idx];
    auto get_from_buffer = [&]() -> std::optional<MarkRanges>
    {
        /// Cap the warmup growth at `min_marks_per_task` so that steady-state task size
        /// matches what the Default pool uses. The initial small ranges still allow early
        /// termination for LIMIT queries.
        const size_t task_size_cap = min_marks_per_task;
        for (auto & desc : buffered_tasks)
        {
            if (desc.info == part_info && desc.projection_name == projection_name && !desc.ranges.empty())
            {
                if (mode == CoordinationMode::WithOrder)
                {
                    /// Past warmup: return all remaining ranges as one task.
                    if (marks_in_range > task_size_cap)
                    {
                        auto result = std::move(desc.ranges);
                        desc.ranges = MarkRanges{};
                        ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, result.getNumberOfMarks());
                        return result;
                    }

                    /// For asc limit, just return one range.
                    /// With a hard limit (no filter) reading stops exactly at the limit, so always emit
                    /// single-range tasks. With a soft limit (filter + LIMIT) the estimation may be off,
                    /// so apply this only to the first task per part: if it didn't reach the limit, the
                    /// filter is likely selective and we should continue with regular block size.
                    if (has_hard_limit_below_one_block || (has_soft_limit_below_one_block && !previous_task))
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
                        while (marks_in_range <= task_size_cap && range.begin + marks_in_range < range.end)
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
                        marks_in_range = std::min(marks_in_range * 2, task_size_cap);
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

    if (failed_to_get_task)
        return nullptr;

    std::optional<ParallelReadResponse> response;
    try
    {
        response = extension.sendReadInOrderRequest(mode, min_marks_per_request, request);
        if (response)
        {
            LOG_DEBUG(log, "Got response: {}", response->describe());
            if (response->description.empty() || response->finish)
                no_more_tasks = true;
        }
        else
        {
            LOG_DEBUG(log, "Got no response");
            no_more_tasks = true;
        }
    }
    catch (...)
    {
        failed_to_get_task = true;
        throw;
    }

    if (no_more_tasks)
        return nullptr;

    /// Fill the buffer — match response parts to buffered_tasks by part info,
    /// not by position, because the coordinator may return parts in a different order.
    for (auto & received_part : response->description)
    {
        auto it = std::find_if(buffered_tasks.begin(), buffered_tasks.end(),
            [&](const RangesInDataPartDescription & task)
            {
                return task.info == received_part.info && task.projection_name == received_part.projection_name;
            });

        if (it == buffered_tasks.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Coordinator returned part {} that wasn't announced by this replica",
                received_part.describe());

        if (mode == CoordinationMode::WithOrder)
            it->ranges.insert(it->ranges.end(), std::make_move_iterator(received_part.ranges.begin()), std::make_move_iterator(received_part.ranges.end()));
        else
            it->ranges.insert(it->ranges.begin(), std::make_move_iterator(received_part.ranges.begin()), std::make_move_iterator(received_part.ranges.end()));
    }

    if (auto result = get_from_buffer())
        return createTask(per_part_infos[task_idx], std::move(*result), previous_task);

    return nullptr;
}

}
