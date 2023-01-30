#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>

#include <ctime>
#include <mutex>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolParallelReplicas::~MergeTreeReadPoolParallelReplicas() = default;


void MergeTreeReadPoolParallelReplicas::initialize()
{
    auto desc = parts_ranges.getDescriptions();

    extension.all_callback(InitialAllRangesAnnouncement{
        .description = desc,
        .replica_num = extension.number_of_current_replica,
    });

    sendRequest();
}

Block MergeTreeReadPoolParallelReplicas::getHeader() const
{
    return storage_snapshot->getSampleBlockForColumns(extension.colums_to_read);
}


std::vector<size_t> MergeTreeReadPoolParallelReplicas::fillPerPartInfo(const RangesInDataParts & parts)
{
    std::vector<size_t> per_part_sum_marks;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    for (const auto index : collections::range(0, parts.size()))
    {
        const auto & part = parts[index];

        /// Read marks for every data part.
        size_t sum_marks = 0;
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);

        auto task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(part.data_part),
            storage_snapshot,
            column_names,
            virtual_column_names,
            prewhere_info,
            /*with_subcolumns=*/true);

        /// Ignore preferred_block_size_bytes
        auto size_predictor = IMergeTreeSelectAlgorithm::getSizePredictor(part.data_part, task_columns, sample_block);

        auto & per_part = parts_ranges_with_params.emplace_back();

        per_part.data_part = part;
        per_part.size_predictor = std::move(size_predictor);

        /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
        const auto & required_column_names = task_columns.columns.getNames();
        per_part.column_name_set = {required_column_names.begin(), required_column_names.end()};
        per_part.task_columns = std::move(task_columns);
    }

    return per_part_sum_marks;
}


void MergeTreeReadPoolParallelReplicas::sendRequest()
{
    auto promise_response = std::make_shared<std::promise<std::optional<ParallelReadResponse>>>();
    future_response = promise_response->get_future();
}


MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t thread)
{
    /// This parameter is needed only to satisfy the interface
    UNUSED(thread);

    std::lock_guard lock(mutex);

    if (no_more_tasks_available)
        return nullptr;

    if (buffered_ranges.empty())
    {
        auto result = extension.callback(ParallelReadRequest{
            .replica_num = extension.number_of_current_replica, .min_number_of_marks = min_marks_for_concurrent_read * threads});

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

    RangesInDataPart part;
    size_t part_idx = 0;
    for (size_t index = 0; index < parts_ranges_with_params.size(); ++index)
    {
        auto & other_part = parts_ranges_with_params[index];
        if (other_part.data_part.data_part->info == current_task.info)
        {
            part = other_part.data_part;
            part_idx = index;
            break;
        }
    }

    MarkRanges ranges_to_read;
    size_t current_sum_marks = 0;
    while (current_sum_marks < min_marks_for_concurrent_read && !current_task.ranges.empty())
    {
        auto diff = min_marks_for_concurrent_read - current_sum_marks;
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

    const auto & per_part = parts_ranges_with_params[part_idx];

    auto curr_task_size_predictor
        = !per_part.size_predictor ? nullptr : std::make_unique<MergeTreeBlockSizePredictor>(*per_part.size_predictor); /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        part.data_part,
        ranges_to_read,
        part.part_index_in_query,
        per_part.column_name_set,
        per_part.task_columns,
        prewhere_info && prewhere_info->remove_prewhere_column,
        std::move(curr_task_size_predictor));
}


void MergeTreeInOrderReadPoolParallelReplicas::initialize()
{
    auto callback = extension.all_callback;
    callback({
        .description = parts_ranges.getDescriptions(),
        .replica_num = extension.number_of_current_replica
    });
}


MarkRanges MergeTreeInOrderReadPoolParallelReplicas::getNewTask(RangesInDataPartDescription description)
{
    std::lock_guard lock(mutex);

    auto get_from_buffer = [&]() -> std::optional<MarkRanges>
    {
        for (auto & desc : buffered_tasks)
        {
            if (desc.info == description.info && !desc.ranges.empty())
            {
                auto result = std::move(desc.ranges);
                desc.ranges = MarkRanges{};
                return result;
            }
        }
        return std::nullopt;
    };

    if (auto result = get_from_buffer(); result)
        return result.value();

    if (no_more_tasks)
        return {};

    auto response = extension.callback(ParallelReadRequest{
        .mode = mode,
        .replica_num = extension.number_of_current_replica,
        .min_number_of_marks = min_marks_for_concurrent_read * request.size(),
        .description = request,
    });

    if (!response || response->description.empty() || response->finish)
    {
        no_more_tasks = true;
        return {};
    }

    /// Fill the buffer
    for (size_t i = 0; i < request.size(); ++i)
    {
        auto & new_ranges = response->description[i].ranges;
        auto & old_ranges = buffered_tasks[i].ranges;
        std::move(new_ranges.begin(), new_ranges.end(), std::back_inserter(old_ranges));
    }

    if (auto result = get_from_buffer(); result)
        return result.value();

    return {};
}

}
