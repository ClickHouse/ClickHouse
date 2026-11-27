#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <iterator>
#include <ranges>


namespace
{

size_t chooseSegmentSize(
    LoggerPtr log, size_t mark_segment_size, size_t min_marks_per_task, size_t threads, size_t sum_marks, size_t number_of_replicas)
{
    /// Mark segment size determines the granularity of work distribution between replicas.
    /// Namely, coordinator will take mark segments of size `mark_segment_size` granules, calculate hash of this segment and assign it to corresponding replica.
    /// Small segments are good when we read a small random subset of a table, big - when we do full-scan over a large table.
    /// With small segments there is a problem: consider a query like `select max(time) from wikistat`. Average size of `time` per granule is ~5KB. So when we
    /// read 128 granules we still read only ~0.5MB of data. With default fs cache segment size of 4MB it means a lot of data will be downloaded and written
    /// in cache for no reason. General case will look like this:
    ///
    ///                                    +---------- useful data
    ///                                    v
    ///                           +------+--+------+
    ///                           |------|++|      |
    ///                           |------|++|      |
    ///                           +------+--+------+
    ///                               ^
    /// predownloaded data -----------+
    ///
    /// Having large segments solves all the problems in this case. Also bigger segments mean less requests (especially for big tables and full-scans).
    /// These three values below chosen mostly intuitively. 128 granules is 1M rows - just a good starting point, 16384 seems to still make sense when reading
    /// billions of rows and 1024 - is a reasonable point in between. We limit our choice to only these three options because when we change segment size
    /// we essentially change distribution of data between replicas and of course we don't want to use simultaneously tens of different distributions, because
    /// it would be a huge waste of cache space.
    constexpr std::array<size_t, 3> borders{128, 1024, 16384};

    LOG_TRACE(
        log,
        "mark_segment_size={}, min_marks_per_task*threads={}, sum_marks/number_of_replicas^2={}",
        mark_segment_size,
        min_marks_per_task * threads,
        sum_marks / number_of_replicas / number_of_replicas);

    /// Here we take max of two numbers:
    /// * (min_marks_per_task * threads) = the number of marks we request from the coordinator each time - there is no point to have segments smaller than one unit of work for a replica
    /// * (sum_marks / number_of_replicas^2) - we use consistent hashing for work distribution (including work stealing). If we have a really slow replica
    ///   everything except (1/number_of_replicas) portion of its work will be stolen by other replicas. And it owns (1/number_of_replicas) share of total number of marks.
    ///   Also important to note here that sum_marks is calculated after PK analysis, it means in particular that different segment sizes might be used for the
    ///   same table for different queries (it is intentional).
    ///
    /// Positive `mark_segment_size` means it is a user provided value, we have to preserve it.
    if (mark_segment_size == 0)
        mark_segment_size = std::max(min_marks_per_task * threads, sum_marks / number_of_replicas / number_of_replicas);

    /// Squeeze the value to the borders.
    mark_segment_size = std::clamp(mark_segment_size, borders.front(), borders.back());
    /// After we calculated a hopefully good value for segment_size let's just find the maximal border that is not bigger than the chosen value.
    for (auto border : borders | std::views::reverse)
    {
        if (mark_segment_size >= border)
        {
            LOG_TRACE(log, "Chosen segment size: {}", border);
            return border;
        }
    }

    UNREACHABLE();
}

size_t getMinMarksPerTask(size_t min_marks_per_task, const std::vector<DB::MergeTreeReadTaskInfoPtr> & per_part_infos)
{
    /// For each part the value of `min_marks_per_task` is capped by `sum_marks / (threads * total_query_nodes) / 2` (see calculateMinMarksPerTask()),
    /// unless `merge_tree_min_read_task_size` or `min_*_for_concurrent_read` settings are set too high. So, we can safely take the maximum of all parts.
    /// On the flip side, it is not safe to take the minimum, because e.g. for compact parts we don't know individual column sizes, so we use whole part size as approximation,
    /// and as the result we might end up with a very small `min_marks_per_task` that will cause too many requests to the coordinator.
    const auto max_across_parts = std::ranges::max(
        per_part_infos, [](const auto & lhs, const auto & rhs) { return lhs->min_marks_per_task < rhs->min_marks_per_task; });
    min_marks_per_task = std::max(min_marks_per_task, max_across_parts->min_marks_per_task);

    if (min_marks_per_task == 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero (likely because of weird interference of settings)");

    return min_marks_per_task;
}
}

namespace ProfileEvents
{
extern const Event ParallelReplicasReadMarks;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 parallel_replicas_mark_segment_size;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

MergeTreeReadPoolParallelReplicas::MergeTreeReadPoolParallelReplicas(
    ParallelReadingExtension extension_,
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
    , coordination_mode(CoordinationMode::Default)
{
    const size_t min_marks_per_task = getMinMarksPerTask(pool_settings.min_marks_for_concurrent_read, per_part_infos);
    min_marks_per_request = min_marks_per_task * pool_settings.threads;

    mark_segment_size = chooseSegmentSize(
        log,
        context_->getSettingsRef()[Setting::parallel_replicas_mark_segment_size],
        min_marks_per_task,
        pool_settings.threads,
        pool_settings.sum_marks,
        extension.getTotalNodesCount());
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t /*task_idx*/, MergeTreeReadTask * previous_task)
{
    while (true)
    {
        size_t part_idx = 0;
        size_t need_marks = 0;
        MarkRanges cut_ranges;

        if (!cutRangesToRead(part_idx, need_marks, cut_ranges))
            return nullptr;

        MarkRanges task_ranges;
        if (!ranges_refiner)
        {
            task_ranges = std::move(cut_ranges);
        }
        else
        {
            /// Marks dropped by the refiner do not count towards the task size: keep cutting
            /// more marks while the coordinator assignment continues with the same part.
            /// Refinement may block (e.g. building a projection index bitmap on the first use
            /// for the part), so it always happens outside of the mutex.
            while (true)
            {
                auto refined = refineReadRanges(*per_part_infos[part_idx], std::move(cut_ranges));
                for (const auto & range : refined)
                {
                    if (!task_ranges.empty() && task_ranges.back().end == range.begin)
                        task_ranges.back().end = range.end;
                    else
                        task_ranges.push_back(range);
                }

                size_t marks_collected = task_ranges.getNumberOfMarks();
                if (marks_collected >= need_marks)
                    break;

                MarkRanges more_ranges;
                if (!cutMoreRangesToRead(part_idx, need_marks - marks_collected, more_ranges))
                    break;

                cut_ranges = std::move(more_ranges);
            }
        }

        /// Everything was refined away, take the next portion (possibly of another part).
        if (task_ranges.empty())
            continue;

        /// Count only the marks that reach a reader: the ones dropped by the refiner are not read.
        ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, task_ranges.getNumberOfMarks());
        return createTask(per_part_infos[part_idx], std::move(task_ranges), previous_task);
    }
}

bool MergeTreeReadPoolParallelReplicas::cutRangesToRead(size_t & part_idx, size_t & need_marks, MarkRanges & ranges_to_read)
{
    std::lock_guard lock(mutex);

    if (no_more_tasks_available)
        return false;

    /// The following unfortunate scenario is possible:
    /// 1. Thread T1 found no `buffered_ranges` left to read and initiated a read request to the coordinator.
    /// 2. Thread T2 called `getTask` too and now waits for the mutex.
    /// 3. The coordinator handled the request from T1 and sent a response.
    ///    In this response it indicated that there are no more tasks left to read (`finish = true`).
    /// 4. While receiving the response we got an exception (e.g. network timeout).
    /// 5. Exception is propagated from `sendReadRequest` up the stack, but once T1 leaves `getTask`, T2 is able to grab the mutex.
    ///    T2 also finds no `buffered_ranges` and repeat the request to the coordinator.
    ///    Coordinator throws logical error: "Got request from replica N after ranges assignment has been completed for the replica."
    ///    To prevent this we set `failed_to_get_task` flag once we catch an exception from `sendReadRequest`.
    if (failed_to_get_task)
        return false;

    if (buffered_ranges.empty())
    {
        std::optional<ParallelReadResponse> response;
        try
        {
            response = extension.sendReadRequest(coordination_mode, min_marks_per_request);
        }
        catch (...)
        {
            failed_to_get_task = true;
            throw;
        }

        if (response)
        {
            LOG_DEBUG(log, "Got response: {}", response->describe());
            if (response->description.empty() || response->finish)
                no_more_tasks_available = true;
        }
        else
        {
            LOG_DEBUG(log, "Got no response");
            no_more_tasks_available = true;
        }
        if (no_more_tasks_available)
            return false;

        buffered_ranges = std::move(response->description);
    }

    if (buffered_ranges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No tasks to read. This is a bug");

    const auto & current_task = buffered_ranges.front();

    auto part_it
        = std::ranges::find_if(
            per_part_infos,
            [&current_task](const auto & part)
            {
                if (!part->data_part_info->isProjectionPart())
                    return part->data_part_info->getPartInfo() == current_task.info;

                chassert(part->parent_part && !current_task.projection_name.empty());
                return part->parent_part->info == current_task.info && current_task.projection_name == part->data_part_info->getPartName();
            });
    if (part_it == per_part_infos.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Assignment contains an unknown part (current_task: {})", current_task.describe());
    part_idx = std::distance(per_part_infos.begin(), part_it);

    /// Since DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK, the coordinator propagates per-part
    /// `min_marks_per_task` computed by the initiator after primary key analysis.
    /// Fall back to locally computed value for old initiators.
    need_marks = current_task.min_marks_per_task > 0 ? current_task.min_marks_per_task : (*part_it)->min_marks_per_task;

    cutFromCurrentTask(need_marks, ranges_to_read);
    return true;
}

bool MergeTreeReadPoolParallelReplicas::cutMoreRangesToRead(size_t part_idx, size_t need_marks, MarkRanges & ranges_to_read)
{
    std::lock_guard lock(mutex);

    /// Continue only while the coordinator assignment goes on with the same part: a read task
    /// must not span multiple parts. Do not request more ranges from the coordinator here.
    if (buffered_ranges.empty())
        return false;

    const auto & current_task = buffered_ranges.front();
    const auto & part = per_part_infos[part_idx];

    bool same_part = !part->data_part_info->isProjectionPart()
        ? part->data_part_info->getPartInfo() == current_task.info
        : (part->parent_part->info == current_task.info && current_task.projection_name == part->data_part_info->getPartName());

    if (!same_part)
        return false;

    cutFromCurrentTask(need_marks, ranges_to_read);
    return true;
}

void MergeTreeReadPoolParallelReplicas::cutFromCurrentTask(size_t need_marks, MarkRanges & ranges_to_read)
{
    auto & current_task = buffered_ranges.front();

    size_t current_sum_marks = 0;
    while (current_sum_marks < need_marks && !current_task.ranges.empty())
    {
        auto diff = need_marks - current_sum_marks;
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
}

}
