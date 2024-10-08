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

    LOG_DEBUG(
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
            LOG_DEBUG(log, "Chosen segment size: {}", border);
            return border;
        }
    }

    UNREACHABLE();
}

size_t getMinMarksPerTask(size_t min_marks_per_task, const std::vector<DB::MergeTreeReadTaskInfoPtr> & per_part_infos)
{
    for (const auto & info : per_part_infos)
        min_marks_per_task = std::max(min_marks_per_task, info->min_marks_per_task);

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
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
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
        context_)
    , extension(std::move(extension_))
    , coordination_mode(CoordinationMode::Default)
    , min_marks_per_task(getMinMarksPerTask(pool_settings.min_marks_for_concurrent_read, per_part_infos))
    , mark_segment_size(chooseSegmentSize(
          log,
          context_->getSettingsRef()[Setting::parallel_replicas_mark_segment_size],
          min_marks_per_task,
          pool_settings.threads,
          pool_settings.sum_marks,
          extension.getTotalNodesCount()))
{
    extension.sendInitialRequest(coordination_mode, parts_ranges, mark_segment_size);
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t /*task_idx*/, MergeTreeReadTask * previous_task)
{
    std::lock_guard lock(mutex);

    if (no_more_tasks_available)
        return nullptr;

    if (buffered_ranges.empty())
    {
        auto result = extension.sendReadRequest(
            coordination_mode,
            min_marks_per_task * pool_settings.threads,
            /// For Default coordination mode we don't need to pass part names.
            RangesInDataPartsDescription{});

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
    while (current_sum_marks < min_marks_per_task && !current_task.ranges.empty())
    {
        auto diff = min_marks_per_task - current_sum_marks;
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

    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadMarks, current_sum_marks);
    return createTask(per_part_infos[part_idx], std::move(ranges_to_read), previous_task);
}

}
