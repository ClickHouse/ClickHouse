#include <Storages/MergeTree/MergeTreeIndexBuildContext.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

namespace DB
{

MergeTreeIndexBuildContext::MergeTreeIndexBuildContext(
    RangesByIndex read_ranges_,
    ProjectionRangesByIndex projection_read_ranges_,
    MergeTreeIndexReadResultPoolPtr index_reader_pool_,
    PartRemainingMarks part_remaining_marks_,
    IndexReadTasks index_read_tasks_)
    : read_ranges(std::move(read_ranges_))
    , projection_read_ranges(std::move(projection_read_ranges_))
    , index_reader_pool(std::move(index_reader_pool_))
    , part_remaining_marks(std::move(part_remaining_marks_))
    , index_read_tasks(std::move(index_read_tasks_))
{
}

MergeTreeIndexReadResultPtr MergeTreeIndexBuildContext::getPreparedIndexReadResult(const MergeTreeReadTaskInfo & task_info, const MarkRanges & ranges) const
{
    if (!index_reader_pool)
        return nullptr;

    const auto & part_ranges = read_ranges.at(task_info.part_index_in_query);
    auto it = projection_read_ranges.find(task_info.part_index_in_query);
    static RangesInDataParts empty_parts_ranges;
    const auto & projection_parts_ranges = it != projection_read_ranges.end() ? it->second : empty_parts_ranges;
    auto & remaining_marks = part_remaining_marks.at(task_info.part_index_in_query).value;
    auto index_read_result = index_reader_pool->getOrBuildIndexReadResult(part_ranges, projection_parts_ranges);

    /// Atomically subtract the number of marks this task will read from the total remaining marks. If the
    /// remaining marks after subtraction reach zero, this is the last task for the part, and we can trigger
    /// cleanup of any per-part cached resources (e.g., skip index read result or projection index bitmaps).
    size_t task_marks = ranges.getNumberOfMarks();
    bool part_last_task = remaining_marks.fetch_sub(task_marks, std::memory_order_acq_rel) == task_marks;

    if (part_last_task)
        index_reader_pool->clear(task_info.data_part);

    return index_read_result;
}

}
