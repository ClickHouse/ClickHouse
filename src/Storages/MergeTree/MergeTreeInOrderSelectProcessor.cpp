#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

bool MergeTreeInOrderSelectProcessor::getNewTaskImpl()
try
{
    if (all_mark_ranges.empty())
        return false;

    if (!reader)
        initializeReaders();

    MarkRanges mark_ranges_for_task;
    /// If we need to read few rows, set one range per task to reduce number of read data.
    if (has_limit_below_one_block)
    {
        mark_ranges_for_task = { std::move(all_mark_ranges.front()) };
        all_mark_ranges.pop_front();
    }
    else
    {
        mark_ranges_for_task = std::move(all_mark_ranges);
        all_mark_ranges.clear();
    }

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, ordered_names, column_name_set, task_columns.columns,
        task_columns.pre_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        task_columns.should_reorder, std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}

}
