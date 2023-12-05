#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include "Storages/MergeTree/RangesInDataPart.h"
#include <Storages/MergeTree/IntersectionsIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

bool MergeTreeInOrderSelectAlgorithm::getNewTaskImpl()
try
{
    if (all_mark_ranges.empty())
        return false;

    if (!reader)
        initializeReaders();

    MarkRanges mark_ranges_for_task;

    if (!pool)
    {
        /// If we need to read few rows, set one range per task to reduce number of read data.
        if (has_limit_below_one_block)
        {
            mark_ranges_for_task = MarkRanges{};
            mark_ranges_for_task.emplace_front(std::move(all_mark_ranges.front()));
            all_mark_ranges.pop_front();
        }
        else
        {
            mark_ranges_for_task = std::move(all_mark_ranges);
            all_mark_ranges.clear();
        }
    }
    else
    {
        auto description = RangesInDataPartDescription{
            .info = data_part->info,
            /// We just ignore all the distribution done before
            /// Everything will be done on coordinator side
            .ranges = {},
        };

        mark_ranges_for_task = pool->getNewTask(description);

        if (mark_ranges_for_task.empty())
            return false;
    }

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part,
        alter_conversions,
        mark_ranges_for_task,
        part_index_in_query,
        column_name_set,
        task_columns,
        std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}

}
