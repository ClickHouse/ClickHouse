#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>
#include "Storages/MergeTree/MergeTreeBaseSelectProcessor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

bool MergeTreeReverseSelectAlgorithm::getNewTaskImpl()
try
{
    if (pool)
        return getNewTaskParallelReplicas();
    else
        return getNewTaskOrdinaryReading();
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}


bool MergeTreeReverseSelectAlgorithm::getNewTaskOrdinaryReading()
{
    if (chunks.empty() && all_mark_ranges.empty())
        return false;

    /// We have some blocks to return in buffer.
    /// Return true to continue reading, but actually don't create a task.
    if (all_mark_ranges.empty())
        return true;

    if (!reader)
        initializeReaders();

    /// Read ranges from right to left.
    MarkRanges mark_ranges_for_task{std::move(all_mark_ranges.back())};
    all_mark_ranges.pop_back();

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, column_name_set,
        task_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        std::move(size_predictor));

    return true;

}

bool MergeTreeReverseSelectAlgorithm::getNewTaskParallelReplicas()
{
    if (chunks.empty() && no_more_tasks)
        return false;

    /// We have some blocks to return in buffer.
    /// Return true to continue reading, but actually don't create a task.
    if (no_more_tasks)
        return true;

    if (!reader)
        initializeReaders();

    auto description = RangesInDataPartDescription{
        .info = data_part->info,
        /// We just ignore all the distribution done before
        /// Everything will be done on coordinator side
        .ranges = {},
    };

    auto mark_ranges_for_task = pool->getNewTask(description);
    if (mark_ranges_for_task.empty())
    {
        /// If we have chunks in buffer - return true to continue reading from them
        return !chunks.empty();
    }

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, column_name_set,
        task_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        std::move(size_predictor));

    return true;
}

MergeTreeReverseSelectAlgorithm::BlockAndProgress MergeTreeReverseSelectAlgorithm::readFromPart()
{
    BlockAndProgress res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    while (!task->isFinished())
    {
        auto chunk = readFromPartImpl();
        chunks.push_back(std::move(chunk));
    }

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();

    return res;
}

}
