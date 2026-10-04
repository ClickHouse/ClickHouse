#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>

#include <Storages/MergeTree/MergeTreeReadPoolProjectionIndex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadTaskPtr MergeTreeInOrderSelectAlgorithm::getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task)
{
    if (!pool.preservesOrderOfRanges())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeInOrderSelectAlgorithm requires read pool that preserves order of ranges, got: {}", pool.getName());

    return pool.getTask(part_idx, previous_task);
}

MergeTreeReadTaskPtr MergeTreeInReverseOrderSelectAlgorithm::getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task)
{
    if (!pool.preservesOrderOfRanges())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeInReverseOrderSelectAlgorithm requires read pool that preserves order of ranges, got: {}", pool.getName());

    if (!chunks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot get new task for reading in reverse order because there are {} buffered chunks", chunks.size());

    return pool.getTask(part_idx, previous_task);
}

MergeTreeReadTask::BlockAndProgress
MergeTreeInReverseOrderSelectAlgorithm::readFromTask(MergeTreeReadTask & task)
{
    MergeTreeReadTask::BlockAndProgress res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    /// The whole task is read here, so the rows it read are reported here. A chunk that is still
    /// buffered when the query stops early is never emitted, so progress left attached to it would
    /// never be reported at all.
    size_t num_read_rows = 0;
    size_t num_read_bytes = 0;

    while (!task.isFinished())
    {
        auto & chunk = chunks.emplace_back(task.read());
        num_read_rows += std::exchange(chunk.num_read_rows, 0);
        num_read_bytes += std::exchange(chunk.num_read_bytes, 0);
    }

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();
    res.num_read_rows = num_read_rows;
    res.num_read_bytes = num_read_bytes;
    return res;
}

MergeTreeReadTaskPtr
MergeTreeProjectionIndexSelectAlgorithm::getNewTask(IMergeTreeReadPool & /* pool */, MergeTreeReadTask * /* previous_task */)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeProjectionIndexSelectAlgorithm cannot be used to generate new tasks");
}

}
