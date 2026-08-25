#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>

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

    while (!task.isFinished())
        chunks.push_back(task.read());

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();
    return res;
}

}
