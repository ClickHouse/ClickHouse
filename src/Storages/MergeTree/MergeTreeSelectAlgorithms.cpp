#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeThreadSelectAlgorithm::TaskResult MergeTreeThreadSelectAlgorithm::getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task)
{
    TaskResult res;
    res.first = pool.getTask(thread_idx, previous_task);
    res.second = !!res.first;
    return res;
}

MergeTreeReadTask::BlockAndProgress MergeTreeThreadSelectAlgorithm::readFromTask(MergeTreeReadTask * task, const MergeTreeReadTask::BlockSizeParams & params)
{
    if (!task)
        return {};

    return task->read(params);
}

IMergeTreeSelectAlgorithm::TaskResult MergeTreeInOrderSelectAlgorithm::getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task)
{
    if (!pool.preservesOrderOfRanges())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeInOrderSelectAlgorithm requires read pool that preserves order of ranges, got: {}", pool.getName());

    TaskResult res;
    res.first = pool.getTask(part_idx, previous_task);
    res.second = !!res.first;
    return res;
}

MergeTreeReadTask::BlockAndProgress MergeTreeInOrderSelectAlgorithm::readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params)
{
    if (!task)
        return {};

    return task->read(params);
}

IMergeTreeSelectAlgorithm::TaskResult MergeTreeInReverseOrderSelectAlgorithm::getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task)
{
    if (!pool.preservesOrderOfRanges())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MergeTreeInReverseOrderSelectAlgorithm requires read pool that preserves order of ranges, got: {}", pool.getName());

    TaskResult res;
    res.first = pool.getTask(part_idx, previous_task);
    /// We may have some chunks to return in buffer.
    /// Set continue_reading to true but actually don't create a new task.
    res.second = !!res.first || !chunks.empty();
    return res;
}

MergeTreeReadTask::BlockAndProgress MergeTreeInReverseOrderSelectAlgorithm::readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params)
{
    MergeTreeReadTask::BlockAndProgress res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    if (!task)
        return {};

    while (!task->isFinished())
        chunks.push_back(task->read(params));

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();
    return res;
}

}
