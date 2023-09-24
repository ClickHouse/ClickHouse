#pragma once

#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class IMergeTreeReadPool;

class IMergeTreeSelectAlgorithm : private boost::noncopyable
{
public:
    /// The pair of {task, continue_reading}.
    using TaskResult = std::pair<MergeTreeReadTaskPtr, bool>;
    using BlockSizeParams = MergeTreeReadTask::BlockSizeParams;

    virtual ~IMergeTreeSelectAlgorithm() = default;

    virtual String getName() const = 0;
    virtual TaskResult getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) = 0;
    virtual MergeTreeReadTask::BlockAndProgress readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params) = 0;
};

using MergeTreeSelectAlgorithmPtr = std::unique_ptr<IMergeTreeSelectAlgorithm>;

class MergeTreeThreadSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeThreadSelectAlgorithm(size_t thread_idx_) : thread_idx(thread_idx_) {}
    String getName() const override { return "Thread"; }
    TaskResult getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTask::BlockAndProgress readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params) override;

private:
    const size_t thread_idx;
};

class MergeTreeInOrderSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeInOrderSelectAlgorithm(size_t part_idx_) : part_idx(part_idx_) {}
    String getName() const override { return "InOrder"; }
    TaskResult getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTask::BlockAndProgress readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params) override;

private:
    const size_t part_idx;
};

class MergeTreeInReverseOrderSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeInReverseOrderSelectAlgorithm(size_t part_idx_) : part_idx(part_idx_) {}
    String getName() const override { return "InReverseOrder"; }
    TaskResult getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTask::BlockAndProgress readFromTask(MergeTreeReadTask * task, const BlockSizeParams & params) override;

private:
    const size_t part_idx;
    std::vector<MergeTreeReadTask::BlockAndProgress> chunks;
};

}
