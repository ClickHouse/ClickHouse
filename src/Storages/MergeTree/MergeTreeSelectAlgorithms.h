#pragma once

#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class IMergeTreeReadPool;

class IMergeTreeSelectAlgorithm : private boost::noncopyable
{
public:
    using BlockSizeParams = MergeTreeReadTask::BlockSizeParams;
    using BlockAndProgress = MergeTreeReadTask::BlockAndProgress;

    virtual ~IMergeTreeSelectAlgorithm() = default;

    virtual String getName() const = 0;
    virtual bool needNewTask(const MergeTreeReadTask & task) const = 0;

    virtual MergeTreeReadTaskPtr getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) = 0;
    virtual BlockAndProgress readFromTask(MergeTreeReadTask & task) = 0;
};

using MergeTreeSelectAlgorithmPtr = std::unique_ptr<IMergeTreeSelectAlgorithm>;

class MergeTreeThreadSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeThreadSelectAlgorithm(size_t thread_idx_) : thread_idx(thread_idx_) {}

    String getName() const override { return "Thread"; }
    bool needNewTask(const MergeTreeReadTask & task) const override { return task.isFinished(); }

    MergeTreeReadTaskPtr getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override { return pool.getTask(thread_idx, previous_task); }
    BlockAndProgress readFromTask(MergeTreeReadTask & task) override { return task.read(); }

private:
    const size_t thread_idx;
};

class MergeTreeInOrderSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeInOrderSelectAlgorithm(size_t part_idx_) : part_idx(part_idx_) {}

    String getName() const override { return "InOrder"; }
    bool needNewTask(const MergeTreeReadTask & task) const override { return task.isFinished(); }

    MergeTreeReadTaskPtr getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTask::BlockAndProgress readFromTask(MergeTreeReadTask & task) override { return task.read(); }

private:
    const size_t part_idx;
};

class MergeTreeInReverseOrderSelectAlgorithm : public IMergeTreeSelectAlgorithm
{
public:
    explicit MergeTreeInReverseOrderSelectAlgorithm(size_t part_idx_) : part_idx(part_idx_) {}

    String getName() const override { return "InReverseOrder"; }
    bool needNewTask(const MergeTreeReadTask & task) const override { return chunks.empty() && task.isFinished(); }

    MergeTreeReadTaskPtr getNewTask(IMergeTreeReadPool & pool, MergeTreeReadTask * previous_task) override;
    BlockAndProgress readFromTask(MergeTreeReadTask & task) override;

private:
    const size_t part_idx;
    std::vector<BlockAndProgress> chunks;
};

}
