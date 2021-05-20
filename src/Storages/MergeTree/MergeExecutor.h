#pragma once

#include "DataStreams/IBlockInputStream.h"
#include "DataStreams/IBlockOutputStream.h"

#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <common/logger_useful.h>

#include <functional>
#include <iostream>
#include <future>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

enum class MergeTaskState : uint8_t
{
    NEED_PREPARE,
    NEED_EXECUTE,
    NEED_FINISH
};

class MergeTask
{
public:
    using PrepareCallback = std::function<void()>;
    using ExecuteForBlockCallback = std::function<bool()>;
    using FinalizeCallback = std::function<void()>;


    MergeTask(PrepareCallback prepare_, ExecuteForBlockCallback execute_for_block_, FinalizeCallback finalize_, UInt64 priority_)
        : priority(priority_)
        , prepare(std::move(prepare_))
        , execute_for_block(std::move(execute_for_block_))
        , finalize(std::move(finalize_))
    {}

    void wait() { is_done.get_future().wait(); }
    void signalDone() { is_done.set_value(); }
    void setException(std::exception_ptr exception_) { is_done.set_exception(exception_); }

    /// Returns true if is is needed to execute again
    bool execute()
    {
        switch (state)
        {
            case MergeTaskState::NEED_PREPARE:
            {
                prepare();
                state = MergeTaskState::NEED_EXECUTE;
                return true;
            }
            case MergeTaskState::NEED_EXECUTE:
            {
                if (execute_for_block())
                    return true;

                state = MergeTaskState::NEED_FINISH;
                return true;
            }
            case MergeTaskState::NEED_FINISH:
            {
                finalize();
                return false;
            }
        }
        return false;
    }

    bool operator> (const MergeTask & rhs) const
    {
        return priority > rhs.priority;
    }

    UInt64 getPriority() const { return priority; }

private:
    using Event = std::promise<void>;
    Event is_done;

    UInt64 priority = 0;

    MergeTaskState state{MergeTaskState::NEED_PREPARE};

    PrepareCallback prepare;
    ExecuteForBlockCallback execute_for_block;
    FinalizeCallback finalize; 
};

using MergeTaskPtr = std::shared_ptr<MergeTask>;

class IMergeExecutor
{
public:
    virtual void schedule(MergeTaskPtr task) = 0;
    virtual ~IMergeExecutor() = default;
};


class InlineMergeExecutor : public IMergeExecutor
{
public:
    void schedule(MergeTaskPtr task) override
    {
        while (task->execute()) {}
        task->signalDone();
    }
};

/*
    Will execute merge block by block.
*/
class ConcurrentMergeExecutor : public IMergeExecutor
{
public:
    explicit ConcurrentMergeExecutor(size_t size_);
    ~ConcurrentMergeExecutor() override;

    void schedule(MergeTaskPtr task) override;

private:
    void threadFunction();

    ThreadPool pool;
    bool force_stop{false};
    std::mutex mutex;
    std::condition_variable has_tasks;
    /// https://en.cppreference.com/w/cpp/memory/shared_ptr/operator_cmp
    std::priority_queue<MergeTaskPtr, std::vector<MergeTaskPtr>, std::greater<MergeTaskPtr>> tasks;
};

}
