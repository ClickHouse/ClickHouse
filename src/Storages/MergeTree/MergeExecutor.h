#pragma once

#include <Poco/Event.h>

#include "DataStreams/IBlockInputStream.h"
#include "DataStreams/IBlockOutputStream.h"
#include "Storages/MergeTree/MergeAlgorithm.h"

#include <Common/ThreadPool.h>

#include <functional>

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
    using IsCancelledCallback = std::function<bool()>;
    using UpdateForBlockCallback = std::function<void(const Block &)>;

    MergeTask(BlockInputStreamPtr merged_stream_, BlockOutputStreamPtr to_,
        IsCancelledCallback is_cancelled_, UpdateForBlockCallback update_for_block_)
        : is_cancelled(std::move(is_cancelled_))
        , update_for_block(std::move(update_for_block_))
        , merged_stream(merged_stream_)
        , to(to_)
    {

    }

    void wait()
    {
        is_done.wait();

        if (exception)
        {
            std::rethrow_exception(exception);
        }
    }

    void signalDone() 
    {
        is_done.set();
    }

    void setException(std::exception_ptr exception_)
    {
        exception = exception_;
        is_done.set();
    }

    /// Returns true if need execute again
    bool execute()
    {
        switch (state)
        {
            case MergeTaskState::NEED_PREPARE:
            {
                merged_stream->readPrefix();
                to->writePrefix();

                state = MergeTaskState::NEED_EXECUTE;
                return true;
            }
            case MergeTaskState::NEED_EXECUTE:
            {
                if (executeForBlock())
                    return true;

                state = MergeTaskState::NEED_FINISH;
                [[fallthrough]];
            }
            case MergeTaskState::NEED_FINISH:
            {
                merged_stream->readSuffix();
                merged_stream.reset();
                return false;
            }
        }
    }

    bool operator< (const MergeTask & rhs) const
    {
        return priority < rhs.priority;
    }

private:

    /// Returns true if need to execute again
    bool executeForBlock()
    {
        Block block;
        if (!is_cancelled() && (block = merged_stream->read()))
        {
            to->write(block);
            update_for_block(block);
            return true;
        }
        return false;
    }


    IsCancelledCallback is_cancelled;
    UpdateForBlockCallback update_for_block;


    MergeTaskState state;
    BlockInputStreamPtr merged_stream;
    BlockOutputStreamPtr to;
    std::exception_ptr exception;
    Poco::Event is_done;

    int priority = 0;
};


using MergeTaskPtr = std::shared_ptr<MergeTask>;


class MergeTaskPtrComparator
{
public:
    bool operator()(MergeTaskPtr lhs, MergeTaskPtr rhs) 
    {
        return (*lhs) < (*rhs);
    }
};


class InlineMergeExecutor
{
public:
    static void schedule(MergeTaskPtr task)
    {
        while (task->execute()) {}
        task->signalDone();
    }
};



class ConcurrentMergeExecutor
{
public:

    explicit ConcurrentMergeExecutor(size_t size_) : pool(size_)
    {
        for (size_t i = 0; i < size_; ++i) 
            pool.scheduleOrThrow([this]() { threadFunction(); } );
    }

    ~ConcurrentMergeExecutor()
    {
        {
            std::lock_guard lock(mutex);
            force_stop = true;
            has_tasks.notify_all();
        }
        pool.wait();
    }

    void schedule(MergeTaskPtr task)
    {
        std::lock_guard lock(mutex);
        tasks.push(std::move(task));
    }

private:

    void threadFunction()
    {
        while (true)
        {
            MergeTaskPtr task;
            {
                std::unique_lock lock(mutex);
                has_tasks.wait(lock, [&]() { return force_stop || !tasks.empty(); });

                if (force_stop)
                    return;

                task = tasks.top();
                tasks.pop();
            }

            try
            {
                if (task->execute())
                {
                    std::lock_guard lock(mutex);
                    tasks.push(task);
                    continue;
                }
                task->signalDone();
            } catch (...)
            {
                task->setException(std::current_exception());
            }
        }
    }

    ThreadPool pool;

    bool force_stop{false};

    std::mutex mutex;
    std::condition_variable has_tasks;
    std::priority_queue<MergeTaskPtr, std::vector<MergeTaskPtr>, MergeTaskPtrComparator> tasks;
};

}
