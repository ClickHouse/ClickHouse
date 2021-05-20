#include "Storages/MergeTree/MergeExecutor.h"

namespace DB
{

ConcurrentMergeExecutor::ConcurrentMergeExecutor(size_t size_) : pool(size_)
{
    for (size_t i = 0; i < size_; ++i) 
        pool.scheduleOrThrow([this]() { threadFunction(); } );
}


ConcurrentMergeExecutor::~ConcurrentMergeExecutor()
{
    {
        std::lock_guard lock(mutex);
        force_stop = true;
        has_tasks.notify_all();
    }
    pool.wait();
}


void ConcurrentMergeExecutor::schedule(MergeTaskPtr task)
{
    std::lock_guard lock(mutex);
    tasks.push(std::move(task));
    has_tasks.notify_one();
}


void ConcurrentMergeExecutor::threadFunction()
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


}
