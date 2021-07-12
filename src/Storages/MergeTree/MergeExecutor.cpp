#include <Storages/MergeTree/MergeExecutor.h>
#include <common/logger_useful.h>

namespace DB
{

MergeExecutor::MergeExecutor(size_t size_) : pool(size_)
{
    for (size_t i = 0; i < size_; ++i)
        pool.scheduleOrThrow([this]() { threadFunction(); } );
}


void MergeExecutor::wait()
{
    {
        std::lock_guard lock(mutex);
        force_stop = true;
        has_tasks.notify_all();
    }
    pool.wait();
}


MergeExecutor::~MergeExecutor()
{
    wait();
}


void MergeExecutor::schedule(BackgroundTaskPtr task)
{
    std::lock_guard lock(mutex);
    tasks.push(std::move(task));
    has_tasks.notify_one();
}


void MergeExecutor::threadFunction()
{
    while (true)
    {
        BackgroundTaskPtr task;
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
            task = {};
        } catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    }


}
