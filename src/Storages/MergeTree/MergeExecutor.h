#pragma once

#include <queue>


#include <Storages/MergeTree/MergeTask.h>

namespace DB
{

using BackgroundTaskPtr = std::shared_ptr<BackgroundTask>;


class MergeExecutor
{
public:

    explicit MergeExecutor(size_t size_);
    ~MergeExecutor();

    void schedule(BackgroundTaskPtr task);

    void wait();

    void setMaxThreads(size_t value)
    {
        pool.setMaxThreads(value);
    }


    void setQueueSize(size_t value)
    {
        pool.setQueueSize(value);
    }


    void setMaxFreeThreads(size_t value)
    {
        pool.setMaxFreeThreads(value);
    }

private:

    void threadFunction();

    ThreadPool pool;
    bool force_stop{false};
    std::mutex mutex;
    std::condition_variable has_tasks;

    std::priority_queue<BackgroundTaskPtr, std::vector<BackgroundTaskPtr>, std::greater<BackgroundTaskPtr>> tasks;

};


}
