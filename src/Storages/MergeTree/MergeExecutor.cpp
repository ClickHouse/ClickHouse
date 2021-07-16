#include <Storages/MergeTree/MergeExecutor.h>
#include <common/logger_useful.h>

namespace DB
{

MergeExecutor::MergeExecutor(size_t size_) : pool(size_)
{
}


void MergeExecutor::wait()
{
    pool.wait();
}


MergeExecutor::~MergeExecutor()
{
    wait();
}


void MergeExecutor::schedule(BackgroundTaskPtr task)
{
    const auto priority = task->getPriority();
    pool.scheduleOrThrow([this, task]() {
        try
        {
            if (task->execute()) {
                schedule(task);
            }
        } catch (...)
        {
            // no-op
        }
    }, priority);
}

}
