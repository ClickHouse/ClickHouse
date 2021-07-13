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
    pool.scheduleOrThrow([this, task]() {
        if (task->execute()) {
            schedule(task);
        }
    }, task->getPriority());
}

}
