#pragma once

#include <functional>
#include <memory>

#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>

#include <IO/WriteBufferFromS3.h>


namespace DB
{

class WriteBufferFromS3Executor : public WriteBufferFromS3::IExecutor
{
    std::unique_ptr<ThreadPool> thread_pool;

public:
    WriteBufferFromS3Executor(size_t max_threads)
    {
        if (max_threads > 1)
            thread_pool = std::make_unique<ThreadPool>(max_threads);
    }

    void scheduleOrThrowOnError(std::function<void()> job) override
    {
        if (thread_pool)
        {
            auto thread_group = CurrentThread::getGroup();

            auto named_job = [thread_group, job]()
            {
                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT(
                        if (thread_group)
                            CurrentThread::detachQueryIfNotDetached();
                );

                job();
            };

            thread_pool->scheduleOrThrowOnError(named_job);
        }
        else
            job();
    }

    void wait() override
    {
        if (thread_pool)
            thread_pool->wait();
    }

    size_t active() const override
    {
        if (thread_pool)
            return thread_pool->active();
        else
            return 0;
    }

    ~WriteBufferFromS3Executor() override
    {
    }
};

}
