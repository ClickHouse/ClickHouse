#include "config.h"

#if USE_AWS_S3

#include <IO/WriteBufferFromS3TaskTracker.h>

namespace DB
{

WriteBufferFromS3::TaskTracker::TaskTracker(ThreadPoolCallbackRunner<void> scheduler_)
    : is_async(bool(scheduler_))
    , scheduler(scheduler_ ? std::move(scheduler_) : syncRunner())
{}

WriteBufferFromS3::TaskTracker::~TaskTracker()
{
    safeWaitAll();
}

ThreadPoolCallbackRunner<void> WriteBufferFromS3::TaskTracker::syncRunner()
{
    return [](Callback && callback, int64_t) mutable -> std::future<void>
    {
        auto package = std::packaged_task<void()>(std::move(callback));
        /// No exceptions are propagated, exceptions are packed to future
        package();
        return  package.get_future();
    };
}

void WriteBufferFromS3::TaskTracker::getReady()
{
    LOG_TEST(log, "getReady, in queue {}", futures.size());

    /// Exceptions are propagated
    auto it = futures.begin();
    while (it != futures.end())
    {
        chassert(it->valid());
        if (it->wait_for(std::chrono::seconds(0)) != std::future_status::ready)
        {
            ++it;
            continue;
        }

        try
        {
            it->get();
        } catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }

        it = futures.erase(it);
    }

    LOG_TEST(log, "getReady ended, in queue {}", futures.size());
}

void WriteBufferFromS3::TaskTracker::getAll()
{
    LOG_TEST(log, "getAll, in queue {}", futures.size());

    /// Exceptions are propagated
    for (auto & future : futures)
    {
        try
        {
            future.get();
        } catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }
    futures.clear();
}

void WriteBufferFromS3::TaskTracker::safeWaitAll()
{
    LOG_TEST(log, "safeWaitAll, wait in queue {}", futures.size());

    /// Exceptions are not propagated
    for (auto & future : futures)
    {
        LOG_TEST(log, "safeWaitAll, wait future");

        if (future.valid())
            future.wait();
    }

    LOG_TEST(log, "safeWaitAll, get in queue {}", futures.size());

    for (auto & future : futures)
    {
        if (future.valid())
        {
            try
            {
                future.get();
            } catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
    futures.clear();
    LOG_TEST(log, "safeWaitAll ended, get in queue {}", futures.size());
}

void WriteBufferFromS3::TaskTracker::add(Callback && func)
{
    LOG_TEST(log, "add, in queue {}", futures.size());

    auto future = scheduler(std::move(func), 0);
    auto exit_scope = scope_guard(
        [&future]()
        {
            future.wait();
        }
    );

    futures.push_back(std::move(future));

    exit_scope.release();
    LOG_TEST(log, "add ended, in queue {}", futures.size());
}

bool WriteBufferFromS3::TaskTracker::isAsync() const
{
    return is_async;
}

}

#endif
