#include "config.h"

#if USE_AWS_S3

#include <IO/WriteBufferFromS3TaskTracker.h>

namespace DB
{

WriteBufferFromS3::TaskTracker::TaskTracker(ThreadPoolCallbackRunner<void> scheduler_, size_t max_tasks_inflight_)
    : is_async(bool(scheduler_))
    , scheduler(scheduler_ ? std::move(scheduler_) : syncRunner())
    , max_tasks_inflight(max_tasks_inflight_)
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

size_t WriteBufferFromS3::TaskTracker::consumeReady()
{
    LOG_TEST(log, "consumeReady, in queue {}", futures.size());

    size_t consumed = 0;

    {
        std::unique_lock lock(mutex);

        for (auto it : finished_futures)
        {
            try
            {
                it->get();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                throw;
            }

            futures.erase(it);
        }

        consumed = finished_futures.size();
        finished_futures.clear();
    }

    LOG_TEST(log, "consumeReady ended, in queue {}", futures.size());
    return consumed;
}

void WriteBufferFromS3::TaskTracker::waitAll()
{
    LOG_TEST(log, "waitAll, in queue {}", futures.size());

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

    /// no concurrent tasks, no mutex required
    finished_futures.clear();
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

    /// no concurrent tasks, no mutex required
    finished_futures.clear();

    LOG_TEST(log, "safeWaitAll ended, get in queue {}", futures.size());
}

void WriteBufferFromS3::TaskTracker::waitAny()
{
    LOG_TEST(log, "waitAny, in queue {}", futures.size());

    while (futures.size() > 0 && consumeReady() == 0)
    {
            std::unique_lock lock(mutex);
            cond_var.wait(lock, [&] () { return finished_futures.size() > 0; });
    }

    LOG_TEST(log, "waitAny ended, in queue {}", futures.size());
}

void WriteBufferFromS3::TaskTracker::add(Callback && func)
{
    futures.emplace_back();
    auto future_placeholder = std::prev(futures.end());
    FinishedList pre_allocated_finished {future_placeholder};

    Callback func_with_notification = [&, func=std::move(func), pre_allocated_finished=std::move(pre_allocated_finished)] () mutable
    {
        SCOPE_EXIT({
            DENY_ALLOCATIONS_IN_SCOPE;

            std::unique_lock lock(mutex);
            finished_futures.splice(finished_futures.end(), pre_allocated_finished, pre_allocated_finished.begin());
            cond_var.notify_one();
        });

        func();
    };

    *future_placeholder = scheduler(std::move(func_with_notification), Priority{});

    LOG_TEST(log, "add ended, in queue {}, limit {}", futures.size(), max_tasks_inflight);

    waitInFlight();
}

void WriteBufferFromS3::TaskTracker::waitInFlight()
{
    if (!max_tasks_inflight)
        return;

    LOG_TEST(log, "waitInFlight, in queue {}", futures.size());

    while (futures.size() >= max_tasks_inflight)
    {
        waitAny();
    }

    LOG_TEST(log, "waitInFlight ended, in queue {}", futures.size());
}

bool WriteBufferFromS3::TaskTracker::isAsync() const
{
    return is_async;
}

}

#endif
