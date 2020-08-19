#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <cassert>
#include <type_traits>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_SCHEDULE_TASK;
    }
}

namespace CurrentMetrics
{
    extern const Metric GlobalThread;
    extern const Metric GlobalThreadActive;
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl()
    : ThreadPoolImpl(getNumberOfPhysicalCPUCores())
{
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads_)
    : ThreadPoolImpl(max_threads_, max_threads_, max_threads_)
{
}

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_)
    : max_threads(max_threads_)
    , max_free_threads(max_free_threads_)
    , queue_size(queue_size_)
    , shutdown_on_exception(shutdown_on_exception_)
{
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setMaxThreads(size_t value)
{
    std::lock_guard lock(mutex);
    max_threads = value;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setMaxFreeThreads(size_t value)
{
    std::lock_guard lock(mutex);
    max_free_threads = value;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setQueueSize(size_t value)
{
    std::lock_guard lock(mutex);
    queue_size = value;
}


template <typename Thread>
template <typename ReturnType>
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds)
{
    auto on_error = [&]
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (first_exception)
            {
                std::exception_ptr exception;
                std::swap(exception, first_exception);
                std::rethrow_exception(exception);
            }
            throw DB::Exception("Cannot schedule a task", DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
        }
        else
            return false;
    };

    {
        std::unique_lock lock(mutex);

        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        if (wait_microseconds)  /// Check for optional. Condition is true if the optional is set and the value is zero.
        {
            if (!job_finished.wait_for(lock, std::chrono::microseconds(*wait_microseconds), pred))
                return on_error();
        }
        else
            job_finished.wait(lock, pred);

        if (shutdown)
            return on_error();

        jobs.emplace(std::move(job), priority);
        ++scheduled_jobs;

        if (threads.size() < std::min(max_threads, scheduled_jobs))
        {
            threads.emplace_front();
            try
            {
                threads.front() = Thread([this, it = threads.begin()] { worker(it); });
            }
            catch (...)
            {
                threads.pop_front();

                /// Remove the job and return error to caller.
                /// Note that if we have allocated at least one thread, we may continue
                /// (one thread is enough to process all jobs).
                /// But this condition indicate an error nevertheless and better to refuse.

                jobs.pop();
                --scheduled_jobs;
                return on_error();
            }
        }
    }
    new_job_or_shutdown.notify_one();
    return ReturnType(true);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrowOnError(Job job, int priority)
{
    scheduleImpl<void>(std::move(job), priority, std::nullopt);
}

template <typename Thread>
bool ThreadPoolImpl<Thread>::trySchedule(Job job, int priority, uint64_t wait_microseconds) noexcept
{
    return scheduleImpl<bool>(std::move(job), priority, wait_microseconds);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrow(Job job, int priority, uint64_t wait_microseconds)
{
    scheduleImpl<void>(std::move(job), priority, wait_microseconds);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::wait()
{
    {
        std::unique_lock lock(mutex);
        job_finished.wait(lock, [this] { return scheduled_jobs == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

template <typename Thread>
ThreadPoolImpl<Thread>::~ThreadPoolImpl()
{
    finalize();
}

template <typename Thread>
void ThreadPoolImpl<Thread>::finalize()
{
    {
        std::unique_lock lock(mutex);
        shutdown = true;
    }

    new_job_or_shutdown.notify_all();

    for (auto & thread : threads)
        thread.join();

    threads.clear();
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::active() const
{
    std::unique_lock lock(mutex);
    return scheduled_jobs;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::worker(typename std::list<Thread>::iterator thread_it)
{
    CurrentMetrics::Increment metric_all_threads(
        std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThread : CurrentMetrics::LocalThread);

    while (true)
    {
        Job job;
        bool need_shutdown = false;

        {
            std::unique_lock lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = jobs.top().job;
                jobs.pop();
            }
            else
            {
                /// shutdown is true, simply finish the thread.
                return;
            }
        }

        if (!need_shutdown)
        {
            try
            {
                CurrentMetrics::Increment metric_active_threads(
                    std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThreadActive : CurrentMetrics::LocalThreadActive);

                job();
            }
            catch (...)
            {
                {
                    std::unique_lock lock(mutex);
                    if (!first_exception)
                        first_exception = std::current_exception(); // NOLINT
                    if (shutdown_on_exception)
                        shutdown = true;
                    --scheduled_jobs;
                }

                job_finished.notify_all();
                new_job_or_shutdown.notify_all();
                return;
            }
        }

        {
            std::unique_lock lock(mutex);
            --scheduled_jobs;

            if (threads.size() > scheduled_jobs + max_free_threads)
            {
                thread_it->detach();
                threads.erase(thread_it);
                job_finished.notify_all();
                return;
            }
        }

        job_finished.notify_all();
    }
}


template class ThreadPoolImpl<std::thread>;
template class ThreadPoolImpl<ThreadFromGlobalPool>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;

void GlobalThreadPool::initialize(size_t max_threads)
{
    assert(!the_instance);

    the_instance.reset(new GlobalThreadPool(max_threads,
        1000 /*max_free_threads*/, 10000 /*max_queue_size*/,
        false /*shutdown_on_exception*/));
}

GlobalThreadPool & GlobalThreadPool::instance()
{
    if (!the_instance)
    {
        // Allow implicit initialization. This is needed for old code that is
        // impractical to redo now, especially Arcadia users and unit tests.
        initialize();
    }

    return *the_instance;
}
