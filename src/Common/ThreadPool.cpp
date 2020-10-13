#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <cassert>
#include <type_traits>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Core/UUID.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_SCHEDULE_TASK;
        extern const int LOGICAL_ERROR;
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
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_, bool only_log_exceptions_, const CurrentMetrics::Metric & custom_job_metric_)
    : max_threads(max_threads_)
    , max_free_threads(max_free_threads_)
    , queue_size(queue_size_)
    , shutdown_on_exception(shutdown_on_exception_)
    , only_log_exceptions(only_log_exceptions_)
    , custom_job_metric(custom_job_metric_)
{
    if (only_log_exceptions && shutdown_on_exception)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Inconsistent flags for thread pool, both only_log_exceptions and shutdown_on_exception specified");
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
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds, const JobGroupID & job_group_id)
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

        auto pred = [this] { return !queue_size || jobCountTotal() < queue_size || shutdown; };

        if (wait_microseconds)  /// Check for optional. Condition is true if the optional is set and the value is zero.
        {
            if (!job_finished.wait_for(lock, std::chrono::microseconds(*wait_microseconds), pred))
                return on_error();
        }
        else
            job_finished.wait(lock, pred);

        if (shutdown)
            return on_error();

        jobs.emplace(std::move(job), priority, job_group_id);
        incrementJobCountInGroup(job_group_id);

        if (threads.size() < std::min(max_threads, jobCountTotal()))
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
                decrementJobCountInGroup(job_group_id);

                if (only_log_exceptions)
                {
                    DB::tryLogCurrentException(__PRETTY_FUNCTION__);
                    if constexpr (std::is_same_v<ReturnType, bool>)
                        return false;
                }
                else
                {
                    return on_error();
                }
            }
        }
    }
    new_job_or_shutdown.notify_one();
    return ReturnType(true);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::wait()
{
    {
        std::unique_lock lock(mutex);
        job_finished.wait(lock, [this] { return jobCountTotal() == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::waitGroup(const JobGroupID & job_group_id)
{
     {
        std::unique_lock lock(mutex);
        job_finished.wait(lock, [this, &job_group_id] { return jobCountInGroup(job_group_id) == 0; });

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
    return jobCountTotal();
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::activeInGroup(const JobGroupID & job_group_id) const
{
    std::unique_lock lock(mutex);
    return jobCountInGroup(job_group_id);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::worker(typename std::list<Thread>::iterator thread_it)
{
    CurrentMetrics::Increment metric_all_threads(
        std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThread : CurrentMetrics::LocalThread);

    while (true)
    {
        Job job;
        JobGroupID job_group_id;
        bool need_shutdown = false;

        {
            std::unique_lock lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = jobs.top().job;
                job_group_id = jobs.top().job_group_id;
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

                /// If we have some custom metric than increment it until job done
                if (custom_job_metric != 0)
                {
                    CurrentMetrics::Increment custom_job_increment(custom_job_metric);
                    job();
                    /// job should be reseted before decrementing scheduled_jobs to
                    /// ensure that the Job destroyed before wait() returns.
                    job = {};
                }
                else
                {
                    job();
                    /// job should be reseted before decrementing scheduled_jobs to
                    /// ensure that the Job destroyed before wait() returns.
                    job = {};
                }
            }
            catch (...)
            {
                /// job should be reseted before decrementing scheduled_jobs to
                /// ensure that the Job destroyed before wait() returns.
                job = {};

                {
                    std::unique_lock lock(mutex);
                    /// Set current exception only if we care about them
                    if (!only_log_exceptions)
                    {
                        if (!first_exception)
                            first_exception = std::current_exception(); // NOLINT
                    }
                    else
                    {
                        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
                    }

                    if (shutdown_on_exception)
                        shutdown = true;

                    decrementJobCountInGroup(job_group_id);
                }

                job_finished.notify_all();
                new_job_or_shutdown.notify_all();
                return;
            }
        }

        {
            std::unique_lock lock(mutex);
            decrementJobCountInGroup(job_group_id);

            if (threads.size() > jobCountTotal() + max_free_threads)
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


template <typename Thread>
void ThreadPoolImpl<Thread>::incrementJobCountInGroup(const JobGroupID & job_group_id)
{
    scheduled_jobs_by_group[job_group_id]++;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::decrementJobCountInGroup(const JobGroupID & job_group_id)
{
    if (scheduled_jobs_by_group[job_group_id] == 1)
        scheduled_jobs_by_group.erase(job_group_id);
    else
        scheduled_jobs_by_group[job_group_id]--;
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::jobCountTotal() const
{
    size_t result = 0;
    for (const auto & job_to_count : scheduled_jobs_by_group)
        result += job_to_count.second;
    return result;
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::jobCountInGroup(const JobGroupID & job_group_id) const
{
    auto elem = scheduled_jobs_by_group.find(job_group_id);
    if (elem != scheduled_jobs_by_group.end())
        return elem->second;

    return 0;
}


template class ThreadPoolImpl<std::thread>;
template class ThreadPoolImpl<ThreadFromGlobalPool>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;

void FreeThreadPool::scheduleOrThrow(Job job)
{
    scheduleImpl<void>(std::move(job), 0, 0, DB::UUIDHelpers::Nil);
}

void FreeThreadPool::scheduleOrThrowOnError(Job job)
{
    scheduleImpl<void>(std::move(job), 0, std::nullopt, DB::UUIDHelpers::Nil);
}


void GlobalThreadPool::initialize(size_t max_threads)
{
    if (the_instance)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "The global thread pool is initialized twice");
    }

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

ThreadPool::ThreadPool(size_t max_threads_)
    : ThreadPoolImpl<ThreadFromGlobalPool>(max_threads_, max_threads_, max_threads_, /* shutdown_on_exception = */ true, /* only_log_exceptions = */ false, 0)
{
}

ThreadPool::ThreadPool()
    : ThreadPool(getNumberOfPhysicalCPUCores())
{
}

ThreadPool::ThreadPool(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_)
    : ThreadPoolImpl<ThreadFromGlobalPool>(max_threads_, max_free_threads_, queue_size_, shutdown_on_exception_, false, 0)
{}

void ThreadPool::scheduleOrThrowOnError(Job job, int priority, const JobGroupID & job_group_id)
{
    scheduleImpl<void>(std::move(job), priority, std::nullopt, job_group_id);
}

bool ThreadPool::trySchedule(Job job, int priority, uint64_t wait_microseconds, const JobGroupID & job_group_id) noexcept
{
    return scheduleImpl<bool>(std::move(job), priority, wait_microseconds, job_group_id);
}

void ThreadPool::scheduleOrThrow(Job job, int priority, uint64_t wait_microseconds, const JobGroupID & job_group_id)
{
    scheduleImpl<void>(std::move(job), priority, wait_microseconds, job_group_id);
}

void BackgroundThreadPool::scheduleOrThrowOnShutdown(Job job, const JobGroupID & job_group_id, int priority)
{
    /// wait_microseconds is nullopt to avoid timeout exception on queue shrink wait.
    scheduleImpl<void>(std::move(job), priority, std::nullopt, job_group_id);
}
