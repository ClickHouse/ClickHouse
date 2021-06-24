#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <cassert>
#include <type_traits>
#include <common/logger_useful.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <iostream>

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

template <typename... Args>
void QueueJobContainer::emplace(Args && ... args)
{
    jobs.emplace(std::forward<Args>(args)...);
}

QueueJobContainer::Job QueueJobContainer::pop()
{
    auto result = std::move(jobs.front());
    jobs.pop();
    return result;
}

bool QueueJobContainer::empty() const
{
    return jobs.empty();
}

bool QueueJobContainer::executeJobOrThrowOnError(QueueJobContainer::Job && job)
{
    try
    {
        job();
        /// job should be reset before decrementing scheduled_jobs to
        /// ensure that the Job destroyed before wait() returns.
        job = {};
        return true;
    }
    catch (...)
    {
        /// job should be reset before decrementing scheduled_jobs to
        /// ensure that the Job destroyed before wait() returns.
        job = {};
        throw;
    }
}


void PriorityJobContainer::emplace(PriorityJobContainer::Job job)
{
    std::lock_guard lock(mutex);
    jobs.emplace(job);
}

PriorityJobContainer::Job PriorityJobContainer::pop()
{
    std::lock_guard lock(mutex);
    /// std::priority_queue does not provide interface for getting non-const reference to an element
    /// to prevent us from modifying its priority. We have to use const_cast to force move semantics.
    auto result = std::move(const_cast<Job &>(jobs.top()));
    jobs.pop();
    return result;
}

bool PriorityJobContainer::empty() const
{
    std::lock_guard lock(mutex);
    return jobs.empty();
}

bool PriorityJobContainer::executeJobOrThrowOnError(PriorityJobContainer::Job && job)
{
    try
    {
        if (job->execute())
        {
            /// Job wants to be executed one more time
            /// Called without lock, because it acquires lock by itself
            emplace(std::move(job));
            return false;
        }
        /// job should be reset before decrementing scheduled_jobs to
        /// ensure that the Job destroyed before wait() returns.
        job.reset();
        return true;
    }
    catch (...)
    {
        /// job should be reset before decrementing scheduled_jobs to
        /// ensure that the Job destroyed before wait() returns.
        job.reset();
        DB::tryLogCurrentException(&Poco::Logger::get("abacaba"));
        throw;
    }
}


template <typename Thread, typename JobContainer>
ThreadPoolImpl<Thread, JobContainer>::ThreadPoolImpl()
    : ThreadPoolImpl(getNumberOfPhysicalCPUCores())
{
}


template <typename Thread, typename JobContainer>
ThreadPoolImpl<Thread, JobContainer>::ThreadPoolImpl(size_t max_threads_)
    : ThreadPoolImpl(max_threads_, max_threads_, max_threads_)
{
}

template <typename Thread, typename JobContainer>
ThreadPoolImpl<Thread, JobContainer>::ThreadPoolImpl(
    size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_)
    : max_threads(max_threads_)
    , max_free_threads(max_free_threads_)
    , queue_size(queue_size_)
    , shutdown_on_exception(shutdown_on_exception_)
{
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::setMaxThreads(size_t value)
{
    std::lock_guard lock(mutex);
    max_threads = value;
}

template <typename Thread, typename JobContainer>
size_t ThreadPoolImpl<Thread, JobContainer>::getMaxThreads() const
{
    std::lock_guard lock(mutex);
    return max_threads;
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::setMaxFreeThreads(size_t value)
{
    std::lock_guard lock(mutex);
    max_free_threads = value;
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::setQueueSize(size_t value)
{
    std::lock_guard lock(mutex);
    queue_size = value;
}


template <typename Thread, typename JobContainer>
template <typename ReturnType>
ReturnType ThreadPoolImpl<Thread, JobContainer>::scheduleImpl(Job job)
{
    auto on_error = [&](const std::string & reason)
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (first_exception)
            {
                std::exception_ptr exception;
                std::swap(exception, first_exception);
                std::rethrow_exception(exception);
            }
            throw DB::Exception(DB::ErrorCodes::CANNOT_SCHEDULE_TASK,
                "Cannot schedule a task: {} (threads={}, jobs={})", reason,
                threads.size(), scheduled_jobs);
        }
        else
            return false;
    };

    {
        std::unique_lock lock(mutex);

        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        job_finished.wait(lock, pred);

        if (shutdown)
            return on_error("shutdown");

        /// We must not to allocate any memory after we emplaced a job in a queue.
        /// Because if an exception would be thrown, we won't notify a thread about job occurrence.

        /// Check if there are enough threads to process job.
        if (threads.size() < std::min(max_threads, scheduled_jobs + 1))
        {
            try
            {
                threads.emplace_front();
            }
            catch (...)
            {
                /// Most likely this is a std::bad_alloc exception
                return on_error("cannot allocate thread slot");
            }

            try
            {
                threads.front() = Thread([this, it = threads.begin()] { worker(it); });
            }
            catch (...)
            {
                threads.pop_front();
                return on_error("cannot allocate thread");
            }
        }

        jobs.emplace(std::move(job));
        ++scheduled_jobs;
        new_job_or_shutdown.notify_one();
    }

    return ReturnType(true);
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::scheduleOrThrowOnError(Job job)
{
    scheduleImpl<void>(std::move(job));
}

template <typename Thread, typename JobContainer>
bool ThreadPoolImpl<Thread, JobContainer>::trySchedule(Job job) noexcept
{
    return scheduleImpl<bool>(std::move(job));
}


template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::wait()
{
    {
        std::unique_lock lock(mutex);
        /// Signal here just in case.
        /// If threads are waiting on condition variables, but there are some jobs in the queue
        /// then it will prevent us from deadlock.
        new_job_or_shutdown.notify_all();
        job_finished.wait(lock, [this] { return scheduled_jobs == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

template <typename Thread, typename JobContainer>
ThreadPoolImpl<Thread, JobContainer>::~ThreadPoolImpl()
{
    finalize();
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::finalize()
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

template <typename Thread, typename JobContainer>
size_t ThreadPoolImpl<Thread, JobContainer>::active() const
{
    std::unique_lock lock(mutex);
    return scheduled_jobs;
}

template <typename Thread, typename JobContainer>
bool ThreadPoolImpl<Thread, JobContainer>::finished() const
{
    std::unique_lock lock(mutex);
    return shutdown;
}

template <typename Thread, typename JobContainer>
void ThreadPoolImpl<Thread, JobContainer>::worker(typename std::list<Thread>::iterator thread_it)
{
    (void)thread_it;
    DENY_ALLOCATIONS_IN_SCOPE;
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
                job = jobs.pop();
            else
                return; /// shutdown is true, simply finish the thread.
        }

        bool decrement_tasks_count = false;

        if (!need_shutdown)
        {
            try
            {
                ALLOW_ALLOCATIONS_IN_SCOPE;
                CurrentMetrics::Increment metric_active_threads(
                    std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThreadActive : CurrentMetrics::LocalThreadActive);

                decrement_tasks_count = jobs.executeJobOrThrowOnError(std::move(job));
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

            if (decrement_tasks_count)
                --scheduled_jobs;


            /// Maybe there are some tasks to do?
            new_job_or_shutdown.notify_all();

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


template class ThreadPoolImpl<std::thread, QueueJobContainer>;
template class ThreadPoolImpl<ThreadFromGlobalPool, QueueJobContainer>;
template class ThreadPoolImpl<ThreadFromGlobalPool, PriorityJobContainer>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;

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
