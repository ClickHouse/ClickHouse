#include <Common/ThreadPool.h>
#include <Common/Exception.h>

#include <iostream>
#include <type_traits>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_SCHEDULE_TASK;
    }
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads)
    : ThreadPoolImpl(max_threads, max_threads, max_threads)
{
}

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads, size_t max_free_threads, size_t queue_size)
    : max_threads(max_threads), max_free_threads(max_free_threads), queue_size(queue_size)
{
}

template <typename Thread>
template <typename ReturnType>
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds)
{
    auto on_error = []
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw DB::Exception("Cannot schedule a task", DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
        else
            return false;
    };

    {
        std::unique_lock lock(mutex);

        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        if (wait_microseconds)
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
            }
        }
    }
    new_job_or_shutdown.notify_one();
    return ReturnType(true);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::schedule(Job job, int priority)
{
    scheduleImpl<void>(std::move(job), priority, std::nullopt);
}

template <typename Thread>
bool ThreadPoolImpl<Thread>::trySchedule(Job job, int priority, uint64_t wait_microseconds)
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
                job();
            }
            catch (...)
            {
                {
                    std::unique_lock lock(mutex);
                    if (!first_exception)
                        first_exception = std::current_exception();
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


void ExceptionHandler::setException(std::exception_ptr && exception)
{
    std::unique_lock lock(mutex);
    if (!first_exception)
        first_exception = std::move(exception);
}

void ExceptionHandler::throwIfException()
{
    std::unique_lock lock(mutex);
    if (first_exception)
        std::rethrow_exception(first_exception);
}


ThreadPool::Job createExceptionHandledJob(ThreadPool::Job job, ExceptionHandler & handler)
{
    return [job{std::move(job)}, &handler] ()
    {
        try
        {
            job();
        }
        catch (...)
        {
            handler.setException(std::current_exception());
        }
    };
}

