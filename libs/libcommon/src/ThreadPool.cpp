#include <common/ThreadPool.h>
#include <iostream>


ThreadPool::ThreadPool(size_t num_threads)
    : ThreadPool(num_threads, num_threads)
{
}


ThreadPool::ThreadPool(size_t num_threads, size_t queue_size)
    : num_threads(num_threads), queue_size(queue_size)
{
    threads.reserve(num_threads);

    try
    {
        for (size_t i = 0; i < num_threads; ++i)
            threads.emplace_back([this] { worker(); });
    }
    catch (...)
    {
        finalize();
        throw;
    }
}


void ThreadPool::schedule(Job job, int priority)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        job_finished.wait(lock, [this] { return !queue_size || active_jobs < queue_size || shutdown; });
        if (shutdown)
            return;

        jobs.emplace(std::move(job), priority);
        ++active_jobs;

        if (threads.size() < std::min(num_threads, active_jobs))
            threads.emplace_back([this] { worker(); });
    }
    new_job_or_shutdown.notify_one();
}

void ThreadPool::wait()
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        job_finished.wait(lock, [this] { return active_jobs == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

ThreadPool::~ThreadPool()
{
    finalize();
}

void ThreadPool::finalize()
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        shutdown = true;
    }

    new_job_or_shutdown.notify_all();

    for (auto & thread : threads)
        thread.join();

    threads.clear();
}

size_t ThreadPool::active() const
{
    std::unique_lock<std::mutex> lock(mutex);
    return active_jobs;
}


void ThreadPool::worker()
{
    while (true)
    {
        Job job;
        bool need_shutdown = false;

        {
            std::unique_lock<std::mutex> lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = jobs.top().job;
                jobs.pop();
            }
            else
            {
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
                    std::unique_lock<std::mutex> lock(mutex);
                    if (!first_exception)
                        first_exception = std::current_exception();
                    shutdown = true;
                    --active_jobs;
                }
                job_finished.notify_all();
                new_job_or_shutdown.notify_all();
                return;
            }
        }

        {
            std::unique_lock<std::mutex> lock(mutex);
            --active_jobs;
        }

        job_finished.notify_all();
    }
}


void ExceptionHandler::setException(std::exception_ptr && exception)
{
    std::unique_lock<std::mutex> lock(mutex);
    if (!first_exception)
        first_exception = std::move(exception);
}

void ExceptionHandler::throwIfException()
{
    std::unique_lock<std::mutex> lock(mutex);
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

