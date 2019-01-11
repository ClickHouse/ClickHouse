#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <vector>


/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait.
  */

class ThreadPool
{
public:
    using Job = std::function<void()>;

    /// Size is constant. Up to num_threads are created on demand and then run until shutdown.
    explicit ThreadPool(size_t num_threads);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than num_threads. Zero means unlimited.
    ThreadPool(size_t num_threads, size_t queue_size);

    /// Add new job. Locks until number of active jobs is less than maximum or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    /// Priority: greater is higher.
    void schedule(Job job, int priority = 0);

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitary order.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    void wait();

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy object while calling schedule or wait methods from another threads.
    ~ThreadPool();

    size_t size() const { return num_threads; }

    /// Returns number of running and scheduled jobs.
    size_t active() const;

private:
    mutable std::mutex mutex;
    std::condition_variable job_finished;
    std::condition_variable new_job_or_shutdown;

    const size_t num_threads;
    const size_t queue_size;

    size_t active_jobs = 0;
    bool shutdown = false;

    struct JobWithPriority
    {
        Job job;
        int priority;

        JobWithPriority(Job job, int priority)
            : job(job), priority(priority) {}

        bool operator< (const JobWithPriority & rhs) const
        {
            return priority < rhs.priority;
        }
    };

    std::priority_queue<JobWithPriority> jobs;
    std::vector<std::thread> threads;
    std::exception_ptr first_exception;


    void worker();

    void finalize();
};


/// Allows to save first catched exception in jobs and postpone its rethrow.
class ExceptionHandler
{
public:
    void setException(std::exception_ptr && exception);
    void throwIfException();

private:
    std::exception_ptr first_exception;
    std::mutex mutex;
};

ThreadPool::Job createExceptionHandledJob(ThreadPool::Job job, ExceptionHandler & handler);
