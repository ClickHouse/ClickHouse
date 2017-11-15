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

    /// Size is constant, all threads are created immediately.
    explicit ThreadPool(size_t m_size);

    /// Add new job. Locks until free thread in pool become available or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    void schedule(Job job);

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitary order.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    void wait();

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy object while calling schedule or wait methods from another threads.
    ~ThreadPool();

    size_t size() const { return m_size; }

    /// Returns number of active jobs.
    size_t active() const;

private:
    mutable std::mutex mutex;
    std::condition_variable has_free_thread;
    std::condition_variable has_new_job_or_shutdown;

    const size_t m_size;
    size_t active_jobs = 0;
    bool shutdown = false;

    std::queue<Job> jobs;
    std::vector<std::thread> threads;
    std::exception_ptr first_exception;


    void worker();
};

