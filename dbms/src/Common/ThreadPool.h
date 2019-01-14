#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <vector>
#include <ext/singleton.h>


/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait.
  */

template <typename Thread>
class ThreadPoolImpl
{
public:
    using Job = std::function<void()>;

    /// Size is constant. Up to num_threads are created on demand and then run until shutdown.
    explicit ThreadPoolImpl(size_t num_threads);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than num_threads. Zero means unlimited.
    ThreadPoolImpl(size_t num_threads, size_t queue_size);

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
    ~ThreadPoolImpl();

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
    std::vector<Thread> threads;
    std::exception_ptr first_exception;


    void worker();

    void finalize();
};


using FreeThreadPool = ThreadPoolImpl<std::thread>;

class GlobalThreadPool : public FreeThreadPool, public ext::singleton<GlobalThreadPool>
{
public:
    GlobalThreadPool() : FreeThreadPool(10000) {}   /// TODO: global blocking limit may lead to deadlocks.
};

class ThreadFromGlobalPool
{
public:
    ThreadFromGlobalPool() {}

    ThreadFromGlobalPool(std::function<void()> func)
    {
        mutex = std::make_unique<std::mutex>();
        /// The function object must be copyable, so we wrap lock_guard in shared_ptr.
        GlobalThreadPool::instance().schedule([lock = std::make_shared<std::lock_guard<std::mutex>>(*mutex), func = std::move(func)] { func(); });
    }

    ThreadFromGlobalPool(ThreadFromGlobalPool && rhs)
    {
        *this = std::move(rhs);
    }

    ThreadFromGlobalPool & operator=(ThreadFromGlobalPool && rhs)
    {
        if (mutex)
            std::terminate();
        mutex = std::move(rhs.mutex);
        return *this;
    }

    ~ThreadFromGlobalPool()
    {
        if (mutex)
            std::terminate();
    }

    void join()
    {
        {
            std::lock_guard lock(*mutex);
        }
        mutex.reset();
    }
private:
    std::unique_ptr<std::mutex> mutex;  /// Object must be moveable.
};

using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPool>;


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
