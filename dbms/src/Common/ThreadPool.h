#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <list>
#include <optional>
#include <ext/singleton.h>

#include <Poco/Event.h>
#include <Common/ThreadStatus.h>


/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait.
  *
  * This thread pool can be used as a task queue.
  * For example, you can create a thread pool with 10 threads (and queue of size 10) and schedule 1000 tasks
  * - in this case you will be blocked to keep 10 tasks in fly.
  *
  * Thread: std::thread or something with identical interface.
  */
template <typename Thread>
class ThreadPoolImpl
{
public:
    using Job = std::function<void()>;

    /// Size is constant. Up to num_threads are created on demand and then run until shutdown.
    explicit ThreadPoolImpl(size_t max_threads);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than max_threads. Zero means unlimited.
    ThreadPoolImpl(size_t max_threads, size_t max_free_threads, size_t queue_size);

    /// Add new job. Locks until number of scheduled jobs is less than maximum or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    /// Priority: greater is higher.
    void schedule(Job job, int priority = 0);

    /// Wait for specified amount of time and schedule a job or return false.
    bool trySchedule(Job job, int priority = 0, uint64_t wait_microseconds = 0);

    /// Wait for specified amount of time and schedule a job or throw an exception.
    void scheduleOrThrow(Job job, int priority = 0, uint64_t wait_microseconds = 0);

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitary order.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    void wait();

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy object while calling schedule or wait methods from another threads.
    ~ThreadPoolImpl();

    /// Returns number of running and scheduled jobs.
    size_t active() const;

private:
    mutable std::mutex mutex;
    std::condition_variable job_finished;
    std::condition_variable new_job_or_shutdown;

    const size_t max_threads;
    const size_t max_free_threads;
    const size_t queue_size;

    size_t scheduled_jobs = 0;
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
    std::list<Thread> threads;
    std::exception_ptr first_exception;


    template <typename ReturnType>
    ReturnType scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds);

    void worker(typename std::list<Thread>::iterator thread_it);

    void finalize();
};


/// ThreadPool with std::thread for threads.
using FreeThreadPool = ThreadPoolImpl<std::thread>;


/** Global ThreadPool that can be used as a singleton.
  * Why it is needed?
  *
  * Linux can create and destroy about 100 000 threads per second (quite good).
  * With simple ThreadPool (based on mutex and condvar) you can assign about 200 000 tasks per second
  * - not much difference comparing to not using a thread pool at all.
  *
  * But if you reuse OS threads instead of creating and destroying them, several benefits exist:
  * - allocator performance will usually be better due to reuse of thread local caches, especially for jemalloc:
  *   https://github.com/jemalloc/jemalloc/issues/1347
  * - address sanitizer and thread sanitizer will not fail due to global limit on number of created threads.
  * - program will work faster in gdb;
  */
class GlobalThreadPool : public FreeThreadPool, public ext::singleton<GlobalThreadPool>
{
public:
    GlobalThreadPool() : FreeThreadPool(10000, 1000, 10000) {}
};


/** Looks like std::thread but allocates threads in GlobalThreadPool.
  * Also holds ThreadStatus for ClickHouse.
  */
class ThreadFromGlobalPool
{
public:
    ThreadFromGlobalPool() {}

    template <typename Function, typename... Args>
    explicit ThreadFromGlobalPool(Function && func, Args &&... args)
        : state(std::make_shared<Poco::Event>())
    {
        /// NOTE: If this will throw an exception, the descructor won't be called.
        GlobalThreadPool::instance().scheduleOrThrow([
            state = state,
            func = std::forward<Function>(func),
            args = std::make_tuple(std::forward<Args>(args)...)]
        {
            {
                DB::ThreadStatus thread_status;
                std::apply(func, args);
            }
            state->set();
        });
    }

    ThreadFromGlobalPool(ThreadFromGlobalPool && rhs)
    {
        *this = std::move(rhs);
    }

    ThreadFromGlobalPool & operator=(ThreadFromGlobalPool && rhs)
    {
        if (joinable())
            std::terminate();
        state = std::move(rhs.state);
        return *this;
    }

    ~ThreadFromGlobalPool()
    {
        if (joinable())
            std::terminate();
    }

    void join()
    {
        if (!joinable())
            std::terminate();

        state->wait();
        state.reset();
    }

    void detach()
    {
        if (!joinable())
            std::terminate();
        state.reset();
    }

    bool joinable() const
    {
        return state != nullptr;
    }

private:
    /// The state used in this object and inside the thread job.
    std::shared_ptr<Poco::Event> state;
};


/// Recommended thread pool for the case when multiple thread pools are created and destroyed.
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
