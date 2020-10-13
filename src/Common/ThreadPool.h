#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <list>
#include <optional>

#include <Common/CurrentMetrics.h>
#include <Poco/Event.h>
#include <Common/ThreadStatus.h>
#include <ext/scope_guard.h>
#include <Core/UUID.h>


/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait or just print them to log if flag only_log_exceptions specified.
  * - allows to run jobs with group identifier and wait until all jobs from some group finishes
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
    /// Identifier for group of jobs in thread pool. Allows to wait all jobs from one group.
    using JobGroupID = DB::UUID;
    /// Empty job group id by default.
    inline static const auto JobIDNil = DB::UUIDHelpers::Nil;

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitrary order.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    void wait();

    /// Same with previous, but wait only for jobs in current job_group_id can
    /// rethrow exception from any group.
    void waitGroup(const JobGroupID & job_group_id);

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy object while calling schedule or wait methods from another threads.
    ~ThreadPoolImpl();

    /// Returns number of running and scheduled jobs.
    size_t active() const;
    /// Returns number of running and scheduled jobs with this job_group_id
    size_t activeInGroup(const JobGroupID & job_group_id) const;

    void setMaxThreads(size_t value);
    void setMaxFreeThreads(size_t value);
    void setQueueSize(size_t value);

protected:
    template <typename ReturnType>
    ReturnType scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds, const JobGroupID & job_group_id);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than max_threads. Zero means unlimited.
    ThreadPoolImpl(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_, bool only_log_exceptions_, const CurrentMetrics::Metric & custom_job_metric);

private:
    mutable std::mutex mutex;
    std::condition_variable job_finished;
    std::condition_variable new_job_or_shutdown;

    size_t max_threads;
    size_t max_free_threads;
    size_t queue_size;

    std::unordered_map<JobGroupID, size_t> scheduled_jobs_by_group;

    bool shutdown = false;
    const bool shutdown_on_exception = true;
    /// Don't rethrow exception in all cases excepti pool shutdown
    const bool only_log_exceptions = false;
    /// Custom metric incremented on each job execution
    /// zero means no metric
    CurrentMetrics::Metric custom_job_metric = 0;

    struct JobWithPriority
    {
        Job job;
        int priority;
        JobGroupID job_group_id;

        JobWithPriority(Job job_, int priority_, const JobGroupID & job_group_id_)
            : job(job_), priority(priority_), job_group_id(job_group_id_) {}

        bool operator<(const JobWithPriority & rhs) const
        {
            return priority < rhs.priority;
        }
    };

    std::priority_queue<JobWithPriority> jobs;
    std::list<Thread> threads;
    std::exception_ptr first_exception;

    void worker(typename std::list<Thread>::iterator thread_it);

    void incrementJobCountInGroup(const JobGroupID & job_group_id);
    void decrementJobCountInGroup(const JobGroupID & job_group_id);

    size_t jobCountTotal() const;
    size_t jobCountInGroup(const JobGroupID & job_group_id) const;

    void finalize();
};

/// ThreadPool with std::thread for threads.
class FreeThreadPool : public ThreadPoolImpl<std::thread>
{
public:
    FreeThreadPool(
        size_t max_threads_, size_t max_free_threads_,
        size_t queue_size_, bool shutdown_on_exception_ = true)
        : ThreadPoolImpl<std::thread>(
            max_threads_, max_free_threads_, queue_size_, shutdown_on_exception_, false, 0)
    {}

    void scheduleOrThrow(Job job);
    void scheduleOrThrowOnError(Job job);
};


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
class GlobalThreadPool : public FreeThreadPool, private boost::noncopyable
{
    static std::unique_ptr<GlobalThreadPool> the_instance;

    using FreeThreadPool::FreeThreadPool;

public:
    static void initialize(size_t max_threads = 10000);
    static GlobalThreadPool & instance();
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
        /// NOTE: If this will throw an exception, the destructor won't be called.
        GlobalThreadPool::instance().scheduleOrThrow([
            state = state,
            func = std::forward<Function>(func),
            args = std::make_tuple(std::forward<Args>(args)...)]() mutable /// mutable is needed to destroy capture
        {
            SCOPE_EXIT(state->set());

            /// This moves are needed to destroy function and arguments before exit.
            /// It will guarantee that after ThreadFromGlobalPool::join all captured params are destroyed.
            auto function = std::move(func);
            auto arguments = std::move(args);

            /// Thread status holds raw pointer on query context, thus it always must be destroyed
            /// before sending signal that permits to join this thread.
            DB::ThreadStatus thread_status;
            std::apply(function, arguments);
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
class ThreadPool : public ThreadPoolImpl<ThreadFromGlobalPool>
{
public:
    ThreadPool();

    ThreadPool(size_t max_threads_);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than max_threads. Zero means unlimited.
    ThreadPool(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_ = true);

    /// Add new job. Locks until number of scheduled jobs is less than maximum or exception in one of threads was thrown.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    /// Also throws an exception if cannot create thread.
    /// Priority: greater is higher.
    /// NOTE: Probably you should call wait() if exception was thrown. If some previously scheduled jobs are using some objects,
    /// located on stack of current thread, the stack must not be unwinded until all jobs finished. However,
    /// if ThreadPool is a local object, it will wait for all scheduled jobs in own destructor.
    void scheduleOrThrowOnError(Job job, int priority = 0, const JobGroupID & job_group_id = JobIDNil);

    /// Similar to scheduleOrThrowOnError(...). Wait for specified amount of time and schedule a job or return false.
    bool trySchedule(Job job, int priority = 0, uint64_t wait_microseconds = 0, const JobGroupID & job_group_id = JobIDNil) noexcept;

    /// Similar to scheduleOrThrowOnError(...). Wait for specified amount of time and schedule a job or throw an exception.
    void scheduleOrThrow(Job job, int priority = 0, uint64_t wait_microseconds = 0, const JobGroupID & job_group_id = JobIDNil);
};

/// Thread pool for background tasks execution. Features:
/// 1. Doesn't rethrow any exception except calls of scheduleOrThrowOnShutdown after shutdown called.
/// 2. Print exceptions throwed from jobs to logs.
/// 3. Allows to increment custom metric during job execution.
class BackgroundThreadPool : public ThreadPoolImpl<ThreadFromGlobalPool>
{
public:
    BackgroundThreadPool(size_t max_threads_, const CurrentMetrics::Metric & custom_job_metric_)
        : ThreadPoolImpl<ThreadFromGlobalPool>(max_threads_, max_threads_, 10000, /* shutdown_on_exception = */ false, /* only_log_exceptions = */ true, custom_job_metric_)
    {}

    /// Throws exception only when shutdown called, otherwise print it to logs
    void scheduleOrThrowOnShutdown(Job job, const JobGroupID & job_group_id = JobIDNil, int priority = 0);
};
