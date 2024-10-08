#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <list>
#include <optional>
#include <atomic>
#include <stack>
#include <random>

#include <boost/heap/priority_queue.hpp>
#include <pcg_random.hpp>

#include <Poco/Event.h>
#include <Common/ThreadStatus.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/Priority.h>
#include <Common/StackTrace.h>
#include <base/scope_guard.h>

class JobWithPriority;

/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait.
  *
  * This thread pool can be used as a task queue.
  * For example, you can create a thread pool with 10 threads (and queue of size 10) and schedule 1000 tasks
  * - in this case you will be blocked to keep 10 tasks in flight.
  *
  * Thread: std::thread or something with identical interface.
  */
template <typename Thread>
class ThreadPoolImpl
{
public:
    // used as 'unlimited' thread pool size
    // on linux you can not have more threads even if the RAM is unlimited
    // see https://docs.kernel.org/admin-guide/sysctl/kernel.html#threads-max
    static constexpr int MAX_THEORETICAL_THREAD_COUNT = 0x3fffffff; // ~1 billion

    using Job = std::function<void()>;
    using Metric = CurrentMetrics::Metric;

    // Subclass that encapsulates the thread and has the ability to remove itself from the pool.
    class ThreadFromThreadPool
    {
    public:
        using ThreadList = std::list<std::unique_ptr<ThreadFromThreadPool>>;

        /// Constructor to initialize and start the thread (but not associate it with the pool)
        explicit ThreadFromThreadPool(ThreadPoolImpl& parent_pool);

        // Shift the thread state from Preparing to Running to allow the worker to start.
        void start(ThreadList::iterator& it);

        void join();

        // Destructor to join the thread if needed (shift the state to Destructing if it was not running)
        ~ThreadFromThreadPool();

    private:
        ThreadPoolImpl& parent_pool;
        Thread thread;

        enum class ThreadState
        {
            Preparing,
            Running,
            Destructing
        };

        // Atomic state to track the thread's state
        std::atomic<ThreadState> thread_state;

        // Stores the position of the thread in the parent thread pool list
        typename std::list<std::unique_ptr<ThreadFromThreadPool>>::iterator thread_it;

        // Remove itself from the parent pool
        void removeSelfFromPoolNoPoolLock();

        // Worker does a busy loop (with yield) while the state is Preparing.
        // After that, immediately returns if the state changed to Destructing,
        // or starts the main working loop if the state is Running.
        void worker();
    };

    /// Maximum number of threads is based on the number of physical cores.
    ThreadPoolImpl(Metric metric_threads_, Metric metric_active_threads_, Metric metric_scheduled_jobs_);

    /// Size is constant. Up to num_threads are created on demand and then run until shutdown.
    explicit ThreadPoolImpl(
        Metric metric_threads_,
        Metric metric_active_threads_,
        Metric metric_scheduled_jobs_,
        size_t max_threads_);

    /// queue_size - maximum number of running plus scheduled jobs. It can be greater than max_threads. Zero means unlimited.
    ThreadPoolImpl(
        Metric metric_threads_,
        Metric metric_active_threads_,
        Metric metric_scheduled_jobs_,
        size_t max_threads_,
        size_t max_free_threads_,
        size_t queue_size_,
        bool shutdown_on_exception_ = true);

    /// Add new job. Locks until the number of scheduled jobs is less than the maximum or an exception in one of the threads was thrown.
    /// If any thread has thrown an exception, the first exception will be rethrown from this method,
    /// and the exception will be cleared.
    /// Also throws an exception if cannot create thread.
    /// Priority: lower is higher.
    /// NOTE: Probably you should call wait() if an exception was thrown. If some previously scheduled jobs are using some objects,
    /// located on the stack of the current thread, the stack must not be unwound until all jobs are finished. However,
    /// if ThreadPool is a local object, it will wait for all scheduled jobs in its own destructor.
    void scheduleOrThrowOnError(Job job, Priority priority = {});

    /// Similar to scheduleOrThrowOnError(...). Wait for specified amount of time and schedule a job or return false.
    bool trySchedule(Job job, Priority priority = {}, uint64_t wait_microseconds = 0) noexcept;

    /// Similar to scheduleOrThrowOnError(...). Wait for specified amount of time and schedule a job or throw an exception.
    void scheduleOrThrow(Job job, Priority priority = {}, uint64_t wait_microseconds = 0, bool propagate_opentelemetry_tracing_context = true);

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitrary order.
    /// If any thread has thrown an exception, the first exception will be rethrown from this method,
    /// and the exception will be cleared.
    void wait();

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy the object while calling schedule or wait methods from other threads.
    ~ThreadPoolImpl();

    /// Returns number of running and scheduled jobs.
    size_t active() const;

    /// Returns true if the pool already terminated
    /// (and any further scheduling will produce CANNOT_SCHEDULE_TASK exception)
    bool finished() const;

    void setMaxThreads(size_t value);
    void setMaxFreeThreads(size_t value);
    void setQueueSize(size_t value);
    size_t getMaxThreads() const;

    /// Adds a callback which is called in destructor after
    /// joining of all threads. The order of calling callbacks
    /// is reversed to the order of their addition.
    /// It may be useful for static thread pools to call
    /// function after joining of threads because order
    /// of destructors of global static objects and callbacks
    /// added by atexit is undefined for different translation units.
    using OnDestroyCallback = std::function<void()>;
    void addOnDestroyCallback(OnDestroyCallback && callback);

private:
    friend class GlobalThreadPool;

    mutable std::mutex mutex;
    std::condition_variable job_finished;
    std::condition_variable new_job_or_shutdown;

    Metric metric_threads;
    Metric metric_active_threads;
    Metric metric_scheduled_jobs;

    size_t max_threads;
    size_t max_free_threads;
    size_t queue_size;

    size_t scheduled_jobs = 0;

    // Originally equals to max_threads, but changes dynamically.
    // Decrements with every new thread started, increments when it finishes.
    // If positive, then more threads can be started.
    // When it comes to zero, it means that max_threads threads have already been started.
    // it can be below zero when the threadpool is shutting down
    std::atomic<int64_t> remaining_pool_capacity;

    // Increments every time a new thread joins the thread pool or a job finishes.
    // Decrements every time a task is scheduled.
    // If positive, it means that there are more threads than jobs (and some are idle).
    // If zero, it means that every thread has a job.
    // If negative, it means that we have more jobs than threads.
    std::atomic<int64_t> available_threads;

    bool shutdown = false;
    bool threads_remove_themselves = true;
    const bool shutdown_on_exception = true;

    boost::heap::priority_queue<JobWithPriority,boost::heap::stable<true>> jobs;
    ThreadFromThreadPool::ThreadList threads;
    std::exception_ptr first_exception;
    std::stack<OnDestroyCallback> on_destroy_callbacks;

    template <typename ReturnType>
    ReturnType scheduleImpl(Job job, Priority priority, std::optional<uint64_t> wait_microseconds, bool propagate_opentelemetry_tracing_context = true);

    /// Tries to start new threads if there are scheduled jobs and the limit `max_threads` is not reached. Must be called with the mutex locked.
    void startNewThreadsNoLock();

    void finalize();
    void onDestroy();
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
class GlobalThreadPool : public FreeThreadPool, private boost::noncopyable
{
    static std::unique_ptr<GlobalThreadPool> the_instance;

    GlobalThreadPool(
        size_t max_threads_,
        size_t max_free_threads_,
        size_t queue_size_,
        bool shutdown_on_exception_,
        UInt64 global_profiler_real_time_period_ns_,
        UInt64 global_profiler_cpu_time_period_ns_);

public:
    UInt64 global_profiler_real_time_period_ns;
    UInt64 global_profiler_cpu_time_period_ns;

    static void initialize(
        size_t max_threads = 10000,
        size_t max_free_threads = 1000,
        size_t queue_size = 10000,
        UInt64 global_profiler_real_time_period_ns_ = 0,
        UInt64 global_profiler_cpu_time_period_ns_ = 0);

    static GlobalThreadPool & instance();
    static void shutdown();
};


/** Looks like std::thread but allocates threads in GlobalThreadPool.
  * Also holds ThreadStatus for ClickHouse.
  *
  * NOTE: User code should use 'ThreadFromGlobalPool' declared below instead of directly using this class.
  *
  */
template <bool propagate_opentelemetry_context = true, bool global_trace_collector_allowed = true>
class ThreadFromGlobalPoolImpl : boost::noncopyable
{
public:
    ThreadFromGlobalPoolImpl() = default;

    template <typename Function, typename... Args>
    explicit ThreadFromGlobalPoolImpl(Function && func, Args &&... args)
        : state(std::make_shared<State>())
    {
        UInt64 global_profiler_real_time_period = GlobalThreadPool::instance().global_profiler_real_time_period_ns;
        UInt64 global_profiler_cpu_time_period = GlobalThreadPool::instance().global_profiler_cpu_time_period_ns;
        /// NOTE:
        /// - If this will throw an exception, the destructor won't be called
        /// - this pointer cannot be passed in the lambda, since after detach() it will not be valid
        GlobalThreadPool::instance().scheduleOrThrow([
            my_state = state,
            global_profiler_real_time_period,
            global_profiler_cpu_time_period,
            my_func = std::forward<Function>(func),
            my_args = std::make_tuple(std::forward<Args>(args)...)]() mutable /// mutable is needed to destroy capture
        {
            SCOPE_EXIT(
                my_state->thread_id = std::thread::id();
                my_state->event.set();
            );

            my_state->thread_id = std::this_thread::get_id();

            /// This moves are needed to destroy function and arguments before exit.
            /// It will guarantee that after ThreadFromGlobalPool::join all captured params are destroyed.
            auto function = std::move(my_func);
            auto arguments = std::move(my_args);

            /// Thread status holds raw pointer on query context, thus it always must be destroyed
            /// before sending signal that permits to join this thread.
            DB::ThreadStatus thread_status;
            if constexpr (global_trace_collector_allowed)
            {
                if (unlikely(global_profiler_real_time_period != 0 || global_profiler_cpu_time_period != 0))
                    thread_status.initGlobalProfiler(global_profiler_real_time_period, global_profiler_cpu_time_period);
            }
            else
            {
                UNUSED(global_profiler_real_time_period);
                UNUSED(global_profiler_cpu_time_period);
            }

            std::apply(function, arguments);
        },
        {}, // default priority
        0, // default wait_microseconds
        propagate_opentelemetry_context
        );
    }

    ThreadFromGlobalPoolImpl(ThreadFromGlobalPoolImpl && rhs) noexcept
    {
        *this = std::move(rhs);
    }

    ThreadFromGlobalPoolImpl & operator=(ThreadFromGlobalPoolImpl && rhs) noexcept
    {
        if (initialized())
            abort();
        state = std::move(rhs.state);
        return *this;
    }

    ~ThreadFromGlobalPoolImpl()
    {
        if (initialized())
            abort();
    }

    void join()
    {
        if (!initialized())
            abort();

        /// Thread cannot join itself.
        if (state->thread_id == std::this_thread::get_id())
            abort();

        state->event.wait();
        state.reset();
    }

    void detach()
    {
        if (!initialized())
            abort();
        state.reset();
    }

    bool joinable() const
    {
        return initialized();
    }

    std::thread::id get_id() const
    {
        return state ? state->thread_id.load() : std::thread::id{};
    }

protected:
    struct State
    {
        /// Should be atomic() because of possible concurrent access between
        /// assignment and joinable() check.
        std::atomic<std::thread::id> thread_id;

        /// The state used in this object and inside the thread job.
        Poco::Event event;
    };
    std::shared_ptr<State> state;

    /// Internally initialized() should be used over joinable(),
    /// since it is enough to know that the thread is initialized,
    /// and ignore that fact that thread cannot join itself.
    bool initialized() const
    {
        return static_cast<bool>(state);
    }
};

/// Schedule jobs/tasks on global thread pool without implicit passing tracing context on current thread to underlying worker as parent tracing context.
///
/// If you implement your own job/task scheduling upon global thread pool or schedules a long time running job in a infinite loop way,
/// you need to use class, or you need to use ThreadFromGlobalPool below.
///
/// See the comments of ThreadPool below to know how it works.
using ThreadFromGlobalPoolNoTracingContextPropagation = ThreadFromGlobalPoolImpl<false, true>;

/// An alias of thread that execute jobs/tasks on global thread pool by implicit passing tracing context on current thread to underlying worker as parent tracing context.
/// If jobs/tasks are directly scheduled by using APIs of this class, you need to use this class or you need to use class above.
using ThreadFromGlobalPool = ThreadFromGlobalPoolImpl<true, true>;
using ThreadFromGlobalPoolWithoutTraceCollector = ThreadFromGlobalPoolImpl<true, false>;

/// Recommended thread pool for the case when multiple thread pools are created and destroyed.
///
/// The template parameter of ThreadFromGlobalPool is set to false to disable tracing context propagation to underlying worker.
/// Because ThreadFromGlobalPool schedules a job upon GlobalThreadPool, this means there will be two workers to schedule a job in 'ThreadPool',
/// one is at GlobalThreadPool level, the other is at ThreadPool level, so tracing context will be initialized on the same thread twice.
///
/// Once the worker on ThreadPool gains the control of execution, it won't return until it's shutdown,
/// which means the tracing context initialized at underlying worker level won't be deleted for a very long time.
/// This would cause wrong context for further jobs scheduled in ThreadPool.
///
/// To make sure the tracing context is correctly propagated, we explicitly disable context propagation(including initialization and de-initialization) at underlying worker level.
///
using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolNoTracingContextPropagation>;

/// Enables fault injections globally for all thread pools
class CannotAllocateThreadFaultInjector
{
    std::atomic_bool enabled = false;
    std::mutex mutex;
    pcg64_fast rndgen;
    std::optional<std::bernoulli_distribution> random;

    static thread_local bool block_fault_injections;

    static CannotAllocateThreadFaultInjector & instance();
public:
    static void setFaultProbability(double probability);
    static bool injectFault();

    static scope_guard blockFaultInjections();
};
