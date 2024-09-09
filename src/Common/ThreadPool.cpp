#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/noexcept_scope.h>

#include <type_traits>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <base/demangle.h>

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
    extern const Metric GlobalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event GlobalThreadPoolExpansions;
    extern const Event GlobalThreadPoolShrinks;
    extern const Event GlobalThreadPoolThreadCreationMicroseconds;
    extern const Event GlobalThreadPoolLockWaitMicroseconds;
    extern const Event GlobalThreadPoolJobs;
    extern const Event GlobalThreadPoolJobWaitTimeMicroseconds;

    extern const Event LocalThreadPoolExpansions;
    extern const Event LocalThreadPoolShrinks;
    extern const Event LocalThreadPoolThreadCreationMicroseconds;
    extern const Event LocalThreadPoolLockWaitMicroseconds;
    extern const Event LocalThreadPoolJobs;
    extern const Event LocalThreadPoolBusyMicroseconds;
    extern const Event LocalThreadPoolJobWaitTimeMicroseconds;

}

static constexpr auto DEFAULT_THREAD_NAME = "ThreadPool";

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(Metric metric_threads_, Metric metric_active_threads_, Metric metric_scheduled_jobs_)
    : ThreadPoolImpl(metric_threads_, metric_active_threads_, metric_scheduled_jobs_, getNumberOfCPUCoresToUse())
{
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(
    Metric metric_threads_,
    Metric metric_active_threads_,
    Metric metric_scheduled_jobs_,
    size_t max_threads_)
    : ThreadPoolImpl(metric_threads_, metric_active_threads_, metric_scheduled_jobs_, max_threads_, max_threads_, max_threads_)
{
}

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(
    Metric metric_threads_,
    Metric metric_active_threads_,
    Metric metric_scheduled_jobs_,
    size_t max_threads_,
    size_t max_free_threads_,
    size_t queue_size_,
    bool shutdown_on_exception_)
    : metric_threads(metric_threads_)
    , metric_active_threads(metric_active_threads_)
    , metric_scheduled_jobs(metric_scheduled_jobs_)
    , max_threads(max_threads_)
    , max_free_threads(std::min(max_free_threads_, max_threads))
    , queue_size(queue_size_ ? std::max(queue_size_, max_threads) : 0 /* zero means the queue is unlimited */)
    , shutdown_on_exception(shutdown_on_exception_)
{
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setMaxThreads(size_t value)
{
    std::lock_guard lock(mutex);
    bool need_start_threads = (value > max_threads);
    bool need_finish_free_threads = (value < max_free_threads);

    max_threads = value;
    max_free_threads = std::min(max_free_threads, max_threads);

    /// We have to also adjust queue size, because it limits the number of scheduled and already running jobs in total.
    queue_size = queue_size ? std::max(queue_size, max_threads) : 0;
    jobs.reserve(queue_size);

    if (need_start_threads)
    {
        /// Start new threads while there are more scheduled jobs in the queue and the limit `max_threads` is not reached.
        startNewThreadsNoLock();
    }
    else if (need_finish_free_threads)
    {
        /// Wake up free threads so they can finish themselves.
        new_job_or_shutdown.notify_all();
    }
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::getMaxThreads() const
{
    std::lock_guard lock(mutex);
    return max_threads;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setMaxFreeThreads(size_t value)
{
    std::lock_guard lock(mutex);
    bool need_finish_free_threads = (value < max_free_threads);

    max_free_threads = std::min(value, max_threads);

    if (need_finish_free_threads)
    {
        /// Wake up free threads so they can finish themselves.
        new_job_or_shutdown.notify_all();
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setQueueSize(size_t value)
{
    std::lock_guard lock(mutex);
    queue_size = value ? std::max(value, max_threads) : 0;
    /// Reserve memory to get rid of allocations
    jobs.reserve(queue_size);
}


template <typename Thread>
template <typename ReturnType>
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, Priority priority, std::optional<uint64_t> wait_microseconds, bool propagate_opentelemetry_tracing_context)
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

    JobWithPriority new_job = JobWithPriority(std::move(job),
                    priority,
                    metric_scheduled_jobs,
                    /// Tracing context on this thread is used as parent context for the sub-thread that runs the job
                    propagate_opentelemetry_tracing_context ? DB::OpenTelemetry::CurrentContext() : DB::OpenTelemetry::TracingContextOnThread(),
                    /// capture_frame_pointers
                    DB::Exception::enable_job_stack_trace);

    {
        Stopwatch watch;
        std::unique_lock lock(mutex);
        ProfileEvents::increment(
            std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolLockWaitMicroseconds : ProfileEvents::LocalThreadPoolLockWaitMicroseconds,
            watch.elapsedMicroseconds());

        if (CannotAllocateThreadFaultInjector::injectFault())
            return on_error("fault injected");

        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        /// Wait for available threads or timeout
        if (wait_microseconds)  /// Check for optional. Condition is true if the optional is set. Even if the value is zero.
        {
            if (!job_finished.wait_for(lock, std::chrono::microseconds(*wait_microseconds), pred))
                return on_error(fmt::format("no free thread (timeout={})", *wait_microseconds));
        }
        else
            job_finished.wait(lock, pred);

        if (shutdown)
            return on_error("shutdown");

        /// Check if there are enough threads to process job.
        if (threads.size() < std::min(max_threads, scheduled_jobs + 1))
        {
            typename std::list<ThreadFromThreadPool>::iterator thread_it;

            try
            {
                /// Add a new deferred-starting thread to the pool
                threads.emplace_front(*this, /* defer_thread_starting */ true);
                thread_it = threads.begin();
            }
            catch (...)
            {
                /// Most likely this is a std::bad_alloc exception
                return on_error("cannot allocate thread slot");
            }

            ++scheduled_jobs;
            lock.unlock();

            try
            {
                /// Start the thread after unlocking the mutex to avoid holding the lock too long
                /// as in some conditions starting new threads can be time-consuming
                /// Once the thread will start it will push the new job in the queue.
                /// this way we have atomicity here w/o holding a lock when the thread is started.
                thread_it->startThread(std::move(new_job));
            }
            catch (...)
            {
                /// Re-acquire the lock and clean up on error
                lock.lock();
                --scheduled_jobs;
                threads.erase(thread_it);
                return on_error("cannot allocate thread");
            }
        }
        else
        {
            /// Place the job in the queue if there are enough threads
            jobs.emplace(std::move(new_job));
            /// We must not to allocate any memory after we emplaced a job in a queue.
            /// Because if an exception would be thrown, we won't notify a thread about job occurrence.
            ++scheduled_jobs;
            lock.unlock();
            /// Wake up a free thread to run the new job.
            new_job_or_shutdown.notify_one();
        }

    }

    ProfileEvents::increment(std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobs : ProfileEvents::LocalThreadPoolJobs);

    return static_cast<ReturnType>(true);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::startNewThreadsNoLock()
{
    if (shutdown)
        return;

    /// Start new threads while there are more scheduled jobs in the queue and the limit `max_threads` is not reached.
    while (threads.size() < std::min(scheduled_jobs, max_threads))
    {
        try
        {
            // Create and add a new thread to the front of the list
            threads.emplace_front(*this);
        }
        catch (...)
        {
            threads.pop_front();
            break; /// failed to start more threads
        }
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrowOnError(Job job, Priority priority)
{
    scheduleImpl<void>(std::move(job), priority, std::nullopt);
}

template <typename Thread>
bool ThreadPoolImpl<Thread>::trySchedule(Job job, Priority priority, uint64_t wait_microseconds) noexcept
{
    return scheduleImpl<bool>(std::move(job), priority, wait_microseconds);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrow(Job job, Priority priority, uint64_t wait_microseconds, bool propagate_opentelemetry_tracing_context)
{
    scheduleImpl<void>(std::move(job), priority, wait_microseconds, propagate_opentelemetry_tracing_context);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::wait()
{
    Stopwatch watch;
    std::unique_lock lock(mutex);
    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolLockWaitMicroseconds : ProfileEvents::LocalThreadPoolLockWaitMicroseconds,
        watch.elapsedMicroseconds());
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

template <typename Thread>
ThreadPoolImpl<Thread>::~ThreadPoolImpl()
{
    /// Note: should not use logger from here,
    /// because it can be an instance of GlobalThreadPool that is a global variable
    /// and the destruction order of global variables is unspecified.

    finalize();
    onDestroy();
}

template <typename Thread>
void ThreadPoolImpl<Thread>::finalize()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
        /// We don't want threads to remove themselves from `threads` anymore, otherwise `thread.join()` will go wrong below in this function.
        threads_remove_themselves = false;
    }

    /// Wake up threads so they can finish themselves.
    new_job_or_shutdown.notify_all();

    /// Wait for all currently running jobs to finish (we don't wait for all scheduled jobs here like the function wait() does).

    threads.clear();
    // destructors of the ThreadFromThreadPool will join them
}

template <typename Thread>
void ThreadPoolImpl<Thread>::addOnDestroyCallback(OnDestroyCallback && callback)
{
    std::lock_guard lock(mutex);
    on_destroy_callbacks.push(std::move(callback));
}

template <typename Thread>
void ThreadPoolImpl<Thread>::onDestroy()
{
    while (!on_destroy_callbacks.empty())
    {
        auto callback = std::move(on_destroy_callbacks.top());
        on_destroy_callbacks.pop();
        NOEXCEPT_SCOPE({ callback(); });
    }
}

template <typename Thread>
size_t ThreadPoolImpl<Thread>::active() const
{
    std::lock_guard lock(mutex);
    return scheduled_jobs;
}

template <typename Thread>
bool ThreadPoolImpl<Thread>::finished() const
{
    std::lock_guard lock(mutex);
    return shutdown;
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadFromThreadPool::ThreadFromThreadPool(ThreadPoolImpl& parent_pool_, bool defer_thread_starting)
    : parent_pool(parent_pool_)
{
    // If thread starting is not deferred, start immediately
    if (!defer_thread_starting)
    {
        std::lock_guard<std::mutex> lock(mutex);
        startThreadNoLock();
    }
}

// mutex should be locked
template <typename Thread>
void ThreadPoolImpl<Thread>::ThreadFromThreadPool::startThreadNoLock()
{
    Stopwatch watch2;

    thread.emplace([this]() { this->worker(); });

    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolThreadCreationMicroseconds : ProfileEvents::LocalThreadPoolThreadCreationMicroseconds,
        watch2.elapsedMicroseconds());
    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolExpansions : ProfileEvents::LocalThreadPoolExpansions);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::ThreadFromThreadPool::startThread(JobWithPriority new_job_)
{
    std::lock_guard<std::mutex> lock(mutex);
    new_job = std::move(new_job_);
    startThreadNoLock();
}

// parent_pool.mutex should be locked!
template <typename Thread>
void ThreadPoolImpl<Thread>::ThreadFromThreadPool::removeSelfFromPoolNoPoolLock()
{
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (thread && thread->joinable())
            thread->detach();
    }

    parent_pool.threads.remove_if([this](const ThreadFromThreadPool& t){ return &t == this; });
}

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadFromThreadPool::~ThreadFromThreadPool()
{
    std::lock_guard<std::mutex> lock(mutex);

    // Ensure the thread is joined before destruction if still joinable
    if (thread && thread->joinable())
        thread->join();

    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolShrinks : ProfileEvents::LocalThreadPoolShrinks);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::ThreadFromThreadPool::worker()
{
    DENY_ALLOCATIONS_IN_SCOPE;
    CurrentMetrics::Increment metric_pool_threads(parent_pool.metric_threads);

    bool job_is_done = false;
    std::exception_ptr exception_from_job;

    /// We'll run jobs in this worker while there are scheduled jobs and until some special event occurs (e.g. shutdown, or decreasing the number of max_threads).
    /// And if `max_free_threads > 0` we keep this number of threads even when there are no jobs for them currently.
    while (true)
    {
        /// This is inside the loop to also reset previous thread names set inside the jobs.
        setThreadName(DEFAULT_THREAD_NAME);

        /// Get a job from the queue.
        std::optional<JobWithPriority> job_data;

        {
            Stopwatch watch;
            std::unique_lock lock(parent_pool.mutex);
            ProfileEvents::increment(
                std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolLockWaitMicroseconds : ProfileEvents::LocalThreadPoolLockWaitMicroseconds,
                watch.elapsedMicroseconds());

            // thread just started and we need to push in the job which was requested
            // and we can not take it directly (FIFO + priorities), so just push it to the queue.
            if (new_job)
            {
                ALLOW_ALLOCATIONS_IN_SCOPE;
                parent_pool.jobs.emplace(std::move(*new_job));
                new_job = std::nullopt;
            }

            // Finish with previous job if any
            if (job_is_done)
            {
                job_is_done = false;
                if (exception_from_job)
                {
                    if (!parent_pool.first_exception)
                        parent_pool.first_exception = exception_from_job;
                    if (parent_pool.shutdown_on_exception)
                        parent_pool.shutdown = true;
                    exception_from_job = {};
                }

                --parent_pool.scheduled_jobs;

                parent_pool.job_finished.notify_all();
                if (parent_pool.shutdown)
                    parent_pool.new_job_or_shutdown.notify_all(); /// `shutdown` was set, wake up other threads so they can finish themselves.
            }

            parent_pool.new_job_or_shutdown.wait(lock, [&] {
                return !parent_pool.jobs.empty() || parent_pool.shutdown || parent_pool.threads.size() > std::min(parent_pool.max_threads, parent_pool.scheduled_jobs + parent_pool.max_free_threads);
            });

            if (parent_pool.jobs.empty() || parent_pool.threads.size() > std::min(parent_pool.max_threads, parent_pool.scheduled_jobs + parent_pool.max_free_threads))
            {
                // We enter here if:
                //  - either this thread is not needed anymore due to max_free_threads excess;
                //  - or shutdown happened AND all jobs are already handled.
                if (parent_pool.threads_remove_themselves)
                    removeSelfFromPoolNoPoolLock(); // Detach and remove itself from the pool

                return;
            }

            /// boost::priority_queue does not provide interface for getting non-const reference to an element
            /// to prevent us from modifying its priority. We have to use const_cast to force move semantics on JobWithPriority.
            job_data = std::move(const_cast<JobWithPriority &>(parent_pool.jobs.top()));
            parent_pool.jobs.pop();

            ProfileEvents::increment(
                std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobWaitTimeMicroseconds : ProfileEvents::LocalThreadPoolJobWaitTimeMicroseconds,
                job_data->elapsedMicroseconds());

            /// We don't run jobs after `shutdown` is set, but we have to properly dequeue all jobs and finish them.
            if (parent_pool.shutdown)
            {
                {
                    ALLOW_ALLOCATIONS_IN_SCOPE;
                    /// job can contain packaged_task which can set exception during destruction
                    job_data.reset();
                }
                job_is_done = true;
                continue;
            }
        }

        ALLOW_ALLOCATIONS_IN_SCOPE;

        /// Set up tracing context for this thread by its parent context.
        DB::OpenTelemetry::TracingContextHolder thread_trace_context("ThreadPool::worker()", job_data->thread_trace_context);

        /// Run the job.
        try
        {
            if (DB::Exception::enable_job_stack_trace)
                DB::Exception::setThreadFramePointers(std::move(job_data->frame_pointers));

            CurrentMetrics::Increment metric_active_pool_threads(parent_pool.metric_active_threads);

            if constexpr (!std::is_same_v<Thread, std::thread>)
            {
                Stopwatch watch;
                job_data->job();
                // This metric is less relevant for the global thread pool, as it would show large values (time while
                // a thread was used by local pools) and increment only when local pools are destroyed.
                //
                // In cases where global pool threads are used directly (without a local thread pool), distinguishing
                // them is difficult.
                ProfileEvents::increment(ProfileEvents::LocalThreadPoolBusyMicroseconds, watch.elapsedMicroseconds());
            }
            else
            {
                job_data->job();
            }


            if (thread_trace_context.root_span.isTraceEnabled())
            {
                /// Use the thread name as operation name so that the tracing log will be more clear.
                /// The thread name is usually set in jobs, we can only get the name after the job finishes
                std::string thread_name = getThreadName();
                if (!thread_name.empty() && thread_name != DEFAULT_THREAD_NAME)
                {
                    thread_trace_context.root_span.operation_name = thread_name;
                }
                else
                {
                    /// If the thread name is not set, use the type name of the job instead
                    thread_trace_context.root_span.operation_name = demangle(job_data->job.target_type().name());
                }
            }

            /// job should be reset before decrementing scheduled_jobs to
            /// ensure that the Job destroyed before wait() returns.
            job_data.reset();
        }
        catch (...)
        {
            exception_from_job = std::current_exception();
            thread_trace_context.root_span.addAttribute(exception_from_job);

            /// job should be reset before decrementing scheduled_jobs to
            /// ensure that the Job destroyed before wait() returns.
            job_data.reset();
        }

        job_is_done = true;
    }
}

template class ThreadPoolImpl<std::thread>;
template class ThreadPoolImpl<ThreadFromGlobalPoolImpl<false, true>>;
template class ThreadPoolImpl<ThreadFromGlobalPoolImpl<false, false>>;
template class ThreadFromGlobalPoolImpl<true, true>;
template class ThreadFromGlobalPoolImpl<true, false>;
template class ThreadFromGlobalPoolImpl<false, false>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;


GlobalThreadPool::GlobalThreadPool(
    size_t max_threads_,
    size_t max_free_threads_,
    size_t queue_size_,
    const bool shutdown_on_exception_,
    UInt64 global_profiler_real_time_period_ns_,
    UInt64 global_profiler_cpu_time_period_ns_)
    : FreeThreadPool(
        CurrentMetrics::GlobalThread,
        CurrentMetrics::GlobalThreadActive,
        CurrentMetrics::GlobalThreadScheduled,
        max_threads_,
        max_free_threads_,
        queue_size_,
        shutdown_on_exception_)
    , global_profiler_real_time_period_ns(global_profiler_real_time_period_ns_)
    , global_profiler_cpu_time_period_ns(global_profiler_cpu_time_period_ns_)
{
}

void GlobalThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size, UInt64 global_profiler_real_time_period_ns, UInt64 global_profiler_cpu_time_period_ns)
{
    if (the_instance)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "The global thread pool is initialized twice");
    }

    the_instance.reset(new GlobalThreadPool(max_threads, max_free_threads, queue_size, false /*shutdown_on_exception*/, global_profiler_real_time_period_ns, global_profiler_cpu_time_period_ns));
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
void GlobalThreadPool::shutdown()
{
    if (the_instance)
    {
        the_instance->finalize();
    }
}

CannotAllocateThreadFaultInjector & CannotAllocateThreadFaultInjector::instance()
{
    static CannotAllocateThreadFaultInjector ins;
    return ins;
}

void CannotAllocateThreadFaultInjector::setFaultProbability(double probability)
{
    auto & ins = instance();
    std::lock_guard lock(ins.mutex);
    ins.enabled = 0 < probability && probability <= 1;
    if (ins.enabled)
        ins.random.emplace(probability);
    else
        ins.random.reset();
}

bool CannotAllocateThreadFaultInjector::injectFault()
{
    auto & ins = instance();
    if (!ins.enabled.load(std::memory_order_relaxed))
        return false;

    if (ins.block_fault_injections)
        return false;

    std::lock_guard lock(ins.mutex);
    return ins.random && (*ins.random)(ins.rndgen);
}

thread_local bool CannotAllocateThreadFaultInjector::block_fault_injections = false;

scope_guard CannotAllocateThreadFaultInjector::blockFaultInjections()
{
    auto & ins = instance();
    ins.block_fault_injections = true;
    return [&ins](){ ins.block_fault_injections = false; };
}
