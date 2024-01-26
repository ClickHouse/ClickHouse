#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/noexcept_scope.h>

#include <cassert>
#include <type_traits>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <base/demangle.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>


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
    extern const Event GlobalThreadPoolJobScheduleMicroseconds;
    extern const Event GlobalThreadPoolThreadCreationMicroseconds;
    extern const Event GlobalThreadPoolJobScheduleLockWaitMicroseconds;
    extern const Event GlobalThreadPoolJobEmplacementMicroseconds;
    extern const Event GlobalThreadPoolJobsCounter;
    extern const Event GlobalThreadPoolCondVarWaitingMicroseconds;
    extern const Event GlobalThreadPoolWorkerLoops;
    extern const Event GlobalThreadPoolWorkerLockWaitMicroseconds;
    extern const Event GlobalThreadPoolWorkerLockHoldingMicroseconds;


    extern const Event LocalThreadPoolExpansions;
    extern const Event LocalThreadPoolShrinks;
    extern const Event LocalThreadPoolJobScheduleMicroseconds;
    extern const Event LocalThreadPoolThreadCreationMicroseconds;
    extern const Event LocalThreadPoolJobScheduleLockWaitMicroseconds;
    extern const Event LocalThreadPoolJobEmplacementMicroseconds;
    extern const Event LocalThreadPoolJobsCounter;
    extern const Event LocalThreadPoolCondVarWaitingMicroseconds;
    extern const Event LocalThreadPoolWorkerLoops;
    extern const Event LocalThreadPoolWorkerLockWaitMicroseconds;
    extern const Event LocalThreadPoolWorkerLockHoldingMicroseconds;

}

class JobWithPriority
{
public:
    using Job = std::function<void()>;

    Job job;
    Priority priority;
    CurrentMetrics::Increment metric_increment;
    DB::OpenTelemetry::TracingContextOnThread thread_trace_context;

    /// Call stacks of all jobs' schedulings leading to this one
    std::vector<StackTrace::FramePointers> frame_pointers;
    bool enable_job_stack_trace = false;

    JobWithPriority(
        Job job_, Priority priority_, CurrentMetrics::Metric metric,
        const DB::OpenTelemetry::TracingContextOnThread & thread_trace_context_,
        bool capture_frame_pointers)
        : job(job_), priority(priority_), metric_increment(metric),
        thread_trace_context(thread_trace_context_), enable_job_stack_trace(capture_frame_pointers)
    {
        if (!capture_frame_pointers)
            return;
        /// Save all previous jobs call stacks and append with current
        frame_pointers = DB::Exception::thread_frame_pointers;
        frame_pointers.push_back(StackTrace().getFramePointers());
    }

    bool operator<(const JobWithPriority & rhs) const
    {
        return priority > rhs.priority; // Reversed for `priority_queue` max-heap to yield minimum value (i.e. highest priority) first
    }
};

static constexpr auto DEFAULT_THREAD_NAME = "ThreadPool";
static constexpr const size_t GLOBAL_THREAD_POOL_MIN_FREE_THREADS = 12;
static constexpr const size_t GLOBAL_THREAD_POOL_EXPANSION_STEP = 8;
static constexpr const size_t GLOBAL_THREAD_POOL_HOUSEKEEP_INTERVAL_MILLISECONDS = 10000; // 10 seconds
static constexpr const size_t GLOBAL_THREAD_POOL_HOUSEKEEP_HISTORY_WINDOW_SECONDS = 600;  // 10 minutes
static constexpr const size_t GLOBAL_THREAD_POOL_EXPANSION_THREADS = 1;

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(Metric metric_threads_, Metric metric_active_threads_, Metric metric_scheduled_jobs_)
    : ThreadPoolImpl(metric_threads_, metric_active_threads_, metric_scheduled_jobs_, getNumberOfPhysicalCPUCores())
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
    calculateDesiredThreadPoolSizeNoLock();
    jobs.reserve(queue_size ? queue_size : desired_pool_size.load());
    // LOG_ERROR(&Poco::Logger::get("ThreadPoolImpl"),
    //               "ThreadPoolImpl constructor [Instance Address: {}]: max_threads = {}, max_free_threads = {}, queue_size = {}, StackTrace: {}",
    //               static_cast<void*>(this), max_threads, max_free_threads, queue_size, StackTrace().toString());

    if constexpr (std::is_same_v<Thread, std::thread>) // global thread pool
    {
        {
            std::lock_guard lock(threads_mutex);

            // for global thread pool we need to start some housekeeper threads synchronously
            // they will run during the whole lifetime of the global thread pool
            threads.emplace_front(&ThreadPoolImpl<Thread>::threadPoolHousekeep, this);

            for (size_t i = 0; i < GLOBAL_THREAD_POOL_EXPANSION_THREADS; ++i)
            {
                threads.emplace_front(&ThreadPoolImpl<Thread>::threadPoolGrow, this);
            }
        }
        startThreads(/* async = */ true);
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setMaxThreads(size_t value)
{
    {
        std::lock_guard lock(mutex);

        max_threads = value;
        max_free_threads = std::min(max_free_threads, max_threads);

        /// We have to also adjust queue size, because it limits the number of scheduled and already running jobs in total.
        queue_size = queue_size ? std::max(queue_size, max_threads) : 0;

        calculateDesiredThreadPoolSizeNoLock();
        jobs.reserve(queue_size ? queue_size : desired_pool_size.load());

    }
    adjustThreadPoolSize();

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
    {
        std::lock_guard lock(mutex);
        max_free_threads = std::min(value, max_threads);
        calculateDesiredThreadPoolSizeNoLock();
    }
    adjustThreadPoolSize();
}

template <typename Thread>
void ThreadPoolImpl<Thread>::setQueueSize(size_t value)
{
    std::lock_guard lock(mutex);
    queue_size = value ? std::max(value, max_threads) : 0;
    calculateDesiredThreadPoolSizeNoLock();
    jobs.reserve(queue_size ? queue_size : desired_pool_size.load());
}

template <typename Thread>
void ThreadPoolImpl<Thread>::removeThread(std::list<Thread>::iterator thread_it)
{
    thread_it->detach();
    threads.erase(thread_it);
    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolShrinks : ProfileEvents::LocalThreadPoolShrinks
    );
    current_pool_size--;
}

template <typename Thread>
void ThreadPoolImpl<Thread>::createThread()
{
    try
    {
        // Placeholder for the thread_it_holder function
        ThreadIterarorHolder thread_it_holder{};

        // create thread outside of the critical section
        // this way threads can be created in parallel

        Stopwatch watch;

        // we pass thread_it_holder function by reference to the thread
        auto thread = Thread(&ThreadPoolImpl<Thread>::worker, this, thread_it_holder);
        ProfileEvents::increment(
            std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolThreadCreationMicroseconds : ProfileEvents::LocalThreadPoolThreadCreationMicroseconds,
            watch.elapsedMicroseconds());

        std::lock_guard lock(threads_mutex);
        threads.push_front(std::move(thread));
        thread_it_holder.set( threads.begin() );

        current_pool_size++;
    }
    catch (const std::exception & e)
    {
        throw DB::Exception(DB::ErrorCodes::CANNOT_SCHEDULE_TASK, "cannot start thread: {})", e.what());
    }
    catch (...)
    {
        throw DB::Exception(DB::ErrorCodes::CANNOT_SCHEDULE_TASK, "cannot start thread");
    }
}


template <typename Thread>
void ThreadPoolImpl<Thread>::startThreads(bool async)
{
    // global thread pool expansion can be very expensive (under load even 1 thread creation can take 100ms-1000ms)
    // so it's better to make that work async in the background by special threads which will be waken up for starting new threads
    //
    // in contrast, local thread pool expansion is very cheap (it's just pushing a new job to a global pool to borrow a thread)
    // and local thread pools are usually small and we don't want them to have excessive threads just to start new ones
    //
    // so for the global thread pool we just signal TPoolGrow threads to start a new thread(s) via condition variable
    // and for the local thread pool we 'start' a new thread here

    if (async)
    {
        if (current_pool_size >= desired_pool_size)
            return;
        pool_grow_thread_cv.notify_all();
    }
    else // if (async)
    {
        // because we don't hold the lock here between check and actual thread creation
        // we can end up with more threads than desired_pool_size, but that's ok
        while (desired_pool_size > current_pool_size)
        {
            createThread();
        }
    }
}


template <typename Thread>
void ThreadPoolImpl<Thread>::adjustThreadPoolSize()
{
    if (current_pool_size < desired_pool_size)
    {
        startThreads(std::is_same_v<Thread, std::thread> /* async for global thread pool */);
    }
    else if (current_pool_size > desired_pool_size)
    {
        /// Wake up free threads so they can finish themselves.
        new_job_or_shutdown.notify_all(); // todo: notify one by one in loop?
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::calculateDesiredThreadPoolSizeNoLock()
{
    // todo: we can monitor here the trend and try to extrapolate it to predict the required number of threads in the future

    if (desired_pool_size > max_threads)
    {
        desired_pool_size = max_threads;
    }
    else if (desired_pool_size < scheduled_jobs + (std::is_same_v<Thread, std::thread> ? GLOBAL_THREAD_POOL_MIN_FREE_THREADS : 0))
    {
        desired_pool_size = std::min(max_threads, scheduled_jobs + (std::is_same_v<Thread, std::thread> ? GLOBAL_THREAD_POOL_EXPANSION_STEP : 0));
    }
    else
    {
        /// our desired_pool_size should be at least as big as minimum utilization over last 10 minutes
        /// and at least as small as maximum utilization over last 10 minutes
        


        // we are in allowed range, let's try to guess the optimal number of threads based on the current & history of utilization
        // we want to have some free threads in the pool, but not too many

    }
    // todo: we need to shrink the pool if there are too many free threads

}






template <typename Thread>
template <typename ReturnType>
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, Priority priority, std::optional<uint64_t> wait_microseconds, bool propagate_opentelemetry_tracing_context)
{
    Stopwatch watch;
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
                current_pool_size, scheduled_jobs);
        }
        else
            return false;
    };

    {
        std::unique_lock lock(mutex);
        ProfileEvents::increment(
            std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobScheduleLockWaitMicroseconds : ProfileEvents::LocalThreadPoolJobScheduleLockWaitMicroseconds,
            watch.elapsedMicroseconds());

        Stopwatch watch2;
        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        /// if (wait_microseconds.has_value() && wait_microseconds.value() > 0)

        if (wait_microseconds)  /// Check for optional. Condition is also true if the optional has value=0.
        {
            if (!job_finished.wait_for(lock, std::chrono::microseconds(*wait_microseconds), pred))
                return on_error(fmt::format("no free thread (timeout={})", *wait_microseconds));
        }
        else
            job_finished.wait(lock, pred);

        ProfileEvents::increment(
            std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolCondVarWaitingMicroseconds : ProfileEvents::LocalThreadPoolCondVarWaitingMicroseconds,
            watch2.elapsedMicroseconds());

        if (shutdown)
            return on_error("shutdown");

        /// We must not to allocate any memory after we emplaced a job in a queue.
        /// Because if an exception would be thrown, we won't notify a thread about job occurrence.

        /// Check if there are enough threads to process job.
        if (desired_pool_size < std::min(max_threads, scheduled_jobs + 1))
        {
            desired_pool_size = std::min(max_threads, scheduled_jobs + 1);
        }

        Stopwatch watch3;
        jobs.emplace(std::move(job),
                     priority,
                     metric_scheduled_jobs,
                     /// Tracing context on this thread is used as parent context for the sub-thread that runs the job
                     propagate_opentelemetry_tracing_context ? DB::OpenTelemetry::CurrentContext() : DB::OpenTelemetry::TracingContextOnThread(),
                     /// capture_frame_pointers
                     DB::Exception::enable_job_stack_trace);

        ProfileEvents::increment(
            std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobEmplacementMicroseconds : ProfileEvents::LocalThreadPoolJobEmplacementMicroseconds,
            watch3.elapsedMicroseconds());

        ++scheduled_jobs;

        current_utilization_record.update(scheduled_jobs);
    }

    if (current_pool_size < desired_pool_size)
    {
        try
        {
            startThreads(std::is_same_v<Thread, std::thread>); // /* async for global thread pool */
        }
        catch (const DB::Exception & e)
        {
            return on_error(fmt::format("cannot start threads: {}", e.what()));
        }
        catch (...)
        {
            return on_error("cannot start threads");
        }
    }

    /// Wake up a free thread to run the new job.
    new_job_or_shutdown.notify_one();

    ProfileEvents::increment(
        std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobScheduleMicroseconds : ProfileEvents::LocalThreadPoolJobScheduleMicroseconds,
        watch.elapsedMicroseconds());

    ProfileEvents::increment( std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolJobsCounter : ProfileEvents::LocalThreadPoolJobsCounter);

    return static_cast<ReturnType>(true);
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
    /// Signal here just in case.
    /// If threads are waiting on condition variables, but there are some jobs in the queue
    /// then it will prevent us from deadlock.
    new_job_or_shutdown.notify_all(); // TODO: is all really needed here?

    std::unique_lock lock(mutex);
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
    // LOG_ERROR(&Poco::Logger::get("ThreadPoolImpl"),
    //               "ThreadPoolImpl destructor [Instance Address: {}]: threads.size() = {}",
    //               static_cast<void*>(this), threads.size());

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
    desired_pool_size = 0;

    /// Wake up different types of threads so they can react on shutdown and finish themselves.
    housekeeping_thread_cv.notify_all();
    pool_grow_thread_cv.notify_all();
    new_job_or_shutdown.notify_all();

    {
        std::lock_guard lock(threads_mutex);

        /// Wait for all currently running jobs to finish (we don't wait for all scheduled jobs here like the function wait() does).
        for (auto & thread : threads)
        {
            thread.join();

            ProfileEvents::increment(
                std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolShrinks : ProfileEvents::LocalThreadPoolShrinks
            );
        }

        threads.clear();
        current_pool_size = 0;
    }
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
void ThreadPoolImpl<Thread>::threadPoolHousekeep()
{
    setThreadName("TPoolHousekeep");
    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);

            // Wait for notification or timeout
            if (housekeeping_thread_cv.wait_for(
                    lock,
                    std::chrono::milliseconds(GLOBAL_THREAD_POOL_HOUSEKEEP_INTERVAL_MILLISECONDS),
                    [this]{ return shutdown || desired_pool_size != current_pool_size; }
                ))
            {
                if (shutdown)
                    return;
            }
            else // timer expired
            {
                try
                {
                    utilization_history.push_back(current_utilization_record);
                    current_utilization_record = UtilizationRecord(scheduled_jobs);

                    // Remove old records outside the monitoring window (e.g., last 10 minutes)
                    auto window_start = current_utilization_record.timestamp - std::chrono::seconds(GLOBAL_THREAD_POOL_HOUSEKEEP_HISTORY_WINDOW_SECONDS);
                    while (!utilization_history.empty() && utilization_history.front().timestamp < window_start)
                    {
                        utilization_history.pop_front();
                    }

                    auto maximum_utilization = scheduled_jobs;

                    for(const auto & record : utilization_history)
                    {
                        maximum_utilization = std::max(maximum_utilization, record.max_scheduled_jobs);
                    }

                    if (maximum_utilization + GLOBAL_THREAD_POOL_MIN_FREE_THREADS < desired_pool_size)
                    {
                        desired_pool_size = std::max(maximum_utilization + GLOBAL_THREAD_POOL_MIN_FREE_THREADS, scheduled_jobs + max_free_threads);
                        calculateDesiredThreadPoolSizeNoLock();
                    }
                }
                catch (...)
                {
                    // memory error or similar, but we still want thread to continue working
                    LOG_ERROR(&Poco::Logger::get("ThreadPoolImpl"),
                        "ThreadPoolImpl::threadPoolHousekeep(): exception while updating utilization history");
                }

                // TODO: check if we have to shrink the pool

            }
        }

    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::threadPoolGrow()
{
    setThreadName("TPoolGrow");
    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            // Wait for notification or timeout
            pool_grow_thread_cv.wait(lock, [this]{ return shutdown || desired_pool_size > current_pool_size; });
            if (shutdown)
                return;
        }
        try
        {
            startThreads(/* async = */ false);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("ThreadPoolImpl"),
                "ThreadPoolImpl::threadPoolGrow(): exception while starting threads: {}",
                e.what());
        }
        catch (...)
        {
            LOG_ERROR(&Poco::Logger::get("ThreadPoolImpl"),
                "ThreadPoolImpl::threadPoolGrow(): unknown exception while starting threads");
        }
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::worker(ThreadIterarorHolder thread_it_holder)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    CurrentMetrics::Increment metric_pool_threads(metric_threads);

    bool job_is_done = false;
    bool thread_is_not_needed_anymore = false;
    bool thread_should_remove_itself = false;

    std::exception_ptr exception_from_job;

    /// We'll run jobs in this worker while there are scheduled jobs and until some special event occurs (e.g. shutdown, or decreasing the number of max_threads).
    /// And if `max_free_threads > 0` we keep this number of threads even when there are no jobs for them currently.
    while (true)
    {
        /// This is inside the loop to also reset previous thread names set inside the jobs.
        setThreadName(DEFAULT_THREAD_NAME);

        ProfileEvents::increment( std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolWorkerLoops : ProfileEvents::LocalThreadPoolWorkerLoops);


        /// Get a job from the queue.
        std::optional<JobWithPriority> job_data;

        {
            Stopwatch watch;
            std::unique_lock lock(mutex);
            ProfileEvents::increment(
                std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolWorkerLockWaitMicroseconds : ProfileEvents::LocalThreadPoolWorkerLockWaitMicroseconds,
                watch.elapsedMicroseconds());

            watch.restart();

            // Finish with previous job if any
            if (job_is_done)
            {
                job_is_done = false;
                if (exception_from_job)
                {
                    if (!first_exception)
                        first_exception = exception_from_job;
                    if (shutdown_on_exception)
                    {
                        shutdown = true;
                        desired_pool_size = 0;
                    }
                    exception_from_job = {};
                }

                --scheduled_jobs;
                current_utilization_record.update(scheduled_jobs);

                job_finished.notify_all();
                if (shutdown) // TODO: if shutdown was set by finalize(), we already notified all threads, so this is redundant, right?
                              // but that can be also set by the job on exception, so we have to notify all threads in that case
                              // also we notify those treads with lock held, so here they wake up and do a lot of lock contentions
                              // so probably that should be optimized
                              // also we can have several loop iteration with shutdown=true...
                    new_job_or_shutdown.notify_all(); /// `shutdown` was set, wake up other threads so they can finish themselves.
            }

            new_job_or_shutdown.wait(lock, [&] { return !jobs.empty() || shutdown || desired_pool_size < current_pool_size; });

            if (jobs.empty() || desired_pool_size < current_pool_size )
            {
                // We enter here if:
                //  - either this thread is not needed anymore due to max_free_threads excess;
                //  - or shutdown happened AND all jobs are already handled or skipped

                // we will remove this thread from the pool after leaving the critical section
                thread_is_not_needed_anymore = true;
                // we will unsafe to access thread_should_remove_itself after leaving the critical section
                thread_should_remove_itself = threads_remove_themselves;
            }
            else
            {
                /// boost::priority_queue does not provide interface for getting non-const reference to an element
                /// to prevent us from modifying its priority. We have to use const_cast to force move semantics on JobWithPriority.
                job_data = std::move(const_cast<JobWithPriority &>(jobs.top()));
                jobs.pop();

                /// We don't run jobs after `shutdown` is set, but we have to properly dequeue all jobs and finish them.
                if (shutdown)
                {
                    job_is_done = true;
                    continue;
                }
            }
            ProfileEvents::increment(
                std::is_same_v<Thread, std::thread> ? ProfileEvents::GlobalThreadPoolWorkerLockHoldingMicroseconds : ProfileEvents::LocalThreadPoolWorkerLockHoldingMicroseconds,
                watch.elapsedMicroseconds());
        }

        if (thread_is_not_needed_anymore)
        {
            if (thread_should_remove_itself)
            {
                std::lock_guard lock(threads_mutex);
                if (thread_it_holder.thread_it.has_value()) {
                    removeThread(thread_it_holder.thread_it.value());
                }
            }

            return;
        }

        ALLOW_ALLOCATIONS_IN_SCOPE;

        /// Set up tracing context for this thread by its parent context.
        DB::OpenTelemetry::TracingContextHolder thread_trace_context("ThreadPool::worker()", job_data->thread_trace_context);

        /// Run the job.
        try
        {
            if (DB::Exception::enable_job_stack_trace)
                DB::Exception::thread_frame_pointers = std::move(job_data->frame_pointers);

            CurrentMetrics::Increment metric_active_pool_threads(metric_active_threads);

            job_data->job();

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
template class ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>;
template class ThreadFromGlobalPoolImpl<true>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;


GlobalThreadPool::GlobalThreadPool(
    size_t max_threads_,
    size_t max_free_threads_,
    size_t queue_size_,
    const bool shutdown_on_exception_)
    : FreeThreadPool(
        CurrentMetrics::GlobalThread,
        CurrentMetrics::GlobalThreadActive,
        CurrentMetrics::GlobalThreadScheduled,
        max_threads_,
        max_free_threads_,
        queue_size_,
        shutdown_on_exception_)
{
}

void GlobalThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (the_instance)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "The global thread pool is initialized twice");
    }

    the_instance.reset(new GlobalThreadPool(max_threads, max_free_threads, queue_size, false /*shutdown_on_exception*/));
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
