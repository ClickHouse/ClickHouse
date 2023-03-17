#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/noexcept_scope.h>

#include <cassert>
#include <iostream>
#include <type_traits>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>

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
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl()
    : ThreadPoolImpl(getNumberOfPhysicalCPUCores())
{
}


template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads_)
    : ThreadPoolImpl(max_threads_, max_threads_, max_threads_)
{
}

template <typename Thread>
ThreadPoolImpl<Thread>::ThreadPoolImpl(size_t max_threads_, size_t max_free_threads_, size_t queue_size_, bool shutdown_on_exception_)
    : max_threads(max_threads_)
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
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, ssize_t priority, std::optional<uint64_t> wait_microseconds, bool propagate_opentelemetry_tracing_context)
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

    {
        std::unique_lock lock(mutex);

        auto pred = [this] { return !queue_size || scheduled_jobs < queue_size || shutdown; };

        if (wait_microseconds)  /// Check for optional. Condition is true if the optional is set and the value is zero.
        {
            if (!job_finished.wait_for(lock, std::chrono::microseconds(*wait_microseconds), pred))
                return on_error(fmt::format("no free thread (timeout={})", *wait_microseconds));
        }
        else
            job_finished.wait(lock, pred);

        if (shutdown)
            return on_error("shutdown");

        /// We must not to allocate any memory after we emplaced a job in a queue.
        /// Because if an exception would be thrown, we won't notify a thread about job occurrence.

        /// Check if there are enough threads to process job.
        if (threads.size() < std::min(max_threads, scheduled_jobs + 1))
        {
            try
            {
                threads.emplace_front();
            }
            catch (...)
            {
                /// Most likely this is a std::bad_alloc exception
                return on_error("cannot allocate thread slot");
            }

            try
            {
                threads.front() = Thread([this, it = threads.begin()] { worker(it); });
            }
            catch (...)
            {
                threads.pop_front();
                return on_error("cannot allocate thread");
            }
        }

        jobs.emplace(std::move(job),
                     priority,
                     /// Tracing context on this thread is used as parent context for the sub-thread that runs the job
                     propagate_opentelemetry_tracing_context ? DB::OpenTelemetry::CurrentContext() : DB::OpenTelemetry::TracingContextOnThread());

        ++scheduled_jobs;
    }

    new_job_or_shutdown.notify_one();

    return static_cast<ReturnType>(true);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::startNewThreadsNoLock()
{
    /// Start new threads while there are more scheduled jobs in the queue and the limit `max_threads` is not reached.
    while (threads.size() < std::min(scheduled_jobs, max_threads))
    {
        try
        {
            threads.emplace_front();
        }
        catch (...)
        {
            break; /// failed to start more threads
        }

        try
        {
            threads.front() = Thread([this, it = threads.begin()] { worker(it); });
        }
        catch (...)
        {
            threads.pop_front();
            break; /// failed to start more threads
        }
    }
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrowOnError(Job job, ssize_t priority)
{
    scheduleImpl<void>(std::move(job), priority, std::nullopt);
}

template <typename Thread>
bool ThreadPoolImpl<Thread>::trySchedule(Job job, ssize_t priority, uint64_t wait_microseconds) noexcept
{
    return scheduleImpl<bool>(std::move(job), priority, wait_microseconds);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::scheduleOrThrow(Job job, ssize_t priority, uint64_t wait_microseconds, bool propagate_opentelemetry_tracing_context)
{
    scheduleImpl<void>(std::move(job), priority, wait_microseconds, propagate_opentelemetry_tracing_context);
}

template <typename Thread>
void ThreadPoolImpl<Thread>::wait()
{
    std::unique_lock lock(mutex);
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
    std::unique_lock lock(mutex);
    shutdown = true;
    new_job_or_shutdown.notify_all(); /// `shutdown` was set

    /// Wait for all currently running jobs to finish (we don't wait for all scheduled jobs here like the function wait() does).
    /// We cannot call thread.join() for each thread here because after a thread finishes it will remove itself from `threads`
    /// (see `threads.erase(thread_it)` in the worker() function).
    thread_finished.wait(lock, [this] { return threads.empty(); });
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
void ThreadPoolImpl<Thread>::worker(typename std::list<Thread>::iterator thread_it)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    CurrentMetrics::Increment metric_all_threads(
        std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThread : CurrentMetrics::LocalThread);

    /// Run jobs while there are scheduled jobs and until some special event occurs (e.g. shutdown, or decreasing the number of max_threads).
    /// And if `max_free_threads > 0` we keep this number of threads even when there are no jobs for them currently.
    while (true)
    {
        /// This is inside the loop to also reset previous thread names set inside the jobs.
        setThreadName("ThreadPool");

        Job job;
        std::exception_ptr exception_from_job;

        /// A copy of parent trace context
        DB::OpenTelemetry::TracingContextOnThread parent_thead_trace_context;

        {
            std::unique_lock lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return !jobs.empty() || shutdown || (threads.size() > std::min(max_threads, scheduled_jobs + max_free_threads)); });

            if (shutdown || (threads.size() > std::min(max_threads, scheduled_jobs + max_free_threads)))
            {
                thread_it->detach();
                threads.erase(thread_it);
                thread_finished.notify_all();
                return;
            }

            chassert(!jobs.empty());
            /// boost::priority_queue does not provide interface for getting non-const reference to an element
            /// to prevent us from modifying its priority. We have to use const_cast to force move semantics on JobWithPriority::job.
            job = std::move(const_cast<Job &>(jobs.top().job));
            parent_thead_trace_context = std::move(const_cast<DB::OpenTelemetry::TracingContextOnThread &>(jobs.top().thread_trace_context));
            jobs.pop();
        }

        {
            ALLOW_ALLOCATIONS_IN_SCOPE;

            /// Set up tracing context for this thread by its parent context
            DB::OpenTelemetry::TracingContextHolder thread_trace_context("ThreadPool::worker()", parent_thead_trace_context);

            try
            {
                CurrentMetrics::Increment metric_active_threads(
                    std::is_same_v<Thread, std::thread> ? CurrentMetrics::GlobalThreadActive : CurrentMetrics::LocalThreadActive);

                job();

                if (thread_trace_context.root_span.isTraceEnabled())
                {
                    /// Use the thread name as operation name so that the tracing log will be more clear.
                    /// The thread name is usually set in the jobs, we can only get the name after the job finishes
                    std::string thread_name = getThreadName();
                    if (!thread_name.empty())
                        thread_trace_context.root_span.operation_name = thread_name;
                }
            }
            catch (...)
            {
                exception_from_job = std::current_exception();
                thread_trace_context.root_span.addAttribute(exception_from_job);
            }
        }

        /// job should be reset before decrementing scheduled_jobs to
        /// ensure that the Job destroyed before wait() returns.
        job = {};
        parent_thead_trace_context.reset();

        {
            std::lock_guard lock(mutex);
            if (exception_from_job)
            {
                if (!first_exception)
                    first_exception = exception_from_job;
                if (shutdown_on_exception)
                    shutdown = true;
            }

            --scheduled_jobs;

            job_finished.notify_all();
            if (shutdown)
                new_job_or_shutdown.notify_all();
        }
    }
}


template class ThreadPoolImpl<std::thread>;
template class ThreadPoolImpl<ThreadFromGlobalPoolImpl<false>>;
template class ThreadFromGlobalPoolImpl<true>;

std::unique_ptr<GlobalThreadPool> GlobalThreadPool::the_instance;

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
