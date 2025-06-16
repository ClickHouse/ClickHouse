#pragma once

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <exception>
#include <future>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result, typename Callback = std::function<Result()>>
using ThreadPoolCallbackRunnerUnsafe = std::function<std::future<Result>(Callback &&, Priority)>;

/// NOTE When using ThreadPoolCallbackRunnerUnsafe you MUST ensure that all async tasks are finished
/// before any objects they may use are destroyed.
/// A common mistake is capturing some some local objects in lambda and passing it to the runner.
/// In case of exception, these local objects will be destroyed before scheduled tasks are finished.

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrowOnError()'.
template <typename Result, typename Callback = std::function<Result()>>
ThreadPoolCallbackRunnerUnsafe<Result, Callback> threadPoolCallbackRunnerUnsafe(ThreadPool & pool, const std::string & thread_name)
{
    return [my_pool = &pool, thread_group = CurrentThread::getGroup(), thread_name](Callback && callback, Priority priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>([thread_group, thread_name, my_callback = std::move(callback)]() mutable -> Result
        {
            ThreadGroupSwitcher switcher(thread_group, thread_name.c_str());

            SCOPE_EXIT_SAFE(
            {
                /// Release all captured resources before detaching thread group
                /// Releasing has to use proper memory tracker which has been set here before callback

                [[maybe_unused]] auto tmp = std::move(my_callback);
            });

            return my_callback();
        });

        auto future = task->get_future();

        /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
        /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
        my_pool->scheduleOrThrowOnError([my_task = std::move(task)]{ (*my_task)(); }, priority);

        return future;
    };
}

template <typename Result, typename T>
std::future<Result> scheduleFromThreadPoolUnsafe(T && task, ThreadPool & pool, const std::string & thread_name, Priority priority = {})
{
    auto schedule = threadPoolCallbackRunnerUnsafe<Result, T>(pool, thread_name);
    return schedule(std::move(task), priority); /// NOLINT
}

/// NOTE It's still not completely safe.
/// When creating a runner on stack, you MUST make sure that it's created (and destroyed) before local objects captured by task lambda.

template <typename Result, typename PoolT = ThreadPool, typename Callback = std::function<Result()>>
class ThreadPoolCallbackRunnerLocal final
{
    static_assert(!std::is_same_v<PoolT, GlobalThreadPool>, "Scheduling tasks directly on GlobalThreadPool is not allowed because it doesn't set up CurrentThread. Create a new ThreadPool (local or in SharedThreadPools.h) or use ThreadFromGlobalPool.");

    PoolT & pool;
    std::string thread_name;

    enum TaskState
    {
        SCHEDULED = 0,
        RUNNING = 1,
        FINISHED = 2,
        CANCELLED = 3,
    };

    struct Task
    {
        std::future<Result> future;
        std::atomic<TaskState> state = SCHEDULED;
    };

    /// NOTE It will leak for a global object with long lifetime
    std::vector<std::shared_ptr<Task>> tasks;

    void cancelScheduledTasks()
    {
        for (auto & task : tasks)
        {
            TaskState expected = SCHEDULED;
            task->state.compare_exchange_strong(expected, CANCELLED);
        }
    }

    /// Set promise result for non-void callbacks
    template <typename Function, typename FunctionResult>
    static void executeCallback(std::promise<FunctionResult> & promise, Function && callback)
    {
        /// Release callback before setting value to the promise to avoid
        /// destruction of captured resources after waitForAllToFinish returns.
        try
        {
            FunctionResult res = callback();
            callback = {};
            promise.set_value(std::move(res));
        }
        catch (...)
        {
            callback = {};
            promise.set_exception(std::current_exception());
        }
    }

    /// Set promise result for void callbacks
    template <typename Function>
    static void executeCallback(std::promise<void> & promise, Function && callback)
    {
        /// Release callback before setting value to the promise to avoid
        /// destruction of captured resources after waitForAllToFinish returns.
        try
        {
            callback();
            callback = {};
            promise.set_value();
        }
        catch (...)
        {
            callback = {};
            promise.set_exception(std::current_exception());
        }
    }

public:
    ThreadPoolCallbackRunnerLocal(PoolT & pool_, const std::string & thread_name_)
        : pool(pool_)
        , thread_name(thread_name_)
    {
    }

    ~ThreadPoolCallbackRunnerLocal()
    {
        cancelScheduledTasks();
        waitForAllToFinish();
    }

    void operator() (Callback && callback, Priority priority = {}, std::optional<uint64_t> wait_microseconds = {})
    {
        auto promise = std::make_shared<std::promise<Result>>();
        auto & task = tasks.emplace_back(std::make_shared<Task>());
        task->future = promise->get_future();

        auto task_func = [task, thread_group = CurrentThread::getGroup(), my_thread_name = thread_name, my_callback = std::move(callback), promise]() mutable -> void
        {
            ThreadGroupSwitcher switcher(thread_group, my_thread_name.c_str());

            TaskState expected = SCHEDULED;
            if (!task->state.compare_exchange_strong(expected, RUNNING))
            {
                if (expected == CANCELLED)
                    return;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when running a task in {}", expected, my_thread_name);
            }

            SCOPE_EXIT_SAFE(
            {
                expected = RUNNING;
                if (!task->state.compare_exchange_strong(expected, FINISHED))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when finishing a task in {}", expected, my_thread_name);
            });

            executeCallback(*promise, std::move(my_callback));
        };

        try
        {
            /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
            /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
            if (wait_microseconds)
                pool.scheduleOrThrow(std::move(task_func), priority, *wait_microseconds);
            else
                pool.scheduleOrThrowOnError(std::move(task_func), priority);
        }
        catch (...)
        {
            promise->set_exception(std::current_exception());
            throw;
        }
    }

    void waitForAllToFinish()
    {
        for (const auto & task : tasks)
        {
            TaskState state = task->state;
            /// It can be cancelled only when waiting in dtor
            if (state == CANCELLED)
                continue;
            if (task->future.valid())
                task->future.wait();
        }
    }

    void waitForAllToFinishAndRethrowFirstError()
    {
        waitForAllToFinish();

        for (auto & task : tasks)
        {
            /// task->future may be invalid if waitForAllToFinishAndRethrowFirstError() is called multiple times
            /// and previous call has already rethrown the exception and has not cleared tasks.
            if (task->future.valid())
                task->future.get();
        }

        tasks.clear();
    }
};

/// Has a task queue and a set of threads from ThreadPool.
/// Per-task overhead is lower than in ThreadPool because ThreadGroup is not switched, stack trace is
/// not propagated, etc.
/// ThreadPool is ok for maybe thousands of tasks per second.
/// ThreadPoolCallbackRunnerFast is ok for maybe tens of thousands.
/// (For hundreds of thousands you'd want a faster queue and some tricks to avoid doing FUTEX_WAKE a lot.
///  But more importantly you'd want to reconsider the design to avoid having such small tasks.)
class ThreadPoolCallbackRunnerFast
{
public:
    enum class Mode
    {
        /// Normal mode, tasks are queued and ran by a thread pool.
        ThreadPool,
        /// Tasks can be enqueued, but there's no thread pool to pick them up.
        /// You have to call runTaskInline() to run them.
        Manual,
        /// operator() will throw.
        Disabled,
    };

    /// TODO [parquet]: Add metrics for queue size and active threads, and maybe event for tasks executed.

    ThreadPoolCallbackRunnerFast();

    void initManual()
    {
        mode = Mode::Manual;
    }

    void initThreadPool(ThreadPool & pool_, size_t max_threads_, std::string thread_name_, ThreadGroupPtr thread_group_);

    /// Manual or Disabled.
    explicit ThreadPoolCallbackRunnerFast(Mode mode_);

    ~ThreadPoolCallbackRunnerFast();

    void shutdown();

    void operator()(std::function<void()> f);

    void bulkSchedule(std::vector<std::function<void()>> fs);

    /// Returns true if a task was run, false if queue is empty.
    bool runTaskInline();

    Mode getMode() const { return mode; }
    size_t getMaxThreads() const { return mode == Mode::ThreadPool ? max_threads : 0; }
    bool isDisabled() const { return mode == Mode::Disabled; }
    bool isManual() const { return mode == Mode::Manual; }

private:
    Mode mode = Mode::Disabled;
    ThreadPool * pool = nullptr;
    size_t max_threads = 0;
    std::string thread_name;
    ThreadGroupPtr thread_group;

    std::mutex mutex;
    size_t threads = 0;
    bool shutdown_requested = false;
    std::condition_variable shutdown_cv;

    std::deque<std::function<void()>> queue;

#ifdef OS_LINUX
    /// Use futex when available. It's faster than condition_variable, especially on the enqueue side.
    std::atomic<UInt32> queue_size {0};
#else
    std::condition_variable queue_cv;
#endif

    void threadFunction();
};

/// Usage:
///
/// struct Foo
/// {
///     std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();
///
///     ~Foo()
///     {
///         shutdown->shutdown();
///
///         // No running background tasks remain.
///         // Some tasks may be left in thread pool queues; these tasks will detect
///         // shutdown and return early without accessing `this`.
///     }
///
///     void someBackgroundTask(std::shared_ptr<ShutdownHelper> shutdown_)
///     {
///         std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
///         if (!shutdown_lock.owns_lock())
///             return; // shutdown was requested, `this` may be destroyed
///
///         // `this` is safe to access as long as `shutdown_lock` is held.
///     }
/// }
///
/// Fun fact: ShutdownHelper can almost be replaced with std::shared_mutex.
/// Background tasks would do try_lock_shared(). Shutdown would do lock() and never unlock.
/// Alas, std::shared_mutex::try_lock_shared() is allowed to spuriously fail, so this doesn't work.
/// (In Common/SharedMutex.h, the futex-based implementation has reliable try_lock_shared(), but the
/// fallback absl implementation can fail spuriously. In Common/CancelableSharedMutex.h there's
/// another suitable futex-based linux-only implementation.)
class ShutdownHelper
{
public:
    bool try_lock_shared();
    void unlock_shared();

    /// For re-checking in the middle of long-running operation while already holding a lock.
    bool shutdown_requested();

    /// Returns false if shutdown was already requested before.
    bool begin_shutdown();
    void wait_shutdown();

    /// Equivalent to `begin_shutdown(); end_shutdown();`. Ok to call multiple times.
    void shutdown();

private:
    static constexpr Int64 SHUTDOWN_START = 1l << 42; // shutdown requested
    static constexpr Int64 SHUTDOWN_END = 1l << 52; // no shared locks remain

    /// If >= SHUTDOWN_START, no new try_lock_shared() calls will succeed.
    /// Whoever changes the value to exactly SHUTDOWN_START (i.e. shutdown requested, no shared locks)
    /// must then add SHUTDOWN_END to it.
    /// Note that SHUTDOWN_END might be added multiple times because of benign race conditions.
    std::atomic<Int64> val {0};
    std::mutex mutex;
    std::condition_variable cv;
};

extern template ThreadPoolCallbackRunnerUnsafe<void> threadPoolCallbackRunnerUnsafe<void>(ThreadPool &, const std::string &);
extern template class ThreadPoolCallbackRunnerLocal<void>;

}
