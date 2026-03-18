#pragma once

#include <condition_variable>
#include <concepts>
#include <exception>
#include <memory>
#include <map>
#include <mutex>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Priority.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/Logger.h>


namespace Poco { class Logger; }

namespace DB
{

// TERMINOLOGY:
// Job (`LoadJob`) - The smallest part of loading process, executed by worker. Job can depend on the other jobs. Jobs are grouped in tasks.
// Task (`LoadTask`) - Owning holder of a set of jobs. Should be held during the whole job lifetime. Cancels all jobs on destruction.
// Goal jobs (goals) - a subset of "final" jobs of a task (usually no job in task depend on a goal job).
//      By default all jobs in task are included in goal jobs.
//      Goals should used if you need to create a job that depends on a task (to avoid placing all jobs of the task in dependencies).
// Pool (worker pool) - A set of workers with specific priority. Every job is assigned to a pool. Job can change its pool dynamically.
// Priority (pool priority) - Constant integer value showing relative priority of a pool. Lower value means higher priority.
// AsyncLoader - scheduling system responsible for job dependency tracking and worker management respecting pool priorities.

class LoadJob;
using LoadJobPtr = std::shared_ptr<LoadJob>;
using LoadJobSet = std::unordered_set<LoadJobPtr>;
class LoadTask;
using LoadTaskPtr = std::shared_ptr<LoadTask>;
using LoadTaskPtrs = std::vector<LoadTaskPtr>;
class AsyncLoader;

void logAboutProgress(LoggerPtr log, size_t processed, size_t total, AtomicStopwatch & watch);

// Execution status of a load job.
enum class LoadStatus : uint8_t
{
    PENDING,  // Load job is not started yet.
    OK,       // Load job executed and was successful.
    FAILED,   // Load job executed and failed.
    CANCELED  // Load job is not going to be executed due to removal or dependency failure.
};

// Smallest indivisible part of a loading process. Load job can have multiple dependencies, thus jobs constitute a direct acyclic graph (DAG).
// Job encapsulates a function to be executed by `AsyncLoader` as soon as job functions of all dependencies are successfully executed.
// Job can be waited for by an arbitrary number of threads. See `AsyncLoader` class description for more details.
// WARNING: jobs are usually held with ownership by tasks (see `LoadTask`). You are encouraged to add jobs into a tasks as soon as the are created.
class LoadJob : private boost::noncopyable
{
public:
    // NOTE: makeLoadJob() helper should be used instead of direct ctor call
    template <class LoadJobSetType, class DFFunc, class Func>
    LoadJob(LoadJobSetType && dependencies_, String name_, size_t pool_id_, DFFunc && dependency_failure_, Func && func_)
        : dependencies(std::forward<LoadJobSetType>(dependencies_))
        , name(std::move(name_))
        , execution_pool_id(pool_id_)
        , pool_id(pool_id_)
        , dependency_failure(std::forward<DFFunc>(dependency_failure_))
        , func(std::forward<Func>(func_))
    {}

    // NOTE: makeLoadJob() helper should be used instead of direct ctor call
    template <class LoadJobSetType, class WIFunc, class WDFunc, class DFFunc, class Func>
    LoadJob(LoadJobSetType && dependencies_, String name_, size_t pool_id_, WIFunc && on_waiters_increment_, WDFunc && on_waiters_decrement_, DFFunc && dependency_failure_, Func && func_)
        : dependencies(std::forward<LoadJobSetType>(dependencies_))
        , name(std::move(name_))
        , execution_pool_id(pool_id_)
        , pool_id(pool_id_)
        , on_waiters_increment(std::forward<WIFunc>(on_waiters_increment_))
        , on_waiters_decrement(std::forward<WDFunc>(on_waiters_decrement_))
        , dependency_failure(std::forward<DFFunc>(dependency_failure_))
        , func(std::forward<Func>(func_))
    {}

    // Current job status.
    LoadStatus status() const;
    std::exception_ptr exception() const;

    // Returns pool in which the job is executing (was executed). May differ from initial pool and from current pool.
    // Value is only valid (and constant) after execution started.
    size_t executionPool() const;

    // Returns current pool of the job. May differ from initial and execution pool.
    // This value is intended for creating new jobs during this job execution.
    // Value may change during job execution by `prioritize()`.
    size_t pool() const;

    // Returns number of threads blocked by `wait()` calls.
    size_t waitersCount() const;

    // Introspection
    using TimePoint = std::chrono::system_clock::time_point;
    UInt64 jobId() const { return job_id; }
    TimePoint scheduleTime() const { return schedule_time; }
    TimePoint enqueueTime() const { return enqueue_time; }
    TimePoint startTime() const { return start_time; }
    TimePoint finishTime() const { return finish_time; }

    const LoadJobSet dependencies; // Jobs to be done before this one (with ownership), it is `const` to make creation of cycles hard
    const String name;

private:
    friend class AsyncLoader;

    void ok();
    void failed(const std::exception_ptr & ptr);
    void canceled(const std::exception_ptr & ptr);
    void finish();

    void scheduled(UInt64 job_id_);
    void enqueued();
    void execute(AsyncLoader & loader, size_t pool, const LoadJobPtr & self);

    std::atomic<UInt64> job_id{0};
    std::atomic<size_t> execution_pool_id;
    std::atomic<size_t> pool_id;

    // Handlers that is called by every new waiting thread, just before going to sleep.
    // If `on_waiters_increment` throws, then wait is canceled, and corresponding `on_waiters_decrement` will never be called.
    // It can be used for counting and limits on number of waiters.
    // Note that implementations are called under `LoadJob::mutex` and should be fast.
    std::function<void(const LoadJobPtr & self)> on_waiters_increment;
    std::function<void(const LoadJobPtr & self)> on_waiters_decrement;

    // Handler for failed or canceled dependencies.
    // If job needs to be canceled on `dependency` failure, then function should set `cancel` to a specific reason.
    // Note that implementation should be fast and cannot use AsyncLoader, because it is called under `AsyncLoader::mutex`.
    // Note that `dependency_failure` is called only on pending jobs.
    std::function<void(const LoadJobPtr & self, const LoadJobPtr & dependency, std::exception_ptr & cancel)> dependency_failure;

    // Function to be called to execute the job.
    std::function<void(AsyncLoader & loader, const LoadJobPtr & self)> func;

    mutable std::mutex mutex;
    mutable std::condition_variable finished;
    mutable size_t waiters = 0;
    LoadStatus load_status{LoadStatus::PENDING};
    std::exception_ptr load_exception;

    std::atomic<TimePoint> schedule_time{TimePoint{}};
    std::atomic<TimePoint> enqueue_time{TimePoint{}};
    std::atomic<TimePoint> start_time{TimePoint{}};
    std::atomic<TimePoint> finish_time{TimePoint{}};
};

// For LoadJob::dependency_failure. Cancels the job on the first dependency failure or cancel.
void cancelOnDependencyFailure(const LoadJobPtr & self, const LoadJobPtr & dependency, std::exception_ptr & cancel);

// For LoadJob::dependency_failure. Never cancels the job due to dependency failure or cancel.
void ignoreDependencyFailure(const LoadJobPtr & self, const LoadJobPtr & dependency, std::exception_ptr & cancel);

template <class F> concept LoadJobDependencyFailure = std::invocable<F, const LoadJobPtr &, const LoadJobPtr &, std::exception_ptr &>;
template <class F> concept LoadJobOnWaiters = std::invocable<F, const LoadJobPtr &>;
template <class F> concept LoadJobFunc = std::invocable<F, AsyncLoader &, const LoadJobPtr &>;

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), 0, on_waiters_increment, on_waiters_decrement, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), 0, on_waiters_increment, on_waiters_decrement, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, size_t pool_id, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), pool_id, on_waiters_increment, on_waiters_decrement, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, size_t pool_id, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), pool_id, on_waiters_increment, on_waiters_decrement, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), 0, on_waiters_increment, on_waiters_decrement, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), 0, on_waiters_increment, on_waiters_decrement, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, size_t pool_id, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), pool_id, on_waiters_increment, on_waiters_decrement, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, size_t pool_id, String name, LoadJobOnWaiters auto && on_waiters_increment, LoadJobOnWaiters auto && on_waiters_decrement, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), pool_id, on_waiters_increment, on_waiters_decrement, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}


LoadJobPtr makeLoadJob(LoadJobSet && dependencies, String name, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), 0, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), 0, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, size_t pool_id, String name, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), pool_id, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, size_t pool_id, String name, LoadJobDependencyFailure auto && dependency_failure, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), pool_id, std::forward<decltype(dependency_failure)>(dependency_failure), std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, String name, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), 0, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), 0, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(LoadJobSet && dependencies, size_t pool_id, String name, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), pool_id, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, size_t pool_id, String name, LoadJobFunc auto && func)
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), pool_id, cancelOnDependencyFailure, std::forward<decltype(func)>(func));
}

// Represents a logically connected set of LoadJobs required to achieve some goals (final LoadJob in the set).
class LoadTask : private boost::noncopyable
{
public:
    LoadTask(AsyncLoader & loader_, LoadJobSet && jobs_, LoadJobSet && goal_jobs_ = {});
    ~LoadTask();

    // Merge all jobs from other task into this task.
    void merge(const LoadTaskPtr & task);

    // Schedule all jobs with AsyncLoader.
    void schedule();

    // Remove all jobs of this task from AsyncLoader.
    void remove();

    // Return the final jobs in this tasks. This job subset should be used as `dependencies` for dependent jobs or tasks:
    //   auto load_task = loadSomethingAsync(async_loader, load_after_task.goals(), something);
    const LoadJobSet & goals() const { return goal_jobs.empty() ? jobs : goal_jobs; }

    AsyncLoader & loader;

private:
    friend class AsyncLoader;

    LoadJobSet jobs;
    LoadJobSet goal_jobs;
};

inline LoadTaskPtr makeLoadTask(AsyncLoader & loader, LoadJobSet && jobs, LoadJobSet && goals = {})
{
    return std::make_shared<LoadTask>(loader, std::move(jobs), std::move(goals));
}


// `AsyncLoader` is a scheduler for DAG of `LoadJob`s. It tracks job dependencies and priorities.
// Basic usage example:
//     // Start async_loader with two thread pools (0=fg, 1=bg):
//     AsyncLoader async_loader({
//         {"FgPool", CurrentMetrics::AsyncLoaderThreads, ..., .max_threads = 2, .priority{0}}
//         {"BgPool", CurrentMetrics::AsyncLoaderThreads, ..., .max_threads = 1, .priority{1}}
//     });
//
//     // Create and schedule a task consisting of three jobs. Job1 has no dependencies and is run first.
//     // Job2 and job3 depend on job1 and are run only after job1 completion.
//     auto job_func = [&] (AsyncLoader & loader, const LoadJobPtr & self) {
//         LOG_TRACE(log, "Executing load job '{}' in pool '{}'", self->name, loader->getPoolName(self->pool()));
//     };
//     auto job1 = makeLoadJob({}, "job1", /* pool_id = */ 1, job_func);
//     auto job2 = makeLoadJob({ job1 }, "job2", /* pool_id = */ 1, job_func);
//     auto job3 = makeLoadJob({ job1 }, "job3", /* pool_id = */ 1, job_func);
//     auto task = makeLoadTask(async_loader, { job1, job2, job3 });
//     task.schedule();
//
//     // Another thread may prioritize a job by changing its pool and wait for it:
//     async_loader.prioritize(job3, /* pool_id = */ 0); // Increase priority: 1 -> 0 (lower is better)
//     async_loader.wait(job3); // Blocks until job completion or cancellation and rethrow an exception (if any)
//
// Every job has a pool associated with it. AsyncLoader starts every job in its thread pool.
// Each pool has a constant priority and a mutable maximum number of threads.
// Higher priority (lower `pool.priority` value) jobs are run first.
// No job with lower priority is started while there is at least one higher priority job ready or running.
//
// Job priority can be elevated (but cannot be lowered)
// (a) if either it has a dependent job with higher priority:
//     in this case the priority and the pool of a dependent job is inherited during `schedule()` call;
// (b) or job was explicitly prioritized by `prioritize(job, higher_priority_pool)` call:
//     this also leads to a priority inheritance for all the dependencies.
// Value stored in load job `pool_id` field is atomic and can be changed even during job execution.
// Job is, of course, not moved from its initial thread pool, but it should use `self->pool()` for
// all new jobs it create to avoid priority inversion. To obtain pool in which job is being executed
// call `self->execution_pool()` instead.
//
// === IMPLEMENTATION DETAILS ===
// All possible states and statuses of a job:
//                       .---------- scheduled ----------.
//  ctor --> assigned --> blocked --> ready --> executing --> finished ------> removed --> dtor
//  STATUS: '------------------ PENDING -----------------'   '-- OK|FAILED|CANCELED --'
//
// AsyncLoader tracks state of all scheduled and finished jobs. Job lifecycle is the following:
// 1)  A job is constructed with PENDING status and assigned to a pool. The job is placed into a task.
// 2)  The task is scheduled with all its jobs and their dependencies. A scheduled job may be ready, blocked (and later executing).
// 3a) When all dependencies are successfully finished, the job became ready. A ready job is enqueued into the ready queue of its pool.
// 3b) If at least one of the job dependencies is failed or canceled, then this job is canceled (with all it's dependent jobs as well).
//     On cancellation an ASYNC_LOAD_CANCELED exception is generated and saved inside LoadJob object. The job status is changed to CANCELED.
//     Exception is rethrown by any existing or new `wait()` call. The job is moved to the set of the finished jobs.
// 4)  The ready job starts execution by a worker. The job is dequeued. Callback `job_func` is called.
//     Status of an executing job is PENDING. Note that `job_func` of a CANCELED job is never executed.
// 5a) On successful execution the job status is changed to OK and all existing and new `wait()` calls finish w/o exceptions.
// 5b) Any exception thrown out of `job_func` is wrapped into an ASYNC_LOAD_FAILED exception and saved inside LoadJob.
//     The job status is changed to FAILED. All the dependent jobs are canceled. The exception is rethrown from all existing and new `wait()` calls.
// 6)  The job is no longer considered as scheduled and is instead moved to the finished jobs set. This is just for introspection of the finished jobs.
// 7)  The task containing this job is destructed or `remove()` is explicitly called. The job is removed from the finished job set.
// 8)  The job is destructed.
class AsyncLoader : private boost::noncopyable
{
public:
    using Metric = CurrentMetrics::Metric;

    // Helper struct for AsyncLoader construction
    struct PoolInitializer
    {
        String name;
        Metric metric_threads;
        Metric metric_active_threads;
        Metric metric_scheduled_threads;
        size_t max_threads; // Zero means use all CPU cores
        Priority priority;
    };

private:
    // Thread pool for job execution.
    // Pools control the following aspects of job execution:
    // 1) Concurrency: Amount of concurrently executing jobs in a pool is `max_threads`.
    // 2) Priority: As long as there is executing worker with higher priority, workers with lower priorities are not started
    //    (although, they can finish last job started before higher priority jobs appeared)
    struct Pool
    {
        const String name;
        const Priority priority;
        std::map<UInt64, LoadJobPtr> ready_queue; // FIFO queue of jobs to be executed in this pool. Map is used for faster erasing. Key is `ready_seqno`
        size_t max_threads; // Max number of workers to be spawn
        size_t workers = 0; // Number of currently executing workers
        std::atomic<size_t> suspended_workers{0}; // Number of workers that are blocked by `wait()` call on a job executing in the same pool (for deadlock resolution)
        std::unique_ptr<ThreadPool> thread_pool; // NOTE: we avoid using a `ThreadPool` queue to be able to move jobs between pools.

        explicit Pool(const PoolInitializer & init);
        Pool(Pool&& o) noexcept;
        bool isActive() const { return workers > 0 || !ready_queue.empty(); }
    };

    // Scheduling information for a pending job.
    struct Info
    {
        size_t dependencies_left = 0; // Current number of dependencies on pending jobs.
        UInt64 ready_seqno = 0; // Zero means that job is not in ready queue.
        LoadJobSet dependent_jobs; // Set of jobs dependent on this job. Contains only scheduled jobs.

        // Three independent states of a scheduled job.
        bool isBlocked() const { return dependencies_left > 0; }
        bool isReady() const { return dependencies_left == 0 && ready_seqno > 0; }
        bool isExecuting() const { return dependencies_left == 0 && ready_seqno == 0; }
    };

public:
    AsyncLoader(std::vector<PoolInitializer> pool_initializers, bool log_failures_, bool log_progress_, bool log_events_);

    // WARNING: all tasks instances should be destructed before associated AsyncLoader.
    ~AsyncLoader();

    // Start workers to execute scheduled load jobs. Note that AsyncLoader is constructed as already started.
    void start();

    // Wait for all load jobs to finish, including all new jobs. So at first take care to stop adding new jobs.
    void wait();

    // Wait for currently executing jobs to finish, but do not run any other pending jobs.
    // Not finished jobs are left in pending state:
    //  - they can be executed by calling start() again;
    //  - or canceled using ~Task() or remove() later.
    void stop();

    // Schedule all jobs of given `task` and their dependencies (even if they are not in task).
    // All dependencies of a scheduled job inherit its pool if it has higher priority. This way higher priority job
    // never waits for (blocked by) lower priority jobs. No priority inversion is possible.
    // Idempotent: multiple schedule() calls for the same job are no-op.
    // Note that `task` destructor ensures that all its jobs are finished (OK, FAILED or CANCELED)
    // and are removed from AsyncLoader, so it is thread-safe to destroy them.
    void schedule(LoadTask & task);
    void schedule(const LoadTaskPtr & task);
    void schedule(const LoadJobSet & jobs_to_schedule);

    // Schedule all tasks atomically. To ensure only highest priority jobs among all tasks are run first.
    void schedule(const LoadTaskPtrs & tasks);

    // Increase priority of a job and all its dependencies recursively.
    // Jobs from pools with priority higher than `new_pool` are not changed.
    void prioritize(const LoadJobPtr & job, size_t new_pool);

    // Sync wait for a pending job to be finished: OK, FAILED or CANCELED status.
    // Throws if job is FAILED or CANCELED unless `no_throw` is set. Returns or throws immediately if called on non-pending job.
    // Waiting for a not scheduled job is considered to be LOGICAL_ERROR, use waitLoad() helper instead to make sure the job is scheduled.
    // There are more rules if `wait()` is called from another job:
    //  1) waiting on a dependent job is considered to be LOGICAL_ERROR;
    //  2) waiting on a job in the same pool might lead to more workers spawned in that pool to resolve "blocked pool" deadlock;
    //  3) waiting on a job with lower priority lead to priority inheritance to avoid priority inversion.
    void wait(const LoadJobPtr & job, bool no_throw = false);

    // Remove finished jobs, cancel scheduled jobs, wait for executing jobs to finish and remove them.
    void remove(const LoadJobSet & jobs);

    // Increase or decrease maximum number of simultaneously executing jobs in `pool`.
    void setMaxThreads(size_t pool, size_t value);

    size_t getMaxThreads(size_t pool) const;
    const String & getPoolName(size_t pool) const;
    Priority getPoolPriority(size_t pool) const;

    size_t getScheduledJobCount() const;

    // Helper class for introspection
    struct JobState
    {
        LoadJobPtr job;
        size_t dependencies_left = 0;
        UInt64 ready_seqno = 0;
        bool is_blocked = false;
        bool is_ready = false;
        bool is_executing = false;
    };

    // For introspection and debug only, see `system.asynchronous_loader` table.
    std::vector<JobState> getJobStates() const;
    size_t suspendedWorkersCount(size_t pool_id);

private:
    void checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    String checkCycle(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock);
    void finish(const LoadJobPtr & job, LoadStatus status, std::exception_ptr reason, std::unique_lock<std::mutex> & lock);
    void gatherNotScheduled(const LoadJobPtr & job, LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    void prioritize(const LoadJobPtr & job, size_t new_pool_id, std::unique_lock<std::mutex> & lock);
    void enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock);
    void wait(std::unique_lock<std::mutex> & job_lock, const LoadJobPtr & job);
    bool canSpawnWorker(Pool & pool, std::unique_lock<std::mutex> & lock);
    bool canWorkerLive(Pool & pool, std::unique_lock<std::mutex> & lock);
    void setCurrentPriority(std::unique_lock<std::mutex> & lock, std::optional<Priority> priority);
    void updateCurrentPriorityAndSpawn(std::unique_lock<std::mutex> & lock);
    void spawn(Pool & pool, std::unique_lock<std::mutex> & lock);
    void worker(Pool & pool);
    bool hasWorker(std::unique_lock<std::mutex> & lock) const;

    // Logging
    const bool log_failures; // Worker should log all exceptions caught from job functions.
    const bool log_progress; // Periodically log total progress
    const bool log_events; // Log all important events: job start/end, waits, prioritizations
    LoggerPtr log;

    mutable std::mutex mutex; // Guards all the fields below.
    bool is_running = true;
    std::optional<Priority> current_priority; // highest priority among active pools
    UInt64 last_ready_seqno = 0; // Increasing counter for ready queue keys.
    UInt64 last_job_id = 0; // Increasing counter for job IDs
    std::unordered_map<LoadJobPtr, Info> scheduled_jobs; // Full set of scheduled pending jobs along with scheduling info.
    std::vector<Pool> pools; // Thread pools for job execution and ready queues
    LoadJobSet finished_jobs; // Set of finished jobs (for introspection only, until jobs are removed).
    AtomicStopwatch stopwatch; // For progress indication
    size_t old_jobs = 0; // Number of jobs that were finished in previous busy period (for correct progress indication)
    std::chrono::system_clock::time_point busy_period_start_time;
};

// === HELPER FUNCTIONS ===
// There are three types of helper functions:
//  schedulerLoad([loader], {jobs|task|tasks}):
//      Just schedule jobs for async loading.
//      Note that normally function `doSomethingAsync()` returns you a task which is NOT scheduled.
//      This is done to allow you:
//          (1) construct complex dependency graph offline.
//          (2) schedule tasks simultaneously to respect their relative priorities.
//          (3) do prioritization independently, before scheduling.
//  prioritizeLoad([loader], pool_id, {jobs|task|tasks}):
//      Prioritize jobs w/o waiting for it.
//      Note that prioritization may be done
//          (1) before scheduling (to ensure all jobs are started in the correct pools)
//          (2) after scheduling (for dynamic prioritization, e.g. when new query arrives)
//  waitLoad([loader], pool_id, {jobs|task|tasks}, [no_throw]):
//      Prioritize and wait for jobs.
//      Note that to avoid deadlocks it implicitly schedules all the jobs before waiting for them.
//      Also to avoid priority inversion you should never wait for a job that has lower priority.
//      So it prioritizes all jobs, then schedules all jobs and waits every job.
//      IMPORTANT: Any load error will be rethrown, unless `no_throw` is set.
//      Common usage pattern is:
//          waitLoad(currentPoolOr(foreground_pool_id), tasks);

// Returns current execution pool if it is called from load job, or `pool` otherwise
// It should be used for waiting other load jobs in places that can be executed from load jobs
size_t currentPoolOr(size_t pool);

inline void scheduleLoad(AsyncLoader & loader, const LoadJobSet & jobs)
{
    loader.schedule(jobs);
}

inline void scheduleLoad(const LoadTaskPtr & task)
{
    task->schedule();
}

inline void scheduleLoad(const LoadTaskPtrs & tasks)
{
    if (tasks.empty())
        return;
    // NOTE: it is assumed that all tasks use the same `AsyncLoader`
    AsyncLoader & loader = tasks.front()->loader;
    loader.schedule(tasks);
}

inline void waitLoad(AsyncLoader & loader, const LoadJobSet & jobs, bool no_throw = false)
{
    scheduleLoad(loader, jobs);
    for (const auto & job : jobs)
        loader.wait(job, no_throw);
}

inline void waitLoad(const LoadTaskPtr & task, bool no_throw = false)
{
    scheduleLoad(task);
    waitLoad(task->loader, task->goals(), no_throw);
}

inline void waitLoad(const LoadTaskPtrs & tasks, bool no_throw = false)
{
    scheduleLoad(tasks);
    for (const auto & task : tasks)
        waitLoad(task->loader, task->goals(), no_throw);
}

inline void prioritizeLoad(AsyncLoader & loader, size_t pool_id, const LoadJobSet & jobs)
{
    for (const auto & job : jobs)
        loader.prioritize(job, pool_id);
}

inline void prioritizeLoad(size_t pool_id, const LoadTaskPtr & task)
{
    prioritizeLoad(task->loader, pool_id, task->goals());
}

inline void prioritizeLoad(size_t pool_id, const LoadTaskPtrs & tasks)
{
    for (const auto & task : tasks)
        prioritizeLoad(task->loader, pool_id, task->goals());
}

inline void waitLoad(AsyncLoader & loader, size_t pool_id, const LoadJobSet & jobs, bool no_throw = false)
{
    prioritizeLoad(loader, pool_id, jobs);
    waitLoad(loader, jobs, no_throw);
}

inline void waitLoad(size_t pool_id, const LoadTaskPtr & task, bool no_throw = false)
{
    prioritizeLoad(task->loader, pool_id, task->goals());
    waitLoad(task->loader, task->goals(), no_throw);
}

inline void waitLoad(size_t pool_id, const LoadTaskPtrs & tasks, bool no_throw = false)
{
    prioritizeLoad(pool_id, tasks);
    waitLoad(tasks, no_throw);
}

inline LoadJobSet getGoals(const LoadTaskPtrs & tasks, const LoadJobSet & alternative = {})
{
    LoadJobSet result;
    for (const auto & task : tasks)
        result.insert(task->goals().begin(), task->goals().end());
    return result.empty() ? alternative : result;
}

inline LoadJobSet joinJobs(const LoadJobSet & jobs1, const LoadJobSet & jobs2)
{
    LoadJobSet result;
    if (!jobs1.empty())
        result.insert(jobs1.begin(), jobs1.end());
    if (!jobs2.empty())
        result.insert(jobs2.begin(), jobs2.end());
    return result;
}

inline LoadTaskPtrs joinTasks(const LoadTaskPtrs & tasks1, const LoadTaskPtrs & tasks2)
{
    if (tasks1.empty())
        return tasks2;
    if (tasks2.empty())
        return tasks1;
    LoadTaskPtrs result;
    result.reserve(tasks1.size() + tasks2.size());
    result.insert(result.end(), tasks1.begin(), tasks1.end());
    result.insert(result.end(), tasks2.begin(), tasks2.end());
    return result;
}

}
