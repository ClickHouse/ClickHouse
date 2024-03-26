#pragma once

#include <condition_variable>
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


namespace Poco { class Logger; }

namespace DB
{

class LoadJob;
using LoadJobPtr = std::shared_ptr<LoadJob>;
using LoadJobSet = std::unordered_set<LoadJobPtr>;
class LoadTask;
using LoadTaskPtr = std::shared_ptr<LoadTask>;
using LoadTaskPtrs = std::vector<LoadTaskPtr>;
class AsyncLoader;

void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch);

// Execution status of a load job.
enum class LoadStatus
{
    PENDING,  // Load job is not started yet.
    OK,       // Load job executed and was successful.
    FAILED,   // Load job executed and failed.
    CANCELED  // Load job is not going to be executed due to removal or dependency failure.
};

// Smallest indivisible part of a loading process. Load job can have multiple dependencies, thus jobs constitute a direct acyclic graph (DAG).
// Job encapsulates a function to be executed by `AsyncLoader` as soon as job functions of all dependencies are successfully executed.
// Job can be waited for by an arbitrary number of threads. See `AsyncLoader` class description for more details.
class LoadJob : private boost::noncopyable
{
public:
    template <class Func, class LoadJobSetType>
    LoadJob(LoadJobSetType && dependencies_, String name_, size_t pool_id_, Func && func_)
        : dependencies(std::forward<LoadJobSetType>(dependencies_))
        , name(std::move(name_))
        , pool_id(pool_id_)
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

    // Sync wait for a pending job to be finished: OK, FAILED or CANCELED status.
    // Throws if job is FAILED or CANCELED. Returns or throws immediately if called on non-pending job.
    void wait() const;

    // Wait for a job to reach any non PENDING status.
    void waitNoThrow() const noexcept;

    // Returns number of threads blocked by `wait()` or `waitNoThrow()` calls.
    size_t waitersCount() const;

    // Introspection
    using TimePoint = std::chrono::system_clock::time_point;
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

    void scheduled();
    void enqueued();
    void execute(size_t pool, const LoadJobPtr & self);

    std::atomic<size_t> execution_pool_id;
    std::atomic<size_t> pool_id;
    std::function<void(const LoadJobPtr & self)> func;

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

struct EmptyJobFunc
{
    void operator()(const LoadJobPtr &) {}
};

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(LoadJobSet && dependencies, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), 0, std::forward<Func>(func));
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), 0, std::forward<Func>(func));
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(LoadJobSet && dependencies, size_t pool_id, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), pool_id, std::forward<Func>(func));
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, size_t pool_id, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), pool_id, std::forward<Func>(func));
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

    // Do not track jobs in this task.
    // WARNING: Jobs will never be removed() and are going to be stored as finished jobs until ~AsyncLoader().
    void detach();

    // Return the final jobs in this tasks. This job subset should be used as `dependencies` for dependent jobs or tasks:
    //   auto load_task = loadSomethingAsync(async_loader, load_after_task.goals(), something);
    const LoadJobSet & goals() const { return goal_jobs.empty() ? jobs : goal_jobs; }

private:
    friend class AsyncLoader;

    AsyncLoader & loader;
    LoadJobSet jobs;
    LoadJobSet goal_jobs;
};

inline LoadTaskPtr makeLoadTask(AsyncLoader & loader, LoadJobSet && jobs, LoadJobSet && goals = {})
{
    return std::make_shared<LoadTask>(loader, std::move(jobs), std::move(goals));
}

inline void scheduleLoad(const LoadTaskPtr & task)
{
    task->schedule();
}

inline void scheduleLoad(const LoadTaskPtrs & tasks)
{
    for (const auto & task : tasks)
        task->schedule();
}

template <class... Args>
inline void scheduleLoadAll(Args && ... args)
{
    (scheduleLoad(std::forward<Args>(args)), ...);
}

inline void waitLoad(const LoadJobSet & jobs)
{
    for (const auto & job : jobs)
        job->wait();
}

inline void waitLoad(const LoadTaskPtr & task)
{
    waitLoad(task->goals());
}

inline void waitLoad(const LoadTaskPtrs & tasks)
{
    for (const auto & task : tasks)
        waitLoad(task->goals());
}

template <class... Args>
inline void waitLoadAll(Args && ... args)
{
    (waitLoad(std::forward<Args>(args)), ...);
}

template <class... Args>
inline void scheduleAndWaitLoadAll(Args && ... args)
{
    scheduleLoadAll(std::forward<Args>(args)...);
    waitLoadAll(std::forward<Args>(args)...);
}

inline LoadJobSet getGoals(const LoadTaskPtrs & tasks)
{
    LoadJobSet result;
    for (const auto & task : tasks)
        result.insert(task->goals().begin(), task->goals().end());
    return result;
}

inline LoadJobSet getGoalsOr(const LoadTaskPtrs & tasks, const LoadJobSet & alternative)
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

// `AsyncLoader` is a scheduler for DAG of `LoadJob`s. It tracks job dependencies and priorities.
// Basic usage example:
//     // Start async_loader with two thread pools (0=fg, 1=bg):
//     AsyncLoader async_loader({
//         {"FgPool", CurrentMetrics::AsyncLoaderThreads, CurrentMetrics::AsyncLoaderThreadsActive, .max_threads = 2, .priority{0}}
//         {"BgPool", CurrentMetrics::AsyncLoaderThreads, CurrentMetrics::AsyncLoaderThreadsActive, .max_threads = 1, .priority{1}}
//     });
//
//     // Create and schedule a task consisting of three jobs. Job1 has no dependencies and is run first.
//     // Job2 and job3 depend on job1 and are run only after job1 completion.
//     auto job_func = [&] (const LoadJobPtr & self) {
//         LOG_TRACE(log, "Executing load job '{}' in pool '{}'", self->name, async_loader->getPoolName(self->pool()));
//     };
//     auto job1 = makeLoadJob({}, "job1", /* pool_id = */ 1, job_func);
//     auto job2 = makeLoadJob({ job1 }, "job2", /* pool_id = */ 1, job_func);
//     auto job3 = makeLoadJob({ job1 }, "job3", /* pool_id = */ 1, job_func);
//     auto task = makeLoadTask(async_loader, { job1, job2, job3 });
//     task.schedule();
//
//     // Another thread may prioritize a job by changing its pool and wait for it:
//     async_loader->prioritize(job3, /* pool_id = */ 0); // Increase priority: 1 -> 0 (lower is better)
//     job3->wait(); // Blocks until job completion or cancellation and rethrow an exception (if any)
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
        std::unique_ptr<ThreadPool> thread_pool; // NOTE: we avoid using a `ThreadPool` queue to be able to move jobs between pools.
        std::map<UInt64, LoadJobPtr> ready_queue; // FIFO queue of jobs to be executed in this pool. Map is used for faster erasing. Key is `ready_seqno`
        size_t max_threads; // Max number of workers to be spawn
        size_t workers = 0; // Number of currently execution workers

        bool isActive() const { return workers > 0 || !ready_queue.empty(); }
    };

    // Scheduling information for a pending job.
    struct Info
    {
        size_t dependencies_left = 0; // Current number of dependencies on pending jobs.
        UInt64 ready_seqno = 0; // Zero means that job is not in ready queue.
        LoadJobSet dependent_jobs; // Set of jobs dependent on this job.

        // Three independent states of a scheduled job.
        bool isBlocked() const { return dependencies_left > 0; }
        bool isReady() const { return dependencies_left == 0 && ready_seqno > 0; }
        bool isExecuting() const { return dependencies_left == 0 && ready_seqno == 0; }
    };

public:
    using Metric = CurrentMetrics::Metric;

    // Helper struct for AsyncLoader construction
    struct PoolInitializer
    {
        String name;
        Metric metric_threads;
        Metric metric_active_threads;
        size_t max_threads;
        Priority priority;
    };

    AsyncLoader(std::vector<PoolInitializer> pool_initializers, bool log_failures_, bool log_progress_);

    // Stops AsyncLoader before destruction
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

    // Schedule all tasks atomically. To ensure only highest priority jobs among all tasks are run first.
    void schedule(const LoadTaskPtrs & tasks);

    // Increase priority of a job and all its dependencies recursively.
    // Jobs from higher (than `new_pool`) priority pools are not changed.
    void prioritize(const LoadJobPtr & job, size_t new_pool);

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

    // For introspection and debug only, see `system.async_loader` table
    std::vector<JobState> getJobStates() const;

private:
    void checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    String checkCycleImpl(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock);
    void finish(const LoadJobPtr & job, LoadStatus status, std::exception_ptr exception_from_job, std::unique_lock<std::mutex> & lock);
    void scheduleImpl(const LoadJobSet & input_jobs);
    void gatherNotScheduled(const LoadJobPtr & job, LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    void prioritize(const LoadJobPtr & job, size_t new_pool_id, std::unique_lock<std::mutex> & lock);
    void enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock);
    bool canSpawnWorker(Pool & pool, std::unique_lock<std::mutex> &);
    bool canWorkerLive(Pool & pool, std::unique_lock<std::mutex> &);
    void updateCurrentPriorityAndSpawn(std::unique_lock<std::mutex> &);
    void spawn(Pool & pool, std::unique_lock<std::mutex> &);
    void worker(Pool & pool);
    bool hasWorker(std::unique_lock<std::mutex> &) const;

    // Logging
    const bool log_failures; // Worker should log all exceptions caught from job functions.
    const bool log_progress; // Periodically log total progress
    Poco::Logger * log;

    mutable std::mutex mutex; // Guards all the fields below.
    bool is_running = true;
    std::optional<Priority> current_priority; // highest priority among active pools
    UInt64 last_ready_seqno = 0; // Increasing counter for ready queue keys.
    std::unordered_map<LoadJobPtr, Info> scheduled_jobs; // Full set of scheduled pending jobs along with scheduling info.
    std::vector<Pool> pools; // Thread pools for job execution and ready queues
    LoadJobSet finished_jobs; // Set of finished jobs (for introspection only, until jobs are removed).
    AtomicStopwatch stopwatch; // For progress indication
    size_t old_jobs = 0; // Number of jobs that were finished in previous busy period (for correct progress indication)
    std::chrono::system_clock::time_point busy_period_start_time;
};

}
