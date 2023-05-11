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
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>


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
    LoadJob(LoadJobSetType && dependencies_, String name_, Func && func_, ssize_t priority_ = 0)
        : dependencies(std::forward<LoadJobSetType>(dependencies_))
        , name(std::move(name_))
        , func(std::forward<Func>(func_))
        , load_priority(priority_)
    {}

    // Current job status.
    LoadStatus status() const;
    std::exception_ptr exception() const;

    // Returns current value of a priority of the job. May differ from initial priority.
    ssize_t priority() const;

    // Sync wait for a pending job to be finished: OK, FAILED or CANCELED status.
    // Throws if job is FAILED or CANCELED. Returns or throws immediately on non-pending job.
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
    void execute(const LoadJobPtr & self);

    std::function<void(const LoadJobPtr & self)> func;
    std::atomic<ssize_t> load_priority;

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
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), std::forward<Func>(func));
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), std::forward<Func>(func));
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(LoadJobSet && dependencies, ssize_t priority, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(std::move(dependencies), std::move(name), std::forward<Func>(func), priority);
}

template <class Func = EmptyJobFunc>
LoadJobPtr makeLoadJob(const LoadJobSet & dependencies, ssize_t priority, String name, Func && func = EmptyJobFunc())
{
    return std::make_shared<LoadJob>(dependencies, std::move(name), std::forward<Func>(func), priority);
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
inline void scheduleAndWaitLoad(Args && ... args)
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

// `AsyncLoader` is a scheduler for DAG of `LoadJob`s. It tracks dependencies and priorities of jobs.
// Basic usage example:
//     auto job_func = [&] (const LoadJobPtr & self) {
//         LOG_TRACE(log, "Executing load job '{}' with priority '{}'", self->name, self->priority());
//     };
//     auto job1 = makeLoadJob({}, "job1", job_func);
//     auto job2 = makeLoadJob({ job1 }, "job2", job_func);
//     auto job3 = makeLoadJob({ job1 }, "job3", job_func);
//     auto task = makeLoadTask(async_loader, { job1, job2, job3 });
//     task.schedule();
// Here we have created and scheduled a task consisting of three jobs. Job1 has no dependencies and is run first.
// Job2 and job3 depend on job1 and are run only after job1 completion. Another thread may prioritize a job and wait for it:
//     async_loader->prioritize(job3, /* priority = */ 1); // higher priority jobs are run first, default priority is zero.
//     job3->wait(); // blocks until job completion or cancellation and rethrow an exception (if any)
//
// AsyncLoader tracks state of all scheduled jobs. Job lifecycle is the following:
// 1)  Job is constructed with PENDING status and initial priority. The job is placed into a task.
// 2)  The task is scheduled with all its jobs and their dependencies. A scheduled job may be ready (i.e. have all its dependencies finished) or blocked.
// 3a) When all dependencies are successfully executed, the job became ready. A ready job is enqueued into the ready queue.
// 3b) If at least one of the job dependencies is failed or canceled, then this job is canceled (with all it's dependent jobs as well).
//     On cancellation an ASYNC_LOAD_CANCELED exception is generated and saved inside LoadJob object. The job status is changed to CANCELED.
//     Exception is rethrown by any existing or new `wait()` call. The job is moved to the set of the finished jobs.
// 4)  The scheduled pending ready job starts execution by a worker. The job is dequeued. Callback `job_func` is called.
//     Status of an executing job is PENDING. And it is still considered as a scheduled job by AsyncLoader.
//     Note that `job_func` of a CANCELED job is never executed.
// 5a) On successful execution the job status is changed to OK and all existing and new `wait()` calls finish w/o exceptions.
// 5b) Any exception thrown out of `job_func` is wrapped into an ASYNC_LOAD_FAILED exception and saved inside LoadJob.
//     The job status is changed to FAILED. All the dependent jobs are canceled. The exception is rethrown from all existing and new `wait()` calls.
// 6)  The job is no longer considered as scheduled and is instead moved to the finished jobs set. This is just for introspection of the finished jobs.
// 7)  The task containing this job is destructed or `remove()` is explicitly called. The job is removed from the finished job set.
// 8)  The job is destructed.
//
// Every job has a priority associated with it. AsyncLoader runs higher priority (greater `priority` value) jobs first. Job priority can be elevated
// (a) if either it has a dependent job with higher priority (in this case priority of a dependent job is inherited);
// (b) or job was explicitly prioritized by `prioritize(job, higher_priority)` call (this also leads to a priority inheritance for all the dependencies).
// Note that to avoid priority inversion `job_func` should use `self->priority()` to schedule new jobs in AsyncLoader or any other pool.
// Value stored in load job priority field is atomic and can be increased even during job execution.
//
// When a task is scheduled it can contain dependencies on previously scheduled jobs. These jobs can have any status. If job A being scheduled depends on
// another job B that is not yet scheduled, then job B will also be scheduled (even if the task does not contain it).
class AsyncLoader : private boost::noncopyable
{
private:
    // Key of a pending job in the ready queue.
    struct ReadyKey
    {
        ssize_t priority; // Ascending order
        ssize_t initial_priority; // Ascending order
        UInt64 ready_seqno; // Descending order

        bool operator<(const ReadyKey & rhs) const
        {
            if (priority > rhs.priority)
                return true;
            if (priority < rhs.priority)
                return false;
            if (initial_priority > rhs.initial_priority)
                return true;
            if (initial_priority < rhs.initial_priority)
                return false;
            return ready_seqno < rhs.ready_seqno;
        }
    };

    // Scheduling information for a pending job.
    struct Info
    {
        ssize_t initial_priority = 0; // Initial priority passed into schedule().
        ssize_t priority = 0; // Elevated priority, due to priority inheritance or prioritize().
        size_t dependencies_left = 0; // Current number of dependencies on pending jobs.
        UInt64 ready_seqno = 0; // Zero means that job is not in ready queue.
        LoadJobSet dependent_jobs; // Set of jobs dependent on this job.

        // Three independent states of a non-finished job.
        bool is_blocked() const { return dependencies_left > 0; }
        bool is_ready() const { return dependencies_left == 0 && ready_seqno > 0; }
        bool is_executing() const { return dependencies_left == 0 && ready_seqno == 0; }

        // Get key of a ready job
        ReadyKey key() const
        {
            return {.priority = priority, .initial_priority = initial_priority, .ready_seqno = ready_seqno};
        }
    };

public:
    using Metric = CurrentMetrics::Metric;

    AsyncLoader(Metric metric_threads, Metric metric_active_threads, size_t max_threads_, bool log_failures_, bool log_progress_);

    // WARNING: all tasks instances should be destructed before associated AsyncLoader.
    ~AsyncLoader();

    // Start workers to execute scheduled load jobs.
    void start();

    // Wait for all load jobs to finish, including all new jobs. So at first take care to stop adding new jobs.
    void wait();

    // Wait for currently executing jobs to finish, but do not run any other pending jobs.
    // Not finished jobs are left in pending state:
    //  - they can be executed by calling start() again;
    //  - or canceled using ~Task() or remove() later.
    void stop();

    // Schedule all jobs of given `task` and their dependencies (if any, not scheduled yet).
    // Higher priority jobs (with greater `job->priority()` value) are executed earlier.
    // All dependencies of a scheduled job inherit its priority if it is higher. This way higher priority job
    // never wait for (blocked by) lower priority jobs. No priority inversion is possible.
    // Note that `task` destructor ensures that all its jobs are finished (OK, FAILED or CANCELED)
    // and are removed from AsyncLoader, so it is thread-safe to destroy them.
    void schedule(LoadTask & task);
    void schedule(const LoadTaskPtr & task);

    // Schedule all tasks atomically. To ensure only highest priority jobs among all tasks are run first.
    void schedule(const LoadTaskPtrs & tasks);

    // Increase priority of a job and all its dependencies recursively.
    void prioritize(const LoadJobPtr & job, ssize_t new_priority);

    // Remove finished jobs, cancel scheduled jobs, wait for executing jobs to finish and remove them.
    void remove(const LoadJobSet & jobs);

    // Increase or decrease maximum number of simultaneously executing jobs.
    void setMaxThreads(size_t value);

    size_t getMaxThreads() const;
    size_t getScheduledJobCount() const;

    // Helper class for introspection
    struct JobState
    {
        LoadJobPtr job;
        size_t dependencies_left = 0;
        bool is_executing = false;
        bool is_blocked = false;
        bool is_ready = false;
        std::optional<ssize_t> initial_priority;
        std::optional<UInt64> ready_seqno;
    };

    // For introspection and debug only, see `system.async_loader` table
    std::vector<JobState> getJobStates() const;

private:
    void checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    String checkCycleImpl(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock);
    void finish(std::unique_lock<std::mutex> & lock, const LoadJobPtr & job, LoadStatus status, std::exception_ptr exception_from_job = {});
    void scheduleImpl(const LoadJobSet & input_jobs);
    void gatherNotScheduled(const LoadJobPtr & job, LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    void prioritize(const LoadJobPtr & job, ssize_t new_priority, std::unique_lock<std::mutex> & lock);
    void enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock);
    void spawn(std::unique_lock<std::mutex> &);
    void worker();

    // Logging
    const bool log_failures; // Worker should log all exceptions caught from job functions.
    const bool log_progress; // Periodically log total progress
    Poco::Logger * log;
    std::chrono::system_clock::time_point busy_period_start_time;
    AtomicStopwatch stopwatch;
    size_t old_jobs = 0; // Number of jobs that were finished in previous busy period (for correct progress indication)

    mutable std::mutex mutex; // Guards all the fields below.
    bool is_running = false;

    // Full set of scheduled pending jobs along with scheduling info.
    std::unordered_map<LoadJobPtr, Info> scheduled_jobs;

    // Subset of scheduled pending non-blocked jobs (waiting for a worker to be executed).
    // Represent a queue of jobs in order of decreasing priority and FIFO for jobs with equal priorities.
    std::map<ReadyKey, LoadJobPtr> ready_queue;

    // Set of finished jobs (for introspection only, until jobs are removed).
    LoadJobSet finished_jobs;

    // Increasing counter for `ReadyKey` assignment (to preserve FIFO order of the jobs with equal priorities).
    UInt64 last_ready_seqno = 0;

    // For executing jobs. Note that we avoid using an internal queue of the pool to be able to prioritize jobs.
    size_t max_threads;
    size_t workers = 0;
    ThreadPool pool;
};

}
