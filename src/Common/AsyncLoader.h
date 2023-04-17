#pragma once

#include <condition_variable>
#include <exception>
#include <memory>
#include <map>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

namespace DB
{

class LoadJob;
using LoadJobPtr = std::shared_ptr<LoadJob>;
using LoadJobSet = std::unordered_set<LoadJobPtr>;
class LoadTask;
class AsyncLoader;

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
    template <class Func>
    LoadJob(LoadJobSet && dependencies_, const String & name_, Func && func_)
        : dependencies(std::move(dependencies_))
        , name(name_)
        , func(std::forward<Func>(func_))
    {}

    // Current job status.
    LoadStatus status() const;
    std::exception_ptr exception() const;

    // Returns current value of a priority of the job. May differ from initial priority passed to `AsyncLoader:::schedule()` call.
    ssize_t priority() const;

    // Sync wait for a pending job to be finished: OK, FAILED or CANCELED status.
    // Throws if job is FAILED or CANCELED. Returns or throws immediately on non-pending job.
    void wait() const;

    // Wait for a job to reach any non PENDING status.
    void waitNoThrow() const noexcept;

    // Returns number of threads blocked by `wait()` or `waitNoThrow()` calls.
    size_t waiters_count() const;

    const LoadJobSet dependencies; // Jobs to be done before this one (with ownership), it is `const` to make creation of cycles hard
    const String name;

private:
    friend class AsyncLoader;

    void ok();
    void failed(const std::exception_ptr & ptr);
    void canceled(const std::exception_ptr & ptr);
    void finish();

    std::function<void(const LoadJobPtr & self)> func;
    std::atomic<ssize_t> load_priority{0};

    mutable std::mutex mutex;
    mutable std::condition_variable finished;
    mutable size_t waiters = 0;
    LoadStatus load_status{LoadStatus::PENDING};
    std::exception_ptr load_exception;
};

template <class Func>
LoadJobPtr makeLoadJob(LoadJobSet && dependencies, const String & name, Func && func)
{
    return std::make_shared<LoadJob>(std::move(dependencies), name, std::forward<Func>(func));
}

// `AsyncLoader` is a scheduler for DAG of `LoadJob`s. It tracks dependencies and priorities of jobs.
// Basic usage example:
//     auto job_func = [&] (const LoadJobPtr & self) {
//         LOG_TRACE(log, "Executing load job '{}' with priority '{}'", self->name, self->priority());
//     };
//     auto job1 = makeLoadJob({}, "job1", job_func);
//     auto job2 = makeLoadJob({ job1 }, "job2", job_func);
//     auto job3 = makeLoadJob({ job1 }, "job3", job_func);
//     auto task = async_loader.schedule({ job1, job2, job3 }, /* priority = */ 0);
// Here we have created and scheduled a task consisting of three jobs. Job1 has no dependencies and is run first.
// Job2 and job3 depend on job1 and are run only after job1 completion. Another thread may prioritize a job and wait for it:
//     async_loader->prioritize(job3, /* priority = */ 1); // higher priority jobs are run first
//     job3->wait(); // blocks until job completion or cancellation and rethrow an exception (if any)
//
// AsyncLoader tracks state of all scheduled jobs. Job lifecycle is the following:
// 1)  Job is constructed with PENDING status.
// 2)  Job is scheduled and placed into a task. Scheduled job may be ready (i.e. have all its dependencies finished) or blocked.
// 3a) When all dependencies are successfully executed, job became ready. Ready job is enqueued into the ready queue.
// 3b) If at least one of job dependencies is failed or canceled, then this job is canceled (with all it's dependent jobs as well).
//     On cancellation an ASYNC_LOAD_CANCELED exception is generated and saved inside LoadJob object. Job status is changed to CANCELED.
//     Exception is rethrown by any existing or new `wait()` call. Job is moved to the set of finished jobs.
// 4)  Scheduled pending ready job starts execution by a worker. Job is dequeuedCallback `job_func` is called.
//     Status of an executing job is PENDING. And it is still considered as scheduled job by AsyncLoader.
//     Note that `job_func` of a CANCELED job is never executed.
// 5a) On successful execution job status is changed to OK and all existing and new `wait()` calls finish w/o exceptions.
// 5b) Any exception thrown out of `job_func` is wrapped into ASYNC_LOAD_FAILED exception and save inside LoadJob.
//     Job status is changed to FAILED. All dependent jobs are canceled. The exception is rethrown from all existing and new `wait()` calls.
// 6)  Job is no longer considered as scheduled and is instead moved to finished jobs set. This is required for introspection of finished jobs.
// 7)  Task object containing this job is destructed or `remove()` is explicitly called. Job is removed from the finished job set.
// 8)  Job is destructed.
//
// Every job has a priority associated with it. AsyncLoader runs higher priority (greater `priority` value) jobs first. Job priority can be elevated
// (a) if either it has a dependent job with higher priority (in this case priority of a dependent job is inherited);
// (b) or job was explicitly prioritized by `prioritize(job, higher_priority)` call (this also leads to a priority inheritance for all the dependencies).
// Note that to avoid priority inversion `job_func` should use `self->priority()` to schedule new jobs in AsyncLoader or any other pool.
// Value stored in load job priority field is atomic and can be increased even during job execution.
//
// When task is scheduled it can contain dependencies on previously scheduled jobs. These jobs can have any status.
// The only forbidden thing is a dependency on job, that was not scheduled in AsyncLoader yet: all dependent jobs are immediately canceled.
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

    // Helper class that removes all not started and finished jobs in destructor and waits for all the executing jobs to finish.
    class Task
    {
    public:
        Task();
        Task(AsyncLoader * loader_, LoadJobSet && jobs_);
        Task(const Task & o) = delete;
        Task(Task && o) noexcept;
        Task & operator=(const Task & o) = delete;
        ~Task();
        Task & operator=(Task && o) noexcept;

        // Merge all jobs from other task into this task. Useful for merging jobs with different priorities into one task.
        void merge(Task && o);

        // Remove all jobs of this task from AsyncLoader.
        void remove();

        // Do not track jobs in this task.
        // WARNING: Jobs will never be removed() and are going to be stored as finished jobs until ~AsyncLoader().
        void detach();

    private:
        AsyncLoader * loader;
        LoadJobSet jobs;
    };

    AsyncLoader(Metric metric_threads, Metric metric_active_threads, size_t max_threads_, bool log_failures_);

    // WARNING: all Task instances returned by `schedule()` should be destructed before AsyncLoader.
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

    // Schedule all `jobs` with given `priority` and return a task containing these jobs.
    // Higher priority jobs (with greater `priority` value) are executed earlier.
    // All dependencies of a scheduled job inherit its priority if it is higher. This way higher priority job
    // never wait for (blocked by) lower priority jobs (no priority inversion).
    // Returned task destructor ensures that all the `jobs` are finished (OK, FAILED or CANCELED)
    // and are removed from AsyncLoader, so it is thread-safe to destroy them.
    [[nodiscard]] Task schedule(LoadJobSet && jobs, ssize_t priority = 0);

    // Increase priority of a job and all its dependencies recursively.
    void prioritize(const LoadJobPtr & job, ssize_t new_priority);

    // Remove finished jobs, cancel scheduled jobs, wait for executing jobs to finish and remove them.
    void remove(const LoadJobSet & jobs);

    // Increase or decrease maximum number of simultaneously executing jobs.
    void setMaxThreads(size_t value);

    size_t getMaxThreads() const;
    size_t getScheduledJobCount() const;

private:
    void checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock);
    String checkCycleImpl(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock);
    void finish(std::unique_lock<std::mutex> & lock, const LoadJobPtr & job, LoadStatus status, std::exception_ptr exception_from_job = {});
    void prioritize(const LoadJobPtr & job, ssize_t new_priority, std::unique_lock<std::mutex> & lock);
    void enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock);
    void spawn(std::unique_lock<std::mutex> &);
    void worker();

    const bool log_failures; // Worker should log all exceptions caught from job functions.

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
