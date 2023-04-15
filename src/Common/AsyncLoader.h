#pragma once

#include <condition_variable>
#include <exception>
#include <memory>
#include <map>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <base/defines.h>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/setThreadName.h>

namespace DB
{

class LoadJob;
using LoadJobPtr = std::shared_ptr<LoadJob>;
using LoadJobSet = std::unordered_set<LoadJobPtr>;
class LoadTask;
class AsyncLoader;

namespace ErrorCodes
{
    extern const int ASYNC_LOAD_SCHEDULE_FAILED;
    extern const int ASYNC_LOAD_FAILED;
    extern const int ASYNC_LOAD_CANCELED;
}

enum class LoadStatus
{
    PENDING,  // Load job is not started yet
    OK,       // Load job executed and was successful
    FAILED,   // Load job executed and failed
    CANCELED  // Load job is not going to be executed due to removal or dependency failure
};

class LoadJob : private boost::noncopyable
{
public:
    template <class Func>
    LoadJob(LoadJobSet && dependencies_, const String & name_, Func && func_)
        : dependencies(std::move(dependencies_))
        , name(name_)
        , func(std::forward<Func>(func_))
    {}

    LoadStatus status() const
    {
        std::unique_lock lock{mutex};
        return load_status;
    }

    std::exception_ptr exception() const
    {
        std::unique_lock lock{mutex};
        return load_exception;
    }

    void wait() const
    {
        std::unique_lock lock{mutex};
        waiters++;
        finished.wait(lock, [this] { return load_status != LoadStatus::PENDING; });
        waiters--;
        if (load_exception)
            std::rethrow_exception(load_exception);
    }

    void waitNoThrow() const
    {
        std::unique_lock lock{mutex};
        waiters++;
        finished.wait(lock, [this] { return load_status != LoadStatus::PENDING; });
        waiters--;
    }

    size_t waiters_count() const
    {
        std::unique_lock lock{mutex};
        return waiters;
    }

    const LoadJobSet dependencies; // jobs to be done before this one (with ownership), it is `const` to make creation of cycles hard
    const String name;
    std::atomic<ssize_t> priority{0};

private:
    friend class AsyncLoader;

    void ok()
    {
        std::unique_lock lock{mutex};
        load_status = LoadStatus::OK;
        if (waiters > 0)
            finished.notify_all();
    }

    void failed(const std::exception_ptr & ptr)
    {
        std::unique_lock lock{mutex};
        load_status = LoadStatus::FAILED;
        load_exception = ptr;
        if (waiters > 0)
            finished.notify_all();
    }

    void canceled(const std::exception_ptr & ptr)
    {
        std::unique_lock lock{mutex};
        load_status = LoadStatus::CANCELED;
        load_exception = ptr;
        if (waiters > 0)
            finished.notify_all();
    }

    // TODO(serxa): add callback/status for cancel?

    std::function<void(const LoadJobPtr & self)> func;

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

// TODO(serxa): write good comment
class AsyncLoader : private boost::noncopyable
{
private:
    // Key of a pending job in ready queue
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

    // Scheduling information for a pending job
    struct Info
    {
        ssize_t initial_priority = 0; // Initial priority passed into schedule()
        ssize_t priority = 0; // Elevated priority, due to priority inheritance or prioritize()
        size_t dependencies_left = 0;
        UInt64 ready_seqno = 0; // Zero means that job is not in ready queue
        LoadJobSet dependent_jobs;

        // Three independent states of a non-finished jobs
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

    // Helper class that removes all not started jobs in destructor and wait all executing jobs to finish
    class Task
    {
    public:
        Task()
            : loader(nullptr)
        {}

        Task(AsyncLoader * loader_, LoadJobSet && jobs_)
            : loader(loader_)
            , jobs(std::move(jobs_))
        {}

        Task(const Task & o) = delete;
        Task & operator=(const Task & o) = delete;

        Task(Task && o) noexcept
            : loader(std::exchange(o.loader, nullptr))
            , jobs(std::move(o.jobs))
        {}

        Task & operator=(Task && o) noexcept
        {
            loader = std::exchange(o.loader, nullptr);
            jobs = std::move(o.jobs);
            return *this;
        }

        void merge(Task && o)
        {
            if (!loader)
            {
                *this = std::move(o);
            }
            else
            {
                chassert(loader == o.loader);
                jobs.merge(o.jobs);
                o.loader = nullptr;
            }
        }

        ~Task()
        {
            remove();
        }

        void remove()
        {
            if (loader)
            {
                loader->remove(jobs);
                detach();
            }
        }

        // Do not track jobs in this task
        void detach()
        {
            loader = nullptr;
            jobs.clear();
        }

    private:
        AsyncLoader * loader;
        LoadJobSet jobs;
    };

    AsyncLoader(Metric metric_threads, Metric metric_active_threads, size_t max_threads_, bool log_failures_)
        : log_failures(log_failures_)
        , max_threads(max_threads_)
        , pool(metric_threads, metric_active_threads, max_threads)
    {}

    // WARNING: all Task instances returned by `schedule()` should be destructed before AsyncLoader
    ~AsyncLoader()
    {
        stop();
    }

    // Start workers to execute scheduled load jobs
    void start()
    {
        std::unique_lock lock{mutex};
        is_running = true;
        for (size_t i = 0; workers < max_threads && i < ready_queue.size(); i++)
            spawn(lock);
    }

    // Wait for all load jobs to finish, including all new jobs. So at first take care to stop adding new jobs.
    void wait()
    {
        pool.wait();
    }

    // Wait for currently executing jobs to finish, but do not run any other pending jobs.
    // Not finished jobs are left in pending state:
    //  - they can be resumed by calling start() again;
    //  - or canceled using ~Task() or remove() later.
    void stop()
    {
        {
            std::unique_lock lock{mutex};
            is_running = false;
            // NOTE: there is no need to notify because workers never wait
        }
        pool.wait();
    }

    [[nodiscard]] Task schedule(LoadJobSet && jobs, ssize_t priority = 0)
    {
        std::unique_lock lock{mutex};

        // Sanity checks
        for (const auto & job : jobs)
        {
            if (job->status() != LoadStatus::PENDING)
                throw Exception(ErrorCodes::ASYNC_LOAD_SCHEDULE_FAILED, "Trying to schedule already finished load job '{}'", job->name);
            if (scheduled_jobs.contains(job))
                throw Exception(ErrorCodes::ASYNC_LOAD_SCHEDULE_FAILED, "Load job '{}' has been already scheduled", job->name);
        }

        // Ensure scheduled_jobs graph will have no cycles. The only way to get a cycle is to add a cycle, assuming old jobs cannot reference new ones.
        checkCycle(jobs, lock);

        // We do not want any exception to be throws after this point, because the following code is not exception-safe
        DENY_ALLOCATIONS_IN_SCOPE;

        // Schedule all incoming jobs
        for (const auto & job : jobs)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                scheduled_jobs.emplace(job, Info{.initial_priority = priority, .priority = priority});
            });
            job->priority.store(priority); // Set user-facing priority
        }

        // Process dependencies on scheduled pending jobs
        for (const auto & job : jobs)
        {
            Info & info = scheduled_jobs.find(job)->second;
            for (const auto & dep : job->dependencies)
            {
                // Register every dependency on scheduled job with back-link to dependent job
                if (auto dep_info = scheduled_jobs.find(dep); dep_info != scheduled_jobs.end())
                {
                    NOEXCEPT_SCOPE({
                        ALLOW_ALLOCATIONS_IN_SCOPE;
                        dep_info->second.dependent_jobs.insert(job);
                    });
                    info.dependencies_left++;

                    // Priority inheritance: prioritize deps to have at least given `priority` to avoid priority inversion
                    prioritize(dep, priority, lock);
                }
            }

            // Enqueue non-blocked jobs (w/o dependencies) to ready queue
            if (!info.is_blocked())
                enqueue(info, job, lock);
        }

        // Process dependencies on other jobs. It is done in a separate pass to facilitate propagation of cancel signals (if any).
        for (const auto & job : jobs)
        {
            if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
            {
                for (const auto & dep : job->dependencies)
                {
                    if (scheduled_jobs.contains(dep))
                        continue; // Skip dependencies on scheduled pending jobs (already processed)
                    LoadStatus dep_status = dep->status();
                    if (dep_status == LoadStatus::OK)
                        continue; // Dependency on already successfully finished job -- it's okay.

                    if (dep_status == LoadStatus::PENDING)
                    {
                        // Dependency on not scheduled pending job -- it's bad.
                        // Probably, there is an error in `jobs` set: not all jobs were passed to `schedule()` call.
                        // We are not going to run any dependent job, so cancel them all.
                        std::exception_ptr e;
                        NOEXCEPT_SCOPE({
                            ALLOW_ALLOCATIONS_IN_SCOPE;
                            e = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                                "Load job '{}' -> Load job '{}': not scheduled pending load job (it must be also scheduled), all dependent load jobs are canceled",
                                job->name,
                                dep->name));
                        });
                        finish(lock, job, LoadStatus::CANCELED, e);
                        break; // This job is now finished, stop its dependencies processing
                    }
                    if (dep_status == LoadStatus::FAILED || dep_status == LoadStatus::CANCELED)
                    {
                        // Dependency on already failed or canceled job -- it's okay. Cancel all dependent jobs.
                        std::exception_ptr e;
                        NOEXCEPT_SCOPE({
                            ALLOW_ALLOCATIONS_IN_SCOPE;
                            e = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                                "Load job '{}' -> {}",
                                job->name,
                                getExceptionMessage(dep->exception(), /* with_stack_trace = */ false)));
                        });
                        finish(lock, job, LoadStatus::CANCELED, e);
                        break; // This job is now finished, stop its dependencies processing
                    }
                }
            }
            else
            {
                // Job was already canceled on previous iteration of this cycle -- skip
            }
        }

        return Task(this, std::move(jobs));
    }

    // Increase priority of a job and all its dependencies recursively
    void prioritize(const LoadJobPtr & job, ssize_t new_priority)
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        std::unique_lock lock{mutex};
        prioritize(job, new_priority, lock);
    }

    // Remove finished jobs, cancel scheduled jobs, wait for executing jobs to finish and remove them
    void remove(const LoadJobSet & jobs)
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        std::unique_lock lock{mutex};
        // On the first pass:
        // - cancel all not executing jobs to avoid races
        // - do not wait executing jobs (otherwise, on unlock a worker could start executing a dependent job, that should be canceled)
        for (const auto & job : jobs)
        {
            if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
            {
                if (info->second.is_executing())
                    continue; // Skip executing jobs on the first pass
                std::exception_ptr e;
                NOEXCEPT_SCOPE({
                    ALLOW_ALLOCATIONS_IN_SCOPE;
                    e = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED, "Load job '{}' canceled", job->name));
                });
                finish(lock, job, LoadStatus::CANCELED, e);
            }
        }
        // On the second pass wait for executing jobs to finish
        for (const auto & job : jobs)
        {
            if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
            {
                // Job is currently executing
                chassert(info->second.is_executing());
                lock.unlock();
                job->waitNoThrow(); // Wait for job to finish
                lock.lock();
            }
        }
        // On the third pass all jobs are finished - remove them all
        // It is better to do it under one lock to avoid exposing intermediate states
        for (const auto & job : jobs)
            finished_jobs.erase(job);
    }

    size_t getMaxThreads() const
    {
        std::unique_lock lock{mutex};
        return pool.getMaxThreads();
    }

    size_t getScheduledJobCount() const
    {
        std::unique_lock lock{mutex};
        return scheduled_jobs.size();
    }

private:
    void checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock)
    {
        LoadJobSet left = jobs;
        LoadJobSet visited;
        visited.reserve(left.size());
        while (!left.empty())
        {
            LoadJobPtr job = *left.begin();
            checkCycleImpl(job, left, visited, lock);
        }
    }

    String checkCycleImpl(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock)
    {
        if (!left.contains(job))
            return {}; // Do not consider external dependencies and already processed jobs
        if (auto [_, inserted] = visited.insert(job); !inserted)
        {
            visited.erase(job); // Mark where cycle ends
            return job->name;
        }
        for (const auto & dep : job->dependencies)
        {
            if (auto chain = checkCycleImpl(dep, left, visited, lock); !chain.empty())
            {
                if (!visited.contains(job)) // Check for cycle end
                    throw Exception(ErrorCodes::ASYNC_LOAD_SCHEDULE_FAILED, "Load job dependency cycle detected: {} -> {}", job->name, chain);
                else
                    return fmt::format("{} -> {}", job->name, chain); // chain is not a cycle yet -- continue building
            }
        }
        left.erase(job);
        return {};
    }

    void finish(std::unique_lock<std::mutex> & lock, const LoadJobPtr & job, LoadStatus status, std::exception_ptr exception_from_job = {})
    {
        if (status == LoadStatus::OK)
        {
            // Notify waiters
            job->ok();

            // Update dependent jobs and enqueue if ready
            chassert(scheduled_jobs.contains(job)); // Job was pending
            for (const auto & dep : scheduled_jobs[job].dependent_jobs)
            {
                chassert(scheduled_jobs.contains(dep)); // All depended jobs must be pending
                Info & dep_info = scheduled_jobs[dep];
                dep_info.dependencies_left--;
                if (!dep_info.is_blocked())
                    enqueue(dep_info, dep, lock);
            }
        }
        else
        {
            // Notify waiters
            if (status == LoadStatus::FAILED)
                job->failed(exception_from_job);
            else if (status == LoadStatus::CANCELED)
                job->canceled(exception_from_job);

            chassert(scheduled_jobs.contains(job)); // Job was pending
            Info & info = scheduled_jobs[job];
            if (info.is_ready())
            {
                ready_queue.erase(info.key());
                info.ready_seqno = 0;
            }

            // Recurse into all dependent jobs
            LoadJobSet dependent;
            dependent.swap(info.dependent_jobs); // To avoid container modification during recursion
            for (const auto & dep : dependent)
            {
                std::exception_ptr e;
                NOEXCEPT_SCOPE({
                    ALLOW_ALLOCATIONS_IN_SCOPE;
                    e = std::make_exception_ptr(
                        Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                            "Load job '{}' -> {}",
                            dep->name,
                            getExceptionMessage(exception_from_job, /* with_stack_trace = */ false)));
                });
                finish(lock, dep, LoadStatus::CANCELED, e);
            }

            // Clean dependency graph edges
            for (const auto & dep : job->dependencies)
                if (auto dep_info = scheduled_jobs.find(dep); dep_info != scheduled_jobs.end())
                    dep_info->second.dependent_jobs.erase(job);
        }

        // Job became finished
        scheduled_jobs.erase(job);
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            finished_jobs.insert(job);
        });
    }

    void prioritize(const LoadJobPtr & job, ssize_t new_priority, std::unique_lock<std::mutex> & lock)
    {
        if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
        {
            if (info->second.priority >= new_priority)
                return; // Never lower priority

            // Update priority and push job forward through ready queue if needed
            if (info->second.ready_seqno)
                ready_queue.erase(info->second.key());
            info->second.priority = new_priority;
            job->priority.store(new_priority); // Set user-facing priority (may affect executing jobs)
            if (info->second.ready_seqno)
            {
                NOEXCEPT_SCOPE({
                    ALLOW_ALLOCATIONS_IN_SCOPE;
                    ready_queue.emplace(info->second.key(), job);
                });
            }

            // Recurse into dependencies
            for (const auto & dep : job->dependencies)
                prioritize(dep, new_priority, lock);
        }
    }

    void enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock)
    {
        chassert(!info.is_blocked());
        chassert(info.ready_seqno == 0);
        info.ready_seqno = ++last_ready_seqno;
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            ready_queue.emplace(info.key(), job);
        });

        if (is_running && workers < max_threads) // TODO(serxa): Can we make max_thread changeable in runtime?
            spawn(lock);
    }

    void spawn(std::unique_lock<std::mutex> &)
    {
        workers++;
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            pool.scheduleOrThrowOnError([this] { worker(); });
        });
    }

    void worker()
    {
        DENY_ALLOCATIONS_IN_SCOPE;

        LoadJobPtr job;
        std::exception_ptr exception_from_job;
        while (true)
        {
            /// This is inside the loop to also reset previous thread names set inside the jobs
            setThreadName("AsyncLoader");

            {
                std::unique_lock lock{mutex};

                // Handle just executed job
                if (exception_from_job)
                    finish(lock, job, LoadStatus::FAILED, exception_from_job);
                else if (job)
                    finish(lock, job, LoadStatus::OK);

                if (!is_running || ready_queue.empty())
                {
                    workers--;
                    return;
                }

                // Take next job to be executed from the ready queue
                auto it = ready_queue.begin();
                job = it->second;
                ready_queue.erase(it);
                scheduled_jobs.find(job)->second.ready_seqno = 0; // This job is no longer in the ready queue
            }

            ALLOW_ALLOCATIONS_IN_SCOPE;

            try
            {
                job->func(job);
                exception_from_job = {};
            }
            catch (...)
            {
                NOEXCEPT_SCOPE({
                    if (log_failures)
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    exception_from_job = std::make_exception_ptr(
                        Exception(ErrorCodes::ASYNC_LOAD_FAILED,
                            "Load job '{}' failed: {}",
                            job->name,
                            getCurrentExceptionMessage(/* with_stacktrace = */ true)));
                });
            }
        }
    }

    const bool log_failures;

    mutable std::mutex mutex;
    bool is_running = false;

    // TODO(serxa): add metrics for number of jobs in every state

    // Full set of scheduled pending jobs along with scheduling info
    std::unordered_map<LoadJobPtr, Info> scheduled_jobs;

    // Subset of scheduled pending jobs with resolved dependencies (waiting for a thread to be executed)
    // Represent a queue of jobs in order of decreasing priority and FIFO for jobs with equal priorities
    std::map<ReadyKey, LoadJobPtr> ready_queue;

    // Set of finished jobs (for introspection only, until job is removed)
    LoadJobSet finished_jobs;

    // Increasing counter for ReadyKey assignment (to preserve FIFO order of jobs with equal priority)
    UInt64 last_ready_seqno = 0;

    // For executing jobs. Note that we avoid using an internal queue of the pool to be able to prioritize jobs
    size_t max_threads;
    size_t workers = 0;
    ThreadPool pool;
};

}
