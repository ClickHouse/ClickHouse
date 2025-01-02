#include <Common/AsyncLoader.h>

#include <limits>
#include <optional>
#include <magic_enum.hpp>
#include <fmt/format.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>


namespace ProfileEvents
{
    extern const Event AsyncLoaderWaitMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ASYNC_LOAD_CYCLE;
    extern const int ASYNC_LOAD_FAILED;
    extern const int ASYNC_LOAD_CANCELED;
    extern const int ASYNC_LOAD_WAIT_FAILED;
    extern const int LOGICAL_ERROR;
}

static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;

void logAboutProgress(LoggerPtr log, size_t processed, size_t total, AtomicStopwatch & watch)
{
    if (total && (processed % PRINT_MESSAGE_EACH_N_OBJECTS == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS)))
    {
        LOG_INFO(log, "Processed: {:.1f}%", static_cast<double>(processed) * 100.0 / total);
        watch.restart();
    }
}

AsyncLoader::Pool::Pool(const AsyncLoader::PoolInitializer & init)
    : name(init.name)
    , priority(init.priority)
    , max_threads(init.max_threads > 0 ? init.max_threads : getNumberOfCPUCoresToUse())
    , thread_pool(std::make_unique<ThreadPool>(
          init.metric_threads,
          init.metric_active_threads,
          init.metric_scheduled_threads,
          /* max_threads = */ ThreadPool::MAX_THEORETICAL_THREAD_COUNT, // Unlimited number of threads, we do worker management ourselves
          /* max_free_threads = */ 0, // We do not require free threads
          /* queue_size = */ 0)) // Unlimited queue to avoid blocking during worker spawning
{}

AsyncLoader::Pool::Pool(Pool&& o) noexcept
    : name(o.name)
    , priority(o.priority)
    , ready_queue(std::move(o.ready_queue))
    , max_threads(o.max_threads)
    , workers(o.workers)
    , suspended_workers(o.suspended_workers.load()) // All these constructors are needed because std::atomic is neither copy-constructible, nor move-constructible. We never move pools after init, so it is safe.
    , thread_pool(std::move(o.thread_pool))
{}

void cancelOnDependencyFailure(const LoadJobPtr & self, const LoadJobPtr & dependency, std::exception_ptr & cancel)
{
    cancel = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
        "Load job '{}' -> {}",
        self->name,
        getExceptionMessage(dependency->exception(), /* with_stacktrace = */ false)));
}

void ignoreDependencyFailure(const LoadJobPtr &, const LoadJobPtr &, std::exception_ptr &)
{
    // No-op
}

LoadStatus LoadJob::status() const
{
    std::unique_lock lock{mutex};
    return load_status;
}

std::exception_ptr LoadJob::exception() const
{
    std::unique_lock lock{mutex};
    return load_exception;
}

size_t LoadJob::executionPool() const
{
    return execution_pool_id;
}

size_t LoadJob::pool() const
{
    return pool_id;
}

size_t LoadJob::waitersCount() const
{
    std::unique_lock lock{mutex};
    return waiters;
}

void LoadJob::ok()
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::OK;
    finish();
}

void LoadJob::failed(const std::exception_ptr & ptr)
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::FAILED;
    load_exception = ptr;
    finish();
}

void LoadJob::canceled(const std::exception_ptr & ptr)
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::CANCELED;
    load_exception = ptr;
    finish();
}

void LoadJob::finish()
{
    // To ensure functions are destructed before `AsyncLoader::wait()` returns
    func = {};
    dependency_failure = {};

    finish_time = std::chrono::system_clock::now();
    if (waiters > 0)
        finished.notify_all();
    else
    {
        on_waiters_increment = {};
        on_waiters_decrement = {};
    }
}

void LoadJob::scheduled(UInt64 job_id_)
{
    chassert(job_id == 0); // Job cannot be scheduled twice
    job_id = job_id_;
    schedule_time = std::chrono::system_clock::now();
}

void LoadJob::enqueued()
{
    if (enqueue_time.load() == TimePoint{}) // Do not rewrite in case of requeue
        enqueue_time = std::chrono::system_clock::now();
}

void LoadJob::execute(AsyncLoader & loader, size_t pool, const LoadJobPtr & self)
{
    execution_pool_id.store(pool);
    start_time = std::chrono::system_clock::now();
    func(loader, self);
}


LoadTask::LoadTask(AsyncLoader & loader_, LoadJobSet && jobs_, LoadJobSet && goal_jobs_)
    : loader(loader_)
    , jobs(std::move(jobs_))
    , goal_jobs(std::move(goal_jobs_))
{}

LoadTask::~LoadTask()
{
    remove();
}

void LoadTask::merge(const LoadTaskPtr & task)
{
    chassert(&loader == &task->loader);
    jobs.merge(task->jobs);
    goal_jobs.merge(task->goal_jobs);
}

void LoadTask::schedule()
{
    loader.schedule(*this);
}

void LoadTask::remove()
{
    if (!jobs.empty())
    {
        loader.remove(jobs);
        jobs.clear();
        goal_jobs.clear();
    }
}

AsyncLoader::AsyncLoader(std::vector<PoolInitializer> pool_initializers, bool log_failures_, bool log_progress_, bool log_events_)
    : log_failures(log_failures_)
    , log_progress(log_progress_)
    , log_events(log_events_)
    , log(getLogger("AsyncLoader"))
{
    pools.reserve(pool_initializers.size());
    for (auto && init : pool_initializers)
        pools.push_back(Pool(init));
}

AsyncLoader::~AsyncLoader()
{
    // All `LoadTask` objects should be destructed before AsyncLoader destruction because they hold a reference.
    // To make sure we check for all pending jobs to be finished.
    {
        std::unique_lock lock{mutex};
        if (!scheduled_jobs.empty() || !finished_jobs.empty())
        {
            std::vector<String> scheduled;
            std::vector<String> finished;
            scheduled.reserve(scheduled_jobs.size());
            finished.reserve(finished_jobs.size());
            for (const auto & [job, _] : scheduled_jobs)
                scheduled.push_back(job->name);
            for (const auto & job : finished_jobs)
                finished.push_back(job->name);
            LOG_ERROR(log, "Bug. Destruction with pending ({}) and finished ({}) load jobs.", fmt::join(scheduled, ", "), fmt::join(finished, ", "));
            abort();
        }
    }

    // When all jobs are done we could still have finalizing workers.
    // These workers could call updateCurrentPriorityAndSpawn() that scans all pools.
    // We need to stop all of them before destructing any of them.
    stop();
}

void AsyncLoader::start()
{
    std::unique_lock lock{mutex};
    is_running = true;
    updateCurrentPriorityAndSpawn(lock);
}

void AsyncLoader::wait()
{
    // Because job can create new jobs in other pools we have to recheck in cycle.
    // Also wait for all workers to finish to avoid races on `pool.workers`,
    // which can decrease even after all jobs are already finished.
    std::unique_lock lock{mutex};
    while (!scheduled_jobs.empty() || hasWorker(lock))
    {
        lock.unlock();
        for (auto & p : pools)
            p.thread_pool->wait();
        lock.lock();

        // If there is no way for all jobs to finish, throw LOGICAL_ERROR instead of deadlock
        if (!scheduled_jobs.empty() && !hasWorker(lock))
        {
            std::vector<String> names;
            names.reserve(scheduled_jobs.size());
            for (const auto & [job, _] : scheduled_jobs)
                names.push_back(job->name);
            LOG_ERROR(log, "Waiting for load jobs to finish while being stopped: {}.", fmt::join(names, ", "));
            abort();
        }
    }
}

void AsyncLoader::stop()
{
    {
        std::unique_lock lock{mutex};
        is_running = false; // NOTE: there is no need to notify because workers never wait
    }

    // Wait for all currently running jobs to finish (and do NOT wait all pending jobs)
    for (auto & p : pools)
        p.thread_pool->wait();
}

void AsyncLoader::schedule(LoadTask & task)
{
    chassert(this == &task.loader);
    schedule(task.jobs);
}

void AsyncLoader::schedule(const LoadTaskPtr & task)
{
    chassert(this == &task->loader);
    schedule(task->jobs);
}

void AsyncLoader::schedule(const LoadTaskPtrs & tasks)
{
    LoadJobSet all_jobs;
    for (const auto & task : tasks)
    {
        chassert(this == &task->loader);
        all_jobs.insert(task->jobs.begin(), task->jobs.end());
    }
    schedule(all_jobs);
}

void AsyncLoader::schedule(const LoadJobSet & jobs_to_schedule)
{
    std::unique_lock lock{mutex};

    // Restart watches after idle period
    if (scheduled_jobs.empty())
    {
        busy_period_start_time = std::chrono::system_clock::now();
        stopwatch.restart();
        old_jobs = finished_jobs.size();
    }

    // Pass 1. Make set of jobs to schedule:
    // 1) exclude already scheduled or finished jobs
    // 2) include assigned job dependencies (that are not yet scheduled)
    LoadJobSet jobs;
    for (const auto & job : jobs_to_schedule)
        gatherNotScheduled(job, jobs, lock);

    // Ensure scheduled_jobs graph will have no cycles. The only way to get a cycle is to add a cycle, assuming old jobs cannot reference new ones.
    checkCycle(jobs, lock);

    // We do not want any exception to be throws after this point, because the following code is not exception-safe
    DENY_ALLOCATIONS_IN_SCOPE;

    // Pass 2. Schedule all incoming jobs
    for (const auto & job : jobs)
    {
        chassert(job->pool() < pools.size());
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            scheduled_jobs.try_emplace(job);
            job->scheduled(++last_job_id);
            if (log_events)
                LOG_DEBUG(log, "Schedule load job '{}' into {}", job->name, getPoolName(job->pool()));
        });
    }

    // Pass 3. Process dependencies on scheduled jobs, priority inheritance
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

                // Priority inheritance: prioritize deps to have at least given `pool.priority` to avoid priority inversion
                prioritize(dep, job->pool_id, lock);
            }
        }

        // Enqueue non-blocked jobs (w/o dependencies) to ready queue
        if (!info.isBlocked())
            enqueue(info, job, lock);
    }

    // Pass 4: Process dependencies on other jobs.
    // It is done in a separate pass to facilitate cancelling due to already failed dependencies.
    for (const auto & job : jobs)
    {
        if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
        {
            for (const auto & dep : job->dependencies)
            {
                if (scheduled_jobs.contains(dep))
                    continue; // Skip dependencies on scheduled jobs (already processed in pass 3)
                LoadStatus dep_status = dep->status();
                if (dep_status == LoadStatus::OK)
                    continue; // Dependency on already successfully finished job -- it's okay.

                // Dependency on assigned job -- it's bad.
                // Probably, there is an error in `jobs` set, `gatherNotScheduled()` should have fixed it.
                chassert(dep_status != LoadStatus::PENDING);

                if (dep_status == LoadStatus::FAILED || dep_status == LoadStatus::CANCELED)
                {
                    // Dependency on already failed or canceled job -- it's okay.
                    // Process as usual (may lead to cancel of all dependent jobs).
                    std::exception_ptr cancel;
                    NOEXCEPT_SCOPE({
                        ALLOW_ALLOCATIONS_IN_SCOPE;
                        if (job->dependency_failure)
                            job->dependency_failure(job, dep, cancel);
                    });
                    if (cancel)
                    {
                        finish(job, LoadStatus::CANCELED, cancel, lock);
                        break; // This job is now finished, stop its dependencies processing
                    }
                }
            }
        }
        else
        {
            // Job was already canceled on previous iteration of this cycle -- skip
        }
    }
}

void AsyncLoader::gatherNotScheduled(const LoadJobPtr & job, LoadJobSet & jobs, std::unique_lock<std::mutex> & lock)
{
    if (job->status() == LoadStatus::PENDING && !scheduled_jobs.contains(job) && !jobs.contains(job))
    {
        jobs.insert(job);
        for (const auto & dep : job->dependencies)
            gatherNotScheduled(dep, jobs, lock);
    }
}

void AsyncLoader::prioritize(const LoadJobPtr & job, size_t new_pool)
{
    if (!job)
        return;
    chassert(new_pool < pools.size());

    DENY_ALLOCATIONS_IN_SCOPE;
    std::unique_lock lock{mutex};
    prioritize(job, new_pool, lock);
}

void AsyncLoader::wait(const LoadJobPtr & job, bool no_throw)
{
    std::unique_lock job_lock{job->mutex};
    wait(job_lock, job);
    if (!no_throw && job->load_exception)
        throw Exception(ErrorCodes::ASYNC_LOAD_WAIT_FAILED, "Waited job failed: {}", getExceptionMessage(job->load_exception, /* with_stacktrace = */ false));
}

void AsyncLoader::remove(const LoadJobSet & jobs)
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
            if (info->second.isExecuting())
                continue; // Skip executing jobs on the first pass
            std::exception_ptr e;
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                e = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED, "Load job '{}' canceled", job->name));
            });
            finish(job, LoadStatus::CANCELED, e, lock);
        }
    }
    // On the second pass wait for executing jobs to finish
    for (const auto & job : jobs)
    {
        if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
        {
            // Job is currently executing
            ALLOW_ALLOCATIONS_IN_SCOPE;
            chassert(info->second.isExecuting());
            lock.unlock();
            wait(job, /* no_throw = */ true); // Wait for job to finish
            lock.lock();
        }
    }
    // On the third pass all jobs are finished - remove them all
    // It is better to do it under one lock to avoid exposing intermediate states
    for (const auto & job : jobs)
    {
        size_t erased = finished_jobs.erase(job);
        if (old_jobs >= erased && job->finishTime() != LoadJob::TimePoint{} && job->finishTime() < busy_period_start_time)
            old_jobs -= erased;
    }
}

void AsyncLoader::setMaxThreads(size_t pool, size_t value)
{
    if (value == 0)
        value = getNumberOfCPUCoresToUse();
    std::unique_lock lock{mutex};
    auto & p = pools[pool];
    // Note that underlying `ThreadPool` always has unlimited `queue_size` and `max_threads`.
    // Worker management is done by `AsyncLoader` based on `Pool::max_threads + Pool::suspended_workers` instead.
    p.max_threads = value;
    if (!is_running)
        return;
    for (size_t i = 0; canSpawnWorker(p, lock) && i < p.ready_queue.size(); i++)
        spawn(p, lock);
}

size_t AsyncLoader::getMaxThreads(size_t pool) const
{
    std::unique_lock lock{mutex};
    return pools[pool].max_threads;
}

const String & AsyncLoader::getPoolName(size_t pool) const
{
    return pools[pool].name; // NOTE: lock is not needed because `name` is const and `pools` are immutable
}

Priority AsyncLoader::getPoolPriority(size_t pool) const
{
    return pools[pool].priority; // NOTE: lock is not needed because `priority` is const and `pools` are immutable
}

size_t AsyncLoader::getScheduledJobCount() const
{
    std::unique_lock lock{mutex};
    return scheduled_jobs.size();
}

std::vector<AsyncLoader::JobState> AsyncLoader::getJobStates() const
{
    std::unique_lock lock{mutex};
    std::multimap<String, JobState> states;
    for (const auto & [job, info] : scheduled_jobs)
        states.emplace(job->name, JobState{
            .job = job,
            .dependencies_left = info.dependencies_left,
            .ready_seqno = info.ready_seqno,
            .is_blocked = info.isBlocked(),
            .is_ready = info.isReady(),
            .is_executing = info.isExecuting()
        });
    for (const auto & job : finished_jobs)
        states.emplace(job->name, JobState{.job = job});
    lock.unlock();
    std::vector<JobState> result;
    for (auto && [_, state] : states)
        result.emplace_back(std::move(state));
    return result;
}

size_t AsyncLoader::suspendedWorkersCount(size_t pool_id)
{
    return pools[pool_id].suspended_workers.load();
}

void AsyncLoader::checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock)
{
    LoadJobSet left = jobs;
    LoadJobSet visited;
    visited.reserve(left.size());
    while (!left.empty())
    {
        LoadJobPtr job = *left.begin();
        checkCycle(job, left, visited, lock);
    }
}

String AsyncLoader::checkCycle(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock)
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
        if (auto chain = checkCycle(dep, left, visited, lock); !chain.empty())
        {
            if (!visited.contains(job)) // Check for cycle end
                throw Exception(ErrorCodes::ASYNC_LOAD_CYCLE, "Load job dependency cycle detected: {} -> {}", job->name, chain);
            return fmt::format("{} -> {}", job->name, chain); // chain is not a cycle yet -- continue building
        }
    }
    left.erase(job);
    return {};
}

void AsyncLoader::finish(const LoadJobPtr & job, LoadStatus status, std::exception_ptr reason, std::unique_lock<std::mutex> & lock)
{
    chassert(scheduled_jobs.contains(job)); // Job was pending

    // Notify waiters
    if (status == LoadStatus::OK)
        job->ok();
    else if (status == LoadStatus::FAILED)
        job->failed(reason);
    else if (status == LoadStatus::CANCELED)
        job->canceled(reason);

    if (log_events)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            LOG_DEBUG(log, "Finish load job '{}' with status {}", job->name, magic_enum::enum_name(status));
        });
    }

    Info & info = scheduled_jobs[job];
    if (info.isReady())
    {
        // Job could be in ready queue (on cancel) -- must be dequeued
        pools[job->pool_id].ready_queue.erase(info.ready_seqno);
        info.ready_seqno = 0;
    }

    // To avoid container modification during recursion (during clean dependency graph edges below)
    LoadJobSet dependent;
    dependent.swap(info.dependent_jobs);

    // Update dependent jobs
    for (const auto & dpt : dependent)
    {
        auto dpt_info = scheduled_jobs.find(dpt);
        if (dpt_info == scheduled_jobs.end())
            continue;
        dpt_info->second.dependencies_left--;
        if (!dpt_info->second.isBlocked())
            enqueue(dpt_info->second, dpt, lock);

        if (status != LoadStatus::OK)
        {
            std::exception_ptr cancel;
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                if (dpt->dependency_failure)
                    dpt->dependency_failure(dpt, job, cancel);
            });
            // Recurse into dependent job if it should be canceled
            if (cancel)
                finish(dpt, LoadStatus::CANCELED, cancel, lock);
        }
    }

    // Clean dependency graph edges pointing to canceled jobs
    if (status != LoadStatus::OK)
    {
        for (const auto & dep : job->dependencies)
        {
            if (auto dep_info = scheduled_jobs.find(dep); dep_info != scheduled_jobs.end())
                dep_info->second.dependent_jobs.erase(job);
        }
    }

    // Job became finished
    scheduled_jobs.erase(job);
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        finished_jobs.insert(job);
        if (log_progress)
            logAboutProgress(log, finished_jobs.size() - old_jobs, finished_jobs.size() + scheduled_jobs.size() - old_jobs, stopwatch);
    });
}

void AsyncLoader::prioritize(const LoadJobPtr & job, size_t new_pool_id, std::unique_lock<std::mutex> & lock)
{
    Pool & old_pool = pools[job->pool_id];
    Pool & new_pool = pools[new_pool_id];
    if (old_pool.priority <= new_pool.priority)
        return; // Never lower priority or change pool leaving the same priority

    // Note that there is no point in prioritizing finished jobs, but because we do not lock `job.mutex` here (due to recursion),
    // Races are inevitable, so we prioritize all job unconditionally: both finished and pending.

    if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
    {
        // Requeue job into the new pool queue without allocations
        if (UInt64 ready_seqno = info->second.ready_seqno)
        {
            new_pool.ready_queue.insert(old_pool.ready_queue.extract(ready_seqno));
            if (canSpawnWorker(new_pool, lock))
                spawn(new_pool, lock);
        }
    }

    job->pool_id.store(new_pool_id);

    if (log_events)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            LOG_DEBUG(log, "Prioritize load job '{}': {} -> {}", job->name, old_pool.name, new_pool.name);
        });
    }

    // Recurse into dependencies
    for (const auto & dep : job->dependencies)
        prioritize(dep, new_pool_id, lock);
}

void AsyncLoader::enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock)
{
    chassert(!info.isBlocked());
    chassert(info.ready_seqno == 0);
    info.ready_seqno = ++last_ready_seqno;
    Pool & pool = pools[job->pool_id];
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        pool.ready_queue.emplace(info.ready_seqno, job);
    });

    job->enqueued();

    if (canSpawnWorker(pool, lock))
        spawn(pool, lock);
}

// Keep track of currently executing load jobs to be able to:
// 1) Detect "wait dependent" deadlocks -- throw LOGICAL_ERROR
//    (when job A function waits for job B that depends on job A)
// 2) Detect "wait not scheduled" deadlocks -- throw LOGICAL_ERROR
//    (thread T is waiting on an assigned job A, but job A is not yet scheduled)
// 3) Resolve "priority inversion" deadlocks -- apply priority inheritance
//    (when high-priority job A function waits for a lower-priority job B, and B never starts due to its priority)
// 4) Resolve "blocked pool" deadlocks -- spawn more workers
//    (when job A in pool P waits for another ready job B in P, but B never starts because there are no free workers in P)
thread_local LoadJob * current_load_job = nullptr;

size_t currentPoolOr(size_t pool)
{
    return current_load_job ? current_load_job->executionPool() : pool;
}

bool detectWaitDependentDeadlock(const LoadJobPtr & waited)
{
    if (waited.get() == current_load_job)
        return true;
    for (const auto & dep : waited->dependencies)
    {
        if (detectWaitDependentDeadlock(dep))
            return true;
    }
    return false;
}

void AsyncLoader::wait(std::unique_lock<std::mutex> & job_lock, const LoadJobPtr & job)
{
    // Ensure job we are going to wait was scheduled to avoid "wait not scheduled" deadlocks
    if (job->job_id == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Load job '{}' waits for not scheduled load job '{}'", current_load_job->name, job->name);

    scope_guard suspended_lock;

    // Deadlock detection and resolution
    if (current_load_job && job->load_status == LoadStatus::PENDING)
    {
        if (detectWaitDependentDeadlock(job))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Load job '{}' waits for dependent load job '{}'", current_load_job->name, job->name);

        auto worker_pool = current_load_job->executionPool();
        auto worker_priority = getPoolPriority(worker_pool);
        auto job_priority = getPoolPriority(job->pool_id);

        // Waiting for a lower-priority job ("priority inversion" deadlock) is resolved using priority inheritance.
        if (worker_priority < job_priority)
        {
            job_lock.unlock(); // Avoid reverse locking order
            prioritize(job, worker_pool);
            job_lock.lock();
        }

        // Spawn more workers to avoid exhaustion of worker pool ("blocked pool" deadlock)
        if (worker_pool == job->pool_id)
        {
            job_lock.unlock(); // Avoid reverse locking order
            std::unique_lock lock{mutex};
            job_lock.lock();

            // Rechecks are required because we have reacquired mutexes
            if (job->load_status != LoadStatus::PENDING)
                return; // Job is already done, no wait required

            if (worker_pool == job->pool_id)
            {
                // To resolve "blocked pool" deadlocks we spawn a new worker for every suspended worker, if required
                // This can lead to a visible excess of `max_threads` specified for a pool,
                // but actual number of NOT suspended workers may exceed `max_threads` ONLY in intermittent state.
                Pool & pool = pools[worker_pool];
                pool.suspended_workers.fetch_add(1);
                suspended_lock = [&pool] { chassert(pool.suspended_workers.load()); pool.suspended_workers.fetch_sub(1); };
                if (canSpawnWorker(pool, lock))
                    spawn(pool, lock);
            }
        }
    }

    if (job->load_status != LoadStatus::PENDING) // Shortcut just to avoid incrementing ProfileEvents
        return;

    if (log_events)
        LOG_DEBUG(log, "Wait load job '{}' in {}", job->name, getPoolName(job->pool_id));

    if (job->on_waiters_increment)
        job->on_waiters_increment(job);

    // WARNING: it is important not to throw below this point to avoid `on_waiters_increment` call w/o matching `on_waiters_decrement` call

    Stopwatch watch;
    job->waiters++;
    job->finished.wait(job_lock, [&] { return job->load_status != LoadStatus::PENDING; });
    job->waiters--;
    ProfileEvents::increment(ProfileEvents::AsyncLoaderWaitMicroseconds, watch.elapsedMicroseconds());

    if (job->on_waiters_decrement)
        job->on_waiters_decrement(job);

    if (job->waiters == 0)
    {
        job->on_waiters_increment = {};
        job->on_waiters_decrement = {};
    }
}

bool AsyncLoader::canSpawnWorker(Pool & pool, std::unique_lock<std::mutex> &)
{
    // TODO(serxa): optimization: we should not spawn new worker on the first enqueue during `finish()` because current worker will take this job.
    return is_running
        && !pool.ready_queue.empty()
        && pool.workers < pool.max_threads + pool.suspended_workers.load()
        && (!current_priority || *current_priority >= pool.priority);
}

bool AsyncLoader::canWorkerLive(Pool & pool, std::unique_lock<std::mutex> &)
{
    return is_running
        && !pool.ready_queue.empty()
        && pool.workers <= pool.max_threads + pool.suspended_workers.load()
        && (!current_priority || *current_priority >= pool.priority);
}

void AsyncLoader::setCurrentPriority(std::unique_lock<std::mutex> &, std::optional<Priority> priority)
{
    if (log_events && current_priority != priority)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            LOG_DEBUG(log, "Change current priority: {} -> {}",
                current_priority ? std::to_string(*current_priority) : "none",
                priority ? std::to_string(*priority) : "none");
        });
    }
    current_priority = priority;
}

void AsyncLoader::updateCurrentPriorityAndSpawn(std::unique_lock<std::mutex> & lock)
{
    // Find current priority.
    // NOTE: We assume low number of pools, so O(N) scans are fine.
    std::optional<Priority> priority;
    for (Pool & pool : pools)
    {
        if (pool.isActive() && (!priority || *priority > pool.priority))
            priority = pool.priority;
    }
    setCurrentPriority(lock, priority);

    // Spawn workers in all pools with current priority
    for (Pool & pool : pools)
    {
        for (size_t i = 0; canSpawnWorker(pool, lock) && i < pool.ready_queue.size(); i++)
            spawn(pool, lock);
    }
}

void AsyncLoader::spawn(Pool & pool, std::unique_lock<std::mutex> & lock)
{
    setCurrentPriority(lock, pool.priority); // canSpawnWorker() ensures this would not decrease current_priority
    pool.workers++;
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        if (log_events)
            LOG_DEBUG(log, "Spawn loader worker #{} in {}", pool.workers, pool.name);
        auto blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();
        pool.thread_pool->scheduleOrThrowOnError([this, &pool] { worker(pool); });
    });
}

void AsyncLoader::worker(Pool & pool)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    size_t pool_id = &pool - &*pools.begin();
    LoadJobPtr job;
    std::exception_ptr exception_from_job;
    while (true)
    {
        // This is inside the loop to also reset previous thread names set inside the jobs
        setThreadName(pool.name.c_str());

        {
            std::unique_lock lock{mutex};

            // Handle just executed job
            if (exception_from_job)
                finish(job, LoadStatus::FAILED, exception_from_job, lock);
            else if (job)
                finish(job, LoadStatus::OK, {}, lock);

            if (!canWorkerLive(pool, lock))
            {
                if (log_events)
                {
                    NOEXCEPT_SCOPE({
                        ALLOW_ALLOCATIONS_IN_SCOPE;
                        LOG_DEBUG(log, "Stop worker in {}", pool.name);
                    });
                }
                if (--pool.workers == 0)
                    updateCurrentPriorityAndSpawn(lock); // It will spawn lower priority workers if needed
                return;
            }

            // Take next job to be executed from the ready queue
            auto it = pool.ready_queue.begin();
            job = it->second;
            pool.ready_queue.erase(it);
            scheduled_jobs.find(job)->second.ready_seqno = 0; // This job is no longer in the ready queue

            if (log_events)
            {
                NOEXCEPT_SCOPE({
                    ALLOW_ALLOCATIONS_IN_SCOPE;
                    LOG_DEBUG(log, "Execute load job '{}' in {}", job->name, pool.name);
                });
            }
        }

        ALLOW_ALLOCATIONS_IN_SCOPE;

        try
        {
            current_load_job = job.get();
            SCOPE_EXIT({ current_load_job = nullptr; }); // Note that recursive job execution is not supported, but jobs can wait one another
            job->execute(*this, pool_id, job);
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

bool AsyncLoader::hasWorker(std::unique_lock<std::mutex> &) const
{
    for (const Pool & pool : pools)
    {
        if (pool.workers > 0)
            return true;
    }
    return false;
}

}
