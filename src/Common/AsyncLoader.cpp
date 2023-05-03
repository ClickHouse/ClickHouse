#include <Common/AsyncLoader.h>

#include <base/defines.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ASYNC_LOAD_CYCLE;
    extern const int ASYNC_LOAD_FAILED;
    extern const int ASYNC_LOAD_CANCELED;
}

static constexpr size_t PRINT_MESSAGE_EACH_N_OBJECTS = 256;
static constexpr size_t PRINT_MESSAGE_EACH_N_SECONDS = 5;

void logAboutProgress(Poco::Logger * log, size_t processed, size_t total, AtomicStopwatch & watch)
{
    if (processed % PRINT_MESSAGE_EACH_N_OBJECTS == 0 || watch.compareAndRestart(PRINT_MESSAGE_EACH_N_SECONDS))
    {
        LOG_INFO(log, "Processed: {}%", processed * 100.0 / total);
        watch.restart();
    }
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

ssize_t LoadJob::priority() const
{
    return load_priority;
}

void LoadJob::wait() const
{
    std::unique_lock lock{mutex};
    waiters++;
    finished.wait(lock, [this] { return load_status != LoadStatus::PENDING; });
    waiters--;
    if (load_exception)
        std::rethrow_exception(load_exception);
}

void LoadJob::waitNoThrow() const noexcept
{
    std::unique_lock lock{mutex};
    waiters++;
    finished.wait(lock, [this] { return load_status != LoadStatus::PENDING; });
    waiters--;
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
    func = {}; // To ensure job function is destructed before `AsyncLoader::wait()` and `LoadJob::wait()` return
    finish_time = std::chrono::system_clock::now();
    if (waiters > 0)
        finished.notify_all();
}

void LoadJob::scheduled()
{
    schedule_time = std::chrono::system_clock::now();
}

void LoadJob::enqueued()
{
    if (enqueue_time.load() == TimePoint{}) // Do not rewrite in case of requeue
        enqueue_time = std::chrono::system_clock::now();
}

void LoadJob::execute(const LoadJobPtr & self)
{
    start_time = std::chrono::system_clock::now();
    func(self);
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
    }
}

void LoadTask::detach()
{
    jobs.clear();
}

AsyncLoader::AsyncLoader(Metric metric_threads, Metric metric_active_threads, size_t max_threads_, bool log_failures_)
    : log_failures(log_failures_)
    , log(&Poco::Logger::get("AsyncLoader"))
    , max_threads(max_threads_)
    , pool(metric_threads, metric_active_threads, max_threads)
{

}

AsyncLoader::~AsyncLoader()
{
    stop();
}

void AsyncLoader::start()
{
    std::unique_lock lock{mutex};
    is_running = true;
    for (size_t i = 0; workers < max_threads && i < ready_queue.size(); i++)
        spawn(lock);
}

void AsyncLoader::wait()
{
    pool.wait();
}

void AsyncLoader::stop()
{
    {
        std::unique_lock lock{mutex};
        is_running = false;
        // NOTE: there is no need to notify because workers never wait
    }
    pool.wait();
}

void AsyncLoader::schedule(LoadTask & task)
{
    chassert(this == &task.loader);
    scheduleImpl(task.jobs);
}

void AsyncLoader::schedule(const LoadTaskPtr & task)
{
    chassert(this == &task->loader);
    scheduleImpl(task->jobs);
}

void AsyncLoader::schedule(const std::vector<LoadTaskPtr> & tasks)
{
    LoadJobSet all_jobs;
    for (const auto & task : tasks)
    {
        chassert(this == &task->loader);
        all_jobs.insert(task->jobs.begin(), task->jobs.end());
    }
    scheduleImpl(all_jobs);
}

void AsyncLoader::scheduleImpl(const LoadJobSet & input_jobs)
{
    std::unique_lock lock{mutex};

    // Restart watches after idle period
    if (scheduled_jobs.empty())
    {
        busy_period_start_time = std::chrono::system_clock::now();
        stopwatch.restart();
        old_jobs = finished_jobs.size();
    }

    // Make set of jobs to schedule:
    // 1) exclude already scheduled or finished jobs
    // 2) include pending dependencies, that are not yet scheduled
    LoadJobSet jobs;
    for (const auto & job : input_jobs)
        gatherNotScheduled(job, jobs, lock);

    // Ensure scheduled_jobs graph will have no cycles. The only way to get a cycle is to add a cycle, assuming old jobs cannot reference new ones.
    checkCycle(jobs, lock);

    // We do not want any exception to be throws after this point, because the following code is not exception-safe
    DENY_ALLOCATIONS_IN_SCOPE;

    // Schedule all incoming jobs
    for (const auto & job : jobs)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            scheduled_jobs.emplace(job, Info{.initial_priority = job->load_priority, .priority = job->load_priority});
            job->scheduled();
        });
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
                prioritize(dep, info.priority, lock);
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

                // Dependency on not scheduled pending job -- it's bad.
                // Probably, there is an error in `jobs` set, `gatherNotScheduled()` should have fixed it.
                chassert(dep_status != LoadStatus::PENDING);

                if (dep_status == LoadStatus::FAILED || dep_status == LoadStatus::CANCELED)
                {
                    // Dependency on already failed or canceled job -- it's okay. Cancel all dependent jobs.
                    std::exception_ptr e;
                    NOEXCEPT_SCOPE({
                        ALLOW_ALLOCATIONS_IN_SCOPE;
                        e = std::make_exception_ptr(Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                            "Load job '{}' -> {}",
                            job->name,
                            getExceptionMessage(dep->exception(), /* with_stacktrace = */ false)));
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

void AsyncLoader::prioritize(const LoadJobPtr & job, ssize_t new_priority)
{
    if (!job)
        return;
    DENY_ALLOCATIONS_IN_SCOPE;
    std::unique_lock lock{mutex};
    prioritize(job, new_priority, lock);
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
    {
        size_t erased = finished_jobs.erase(job);
        if (old_jobs >= erased && job->finishTime() != LoadJob::TimePoint{} && job->finishTime() < busy_period_start_time)
            old_jobs -= erased;
    }
}

void AsyncLoader::setMaxThreads(size_t value)
{
    std::unique_lock lock{mutex};
    pool.setMaxThreads(value);
    pool.setMaxFreeThreads(value);
    pool.setQueueSize(value);
    max_threads = value;
    if (!is_running)
        return;
    for (size_t i = 0; workers < max_threads && i < ready_queue.size(); i++)
        spawn(lock);
}

size_t AsyncLoader::getMaxThreads() const
{
    std::unique_lock lock{mutex};
    return max_threads;
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
            .is_executing = info.is_executing(),
            .is_blocked = info.is_blocked(),
            .is_ready = info.is_ready(),
            .initial_priority = info.initial_priority,
            .ready_seqno = last_ready_seqno
        });
    for (const auto & job : finished_jobs)
        states.emplace(job->name, JobState{.job = job});
    lock.unlock();
    std::vector<JobState> result;
    for (auto && [_, state] : states)
        result.emplace_back(std::move(state));
    return result;
}

void AsyncLoader::checkCycle(const LoadJobSet & jobs, std::unique_lock<std::mutex> & lock)
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

String AsyncLoader::checkCycleImpl(const LoadJobPtr & job, LoadJobSet & left, LoadJobSet & visited, std::unique_lock<std::mutex> & lock)
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
                throw Exception(ErrorCodes::ASYNC_LOAD_CYCLE, "Load job dependency cycle detected: {} -> {}", job->name, chain);
            else
                return fmt::format("{} -> {}", job->name, chain); // chain is not a cycle yet -- continue building
        }
    }
    left.erase(job);
    return {};
}

void AsyncLoader::finish(std::unique_lock<std::mutex> & lock, const LoadJobPtr & job, LoadStatus status, std::exception_ptr exception_from_job)
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
            if (!scheduled_jobs.contains(dep))
                continue; // Job has already been canceled
            std::exception_ptr e;
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                e = std::make_exception_ptr(
                    Exception(ErrorCodes::ASYNC_LOAD_CANCELED,
                        "Load job '{}' -> {}",
                        dep->name,
                        getExceptionMessage(exception_from_job, /* with_stacktrace = */ false)));
            });
            finish(lock, dep, LoadStatus::CANCELED, e);
        }

        // Clean dependency graph edges pointing to canceled jobs
        for (const auto & dep : job->dependencies)
            if (auto dep_info = scheduled_jobs.find(dep); dep_info != scheduled_jobs.end())
                dep_info->second.dependent_jobs.erase(job);
    }

    // Job became finished
    scheduled_jobs.erase(job);
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        finished_jobs.insert(job);
        logAboutProgress(log, finished_jobs.size() - old_jobs, finished_jobs.size() + scheduled_jobs.size() - old_jobs, stopwatch);
    });
}

void AsyncLoader::prioritize(const LoadJobPtr & job, ssize_t new_priority, std::unique_lock<std::mutex> & lock)
{
    if (auto info = scheduled_jobs.find(job); info != scheduled_jobs.end())
    {
        if (info->second.priority >= new_priority)
            return; // Never lower priority

        // Update priority and push job forward through ready queue if needed
        if (info->second.ready_seqno)
            ready_queue.erase(info->second.key());
        info->second.priority = new_priority;
        job->load_priority.store(new_priority); // Set user-facing priority (may affect executing jobs)
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

void AsyncLoader::enqueue(Info & info, const LoadJobPtr & job, std::unique_lock<std::mutex> & lock)
{
    chassert(!info.is_blocked());
    chassert(info.ready_seqno == 0);
    info.ready_seqno = ++last_ready_seqno;
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        ready_queue.emplace(info.key(), job);
    });

    job->enqueued();

    if (is_running && workers < max_threads)
        spawn(lock);
}

void AsyncLoader::spawn(std::unique_lock<std::mutex> &)
{
    workers++;
    NOEXCEPT_SCOPE({
        ALLOW_ALLOCATIONS_IN_SCOPE;
        pool.scheduleOrThrowOnError([this] { worker(); });
    });
}

void AsyncLoader::worker()
{
    DENY_ALLOCATIONS_IN_SCOPE;

    LoadJobPtr job;
    std::exception_ptr exception_from_job;
    while (true)
    {
        // This is inside the loop to also reset previous thread names set inside the jobs
        setThreadName("AsyncLoader");

        {
            std::unique_lock lock{mutex};

            // Handle just executed job
            if (exception_from_job)
                finish(lock, job, LoadStatus::FAILED, exception_from_job);
            else if (job)
                finish(lock, job, LoadStatus::OK);

            if (!is_running || ready_queue.empty() || workers > max_threads)
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
            job->execute(job);
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

}
