#include <Common/AsyncLoader.h>

#include <base/defines.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ASYNC_LOAD_SCHEDULE_FAILED;
    extern const int ASYNC_LOAD_FAILED;
    extern const int ASYNC_LOAD_CANCELED;
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

size_t LoadJob::waiters_count() const
{
    std::unique_lock lock{mutex};
    return waiters;
}

void LoadJob::ok()
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::OK;
    if (waiters > 0)
        finished.notify_all();
}

void LoadJob::failed(const std::exception_ptr & ptr)
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::FAILED;
    load_exception = ptr;
    if (waiters > 0)
        finished.notify_all();
}

void LoadJob::canceled(const std::exception_ptr & ptr)
{
    std::unique_lock lock{mutex};
    load_status = LoadStatus::CANCELED;
    load_exception = ptr;
    if (waiters > 0)
        finished.notify_all();
}


AsyncLoader::Task::Task()
    : loader(nullptr)
{}

AsyncLoader::Task::Task(AsyncLoader * loader_, LoadJobSet && jobs_)
    : loader(loader_)
    , jobs(std::move(jobs_))
{}

AsyncLoader::Task::Task(AsyncLoader::Task && o) noexcept
    : loader(std::exchange(o.loader, nullptr))
    , jobs(std::move(o.jobs))
{}

AsyncLoader::Task::~Task()
{
    remove();
}

AsyncLoader::Task & AsyncLoader::Task::operator=(AsyncLoader::Task && o) noexcept
{
    loader = std::exchange(o.loader, nullptr);
    jobs = std::move(o.jobs);
    return *this;
}

void AsyncLoader::Task::merge(AsyncLoader::Task && o)
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

void AsyncLoader::Task::remove()
{
    if (loader)
    {
        loader->remove(jobs);
        detach();
    }
}

void AsyncLoader::Task::detach()
{
    loader = nullptr;
    jobs.clear();
}

AsyncLoader::AsyncLoader(Metric metric_threads, Metric metric_active_threads, size_t max_threads_, bool log_failures_)
    : log_failures(log_failures_)
    , max_threads(max_threads_)
    , pool(metric_threads, metric_active_threads, max_threads)
{}

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

AsyncLoader::Task AsyncLoader::schedule(LoadJobSet && jobs, ssize_t priority)
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
        job->load_priority.store(priority); // Set user-facing priority
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

void AsyncLoader::prioritize(const LoadJobPtr & job, ssize_t new_priority)
{
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
        finished_jobs.erase(job);
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
                throw Exception(ErrorCodes::ASYNC_LOAD_SCHEDULE_FAILED, "Load job dependency cycle detected: {} -> {}", job->name, chain);
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

}
