#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>

#include <mutex>

namespace DB
{

namespace Runtime::V2
{

WorkersCoordinator::WorkersCoordinator(WorkerPool & pool_, size_t max_workers)
    : pool(pool_)
    , is_registered(max_workers, false)
{
    sleeping_workers.reserve(max_workers);
}

bool WorkersCoordinator::allIdle() const
{
    return idle_count == registered_workers && polling_count == 0 && pool.global_tasks.size() == 0 && pool.poller.pending() == 0;
}

Worker * WorkersCoordinator::takeAnySleepingWorker()
{
    if (sleeping_workers.empty())
        return nullptr;

    Worker * worker = sleeping_workers.back();
    sleeping_workers.pop_back();
    idle_count.fetch_sub(1);
    return worker;
}

void WorkersCoordinator::wakeOneLocked()
{
    if (Worker * worker = takeAnySleepingWorker())
        worker->wake();
    else if (polling_count > 0)
        pool.poller.wakeup();
}

void WorkersCoordinator::stopLocked()
{
    is_stopped = true;
    while (Worker * worker = takeAnySleepingWorker())
        worker->wake();
    pool.poller.wakeup();
}

void WorkersCoordinator::enter(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (is_registered[worker_id])
        return;

    is_registered[worker_id] = true;
    registered_workers.fetch_add(1);
}

void WorkersCoordinator::leave(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (!is_registered[worker_id])
        return;

    is_registered[worker_id] = false;
    registered_workers.fetch_sub(1);
    pool.workers[worker_id].drain();

    if (allIdle())
        stopLocked();
    else
        wakeOneLocked();
}

std::optional<Task> WorkersCoordinator::wait(Worker & worker)
{
    std::unique_lock lock(mutex);

    if (is_stopped)
        return std::nullopt;

    idle_count.fetch_add(1);

    if (pool.global_tasks.size() > 0)
    {
        idle_count.fetch_sub(1);
        return std::nullopt;
    }

    if (allIdle())
    {
        idle_count.fetch_sub(1);
        stopLocked();
        return std::nullopt;
    }

    if (pool.poller.pending() > 0 && polling_count == 0)
    {
        idle_count.fetch_sub(1);
        polling_count.fetch_add(1);
        lock.unlock();
        auto polled = worker.poll(-1);
        lock.lock();
        polling_count.fetch_sub(1);
        if (is_stopped)
            return std::nullopt;
        return polled;
    }

    sleeping_workers.push_back(&worker);
    lock.unlock();
    worker.park();
    return std::nullopt;
}

size_t WorkersCoordinator::wake(size_t to_wake)
{
    std::vector<Worker *> woken;
    {
        std::lock_guard lock(mutex);
        woken.reserve(std::min(to_wake, sleeping_workers.size()));
        while (woken.size() < to_wake)
        {
            if (Worker * worker = takeAnySleepingWorker())
                woken.push_back(worker);
            else
                break;
        }
    }

    for (Worker * worker : woken)
        worker->wake();

    if (woken.empty() && polling_count > 0)
        pool.poller.wakeup();

    return woken.size();
}

bool WorkersCoordinator::isPolling() const
{
    return polling_count > 0;
}

void WorkersCoordinator::stop()
{
    std::lock_guard lock(mutex);
    stopLocked();
}

bool WorkersCoordinator::stopped() const
{
    return is_stopped;
}

size_t WorkersCoordinator::idle() const
{
    return idle_count + polling_count;
}

}

}
