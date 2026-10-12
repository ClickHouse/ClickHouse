#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>

#include <algorithm>
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
    Wyrd::setName(mutex, "coordinator.mutex");
    Wyrd::setName(registered_workers, "coordinator.registered_workers");
    Wyrd::setName(sleepers_count, "coordinator.sleepers_count");
    Wyrd::setName(stealing_count, "coordinator.stealing_count");
    Wyrd::setName(is_stopped, "coordinator.is_stopped");
}

bool WorkersCoordinator::isFinished() const
{
    return sleepers_count.load() == registered_workers.load() && pool.poller.pending() == 0 && pool.async_ready_tasks.size() == 0 && pool.global_tasks.size() == 0;
}

std::optional<size_t> WorkersCoordinator::takeAnySleepingWorker()
{
    if (sleeping_workers.empty())
        return std::nullopt;

    size_t worker_id = sleeping_workers.back();
    sleeping_workers.pop_back();
    sleepers_count.fetch_sub(1);
    return worker_id;
}

void WorkersCoordinator::stopLocked()
{
    if (is_stopped.load())
        return;

    is_stopped.store(true);
    while (auto worker_id = takeAnySleepingWorker())
        pool.workers[*worker_id].wake();
    pool.poller.stop();
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

    Wyrd::verify(!std::ranges::contains(sleeping_workers, worker_id), "A worker left the pool while it was listed as a sleeper");

    if (isFinished())
        stopLocked();
    else if (auto sleeper = takeAnySleepingWorker())
        pool.workers[*sleeper].wake();
}

size_t WorkersCoordinator::registeredWorkers() const
{
    return registered_workers.load();
}

bool WorkersCoordinator::startStealing()
{
    Wyrd::SpinWait spin_wait;
    Int32 stealing = stealing_count.load();
    while (2 * stealing < registered_workers.load())
    {
        if (stealing_count.compare_exchange_weak(stealing, stealing + 1))
            return true;
        spin_wait.spin();
    }

    return false;
}

void WorkersCoordinator::stopStealing()
{
    stealing_count.fetch_sub(1);
}

bool WorkersCoordinator::hasStealer() const
{
    return stealing_count.load() > 0;
}

bool WorkersCoordinator::startParking(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (is_stopped.load() || !is_registered[worker_id])
        return false;

    sleepers_count.fetch_add(1);
    if (isFinished())
    {
        sleepers_count.fetch_sub(1);
        stopLocked();
        return false;
    }

    sleeping_workers.push_back(worker_id);
    return true;
}

void WorkersCoordinator::becomeNormal(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (std::erase(sleeping_workers, worker_id) > 0)
        sleepers_count.fetch_sub(1);
}

bool WorkersCoordinator::wakeOne()
{
    std::optional<size_t> worker_id;
    {
        std::lock_guard lock(mutex);
        worker_id = takeAnySleepingWorker();
    }

    if (worker_id)
    {
        pool.workers[*worker_id].wake();
        return true;
    }

    return false;
}

bool WorkersCoordinator::hasSleepers() const
{
    return sleepers_count.load() > 0;
}

void WorkersCoordinator::stop()
{
    std::lock_guard lock(mutex);
    stopLocked();
}

bool WorkersCoordinator::isStopped() const
{
    return is_stopped.load();
}

}

}
