#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>

namespace DB
{

void WorkersCoordinator::ParkingSpot::park()
{
    wake_up.wait(false);
    wake_up.store(false);
}

void WorkersCoordinator::ParkingSpot::unpark()
{
    wake_up.store(true);
    wake_up.notify_one();
}

WorkersCoordinator::WorkersCoordinator(TaskScheduler & scheduler_, Poller & poller_, size_t max_workers)
    : scheduler(scheduler_)
    , poller(poller_)
    , is_registered(max_workers, false)
    , sleeping_spots(max_workers)
{
    sleeping_threads.reserve(max_workers);
}

bool WorkersCoordinator::allIdle(size_t idle_workers) const
{
    return idle_workers == registered_workers && scheduler.total() == 0;
}

std::optional<size_t> WorkersCoordinator::takeAnySleepingThread()
{
    if (sleeping_threads.empty())
        return std::nullopt;

    size_t worker_id = sleeping_threads.back();
    sleeping_threads.pop_back();
    --sleeping_count;
    --idle_count;
    return worker_id;
}

void WorkersCoordinator::wakeOneLocked()
{
    if (auto worker_id = takeAnySleepingThread())
        sleeping_spots[*worker_id].unpark();
    else if (polling_count > 0)
        poller.wakeup();
}

void WorkersCoordinator::stopLocked()
{
    is_stopped = true;
    while (auto worker_id = takeAnySleepingThread())
        sleeping_spots[*worker_id].unpark();
    poller.wakeup();
}

void WorkersCoordinator::enter(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (is_registered[worker_id])
        return;

    is_registered[worker_id] = true;
    ++registered_workers;
}

void WorkersCoordinator::leave(size_t worker_id)
{
    std::lock_guard lock(mutex);

    if (!is_registered[worker_id])
        return;

    is_registered[worker_id] = false;
    --registered_workers;
    scheduler.drain(worker_id);

    if (allIdle(idle_count))
        stopLocked();
    else
        wakeOneLocked();
}

bool WorkersCoordinator::wait(size_t worker_id)
{
    std::unique_lock lock(mutex);

    if (is_stopped)
        return false;

    ++idle_count;

    if (scheduler.queued() > 0)
    {
        --idle_count;
        return true;
    }

    if (allIdle(idle_count))
    {
        --idle_count;
        stopLocked();
        return false;
    }

    if (poller.pending() > 0 && polling_count == 0)
    {
        ++polling_count;
        lock.unlock();
        scheduler.poll(worker_id, -1);
        lock.lock();
        --polling_count;
        --idle_count;
        return !is_stopped;
    }

    sleeping_threads.push_back(worker_id);
    ++sleeping_count;
    lock.unlock();
    sleeping_spots[worker_id].park();

    return !is_stopped;
}

size_t WorkersCoordinator::wake(size_t to_wake)
{
    std::vector<size_t> woken;
    {
        std::lock_guard lock(mutex);
        woken.reserve(std::min(to_wake, sleeping_threads.size()));
        while (woken.size() < to_wake)
        {
            if (auto worker_id = takeAnySleepingThread())
                woken.push_back(*worker_id);
            else
                break;
        }
    }

    for (size_t worker_id : woken)
        sleeping_spots[worker_id].unpark();

    if (woken.empty() && polling_count > 0)
        poller.wakeup();

    return woken.size();
}

bool WorkersCoordinator::needsPoller() const
{
    return polling_count == 0 && sleeping_count > 0 && poller.pending() > 0;
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
    return idle_count;
}

size_t WorkersCoordinator::registered() const
{
    return registered_workers;
}

}
