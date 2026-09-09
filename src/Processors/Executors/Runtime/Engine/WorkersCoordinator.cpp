#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <base/scope_guard.h>

namespace DB
{

WorkersCoordinator::WorkersCoordinator(TaskScheduler & scheduler_, Poller & poller_, size_t max_workers)
    : scheduler(scheduler_)
    , poller(poller_)
    , is_registered(max_workers, false)
{
}

bool WorkersCoordinator::allIdle(size_t idle_workers) const
{
    return idle_workers == registered_workers && scheduler.total() == 0;
}

void WorkersCoordinator::wakeOneLocked()
{
    if (sleeping_count > 0)
        have_work.notify_one();
    else if (polling_count > 0)
        poller.wakeup();
}

void WorkersCoordinator::stopLocked()
{
    is_stopped = true;
    have_work.notify_all();
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

    /// Counted as idle before the recheck, so a pusher that does not see us idle has pushed a task we see.
    ++idle_count;
    SCOPE_EXIT(--idle_count);

    if (scheduler.queued() > 0)
        return true;

    if (allIdle(idle_count))
    {
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
    }
    else
    {
        ++sleeping_count;
        have_work.wait(lock);
        --sleeping_count;
    }

    return !is_stopped;
}

void WorkersCoordinator::wakeOne()
{
    std::lock_guard lock(mutex);
    wakeOneLocked();
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
