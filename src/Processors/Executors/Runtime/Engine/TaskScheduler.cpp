#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <bit>
#include <ranges>

namespace DB
{

namespace
{

constexpr size_t max_to_steal = 7;
constexpr size_t max_local_tasks = 256;
constexpr size_t max_lifo_pops = 128;
constexpr size_t max_pops_between_polls = 128;

bool everyNth(size_t count, size_t period)
{
    chassert(std::has_single_bit(period));
    return (count & (period - 1)) == 0;
}

size_t randomWorker(size_t count)
{
    thread_local pcg64_fast rng(randomSeed());
    return rng() % count;
}

}

TaskScheduler::TaskScheduler(Poller & poller_, size_t max_workers)
    : poller(poller_)
    , local(max_workers)
{
}

std::optional<Task> TaskScheduler::takeFrom(GuardedQueue & from, bool oldest)
{
    std::lock_guard lock(from.mutex);
    if (from.queue.empty())
        return std::nullopt;

    --queued_count;
    --total_count;
    return oldest ? from.queue.popFront() : from.queue.popBack();
}

std::optional<Task> TaskScheduler::takeFromQueues(GuardedQueue & own, bool oldest)
{
    if (oldest)
    {
        if (auto task = takeFrom(global, /*oldest=*/true))
            return task;
        return takeFrom(own, oldest);
    }
    else
    {
        if (auto task = takeFrom(own, oldest))
            return task;
        return takeFrom(global, /*oldest=*/true);
    }
}

std::optional<Task> TaskScheduler::steal(size_t worker_id)
{
    WorkStealingQueue taken;

    const size_t start = randomWorker(local.size());
    for (size_t i = 0; i < local.size() && taken.size() < max_to_steal; ++i)
    {
        const size_t victim = (start + i) % local.size();
        if (victim == worker_id)
            continue;

        std::lock_guard lock(local[victim].mutex);
        taken.takeFirst(local[victim].queue, max_to_steal - taken.size());
    }

    if (!taken.empty())
    {
        std::lock_guard lock(local[worker_id].mutex);
        local[worker_id].queue.takeAll(taken);
    }

    return takeFrom(local[worker_id], /*oldest=*/false);
}

void TaskScheduler::moveOldestHalfToGlobal(GuardedQueue & own)
{
    WorkStealingQueue oldest_half;
    {
        std::lock_guard lock(own.mutex);
        oldest_half.takeFirst(own.queue, own.queue.size() / 2);
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(oldest_half);
}

void TaskScheduler::push(Task task, size_t worker_id)
{
    GuardedQueue & own = local[worker_id];
    bool overflown = false;
    {
        std::lock_guard lock(own.mutex);
        own.queue.pushBack(task);
        overflown = own.queue.size() > max_local_tasks;
        ++queued_count;
        ++total_count;
    }

    if (overflown)
        moveOldestHalfToGlobal(own);
}

void TaskScheduler::push(Task task)
{
    std::lock_guard lock(global.mutex);
    global.queue.pushBack(task);
    ++queued_count;
    ++total_count;
}

void TaskScheduler::push(AsyncTask task)
{
    ++total_count;
    poller.add(*task.state, task.fd, task.events, task.timeout_ms);
}

std::optional<Task> TaskScheduler::tryPop(size_t worker_id)
{
    GuardedQueue & own = local[worker_id];
    const size_t pops = ++own.pops;

    if (everyNth(pops, max_pops_between_polls) && poller.pending() > 0)
        poll(worker_id, 0);

    if (auto task = takeFromQueues(own, /*oldest=*/everyNth(pops, max_lifo_pops)))
        return task;

    if (auto task = steal(worker_id))
        return task;

    if (poll(worker_id, 0) > 0)
        return takeFrom(own, /*oldest=*/false);

    return std::nullopt;
}

size_t TaskScheduler::poll(size_t worker_id, int timeout_ms)
{
    auto fired = poller.poll(timeout_ms);
    if (fired.empty())
        return 0;

    std::lock_guard lock(local[worker_id].mutex);

    for (auto * state : fired | std::views::reverse)
        local[worker_id].queue.pushBack(Task{.state = state, .kind = Task::Kind::AsyncReady});

    queued_count += fired.size();
    return fired.size();
}

void TaskScheduler::drain(size_t worker_id)
{
    WorkStealingQueue taken;
    {
        std::lock_guard lock(local[worker_id].mutex);
        taken.takeAll(local[worker_id].queue);
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(taken);
}

size_t TaskScheduler::queued() const
{
    return queued_count.load();
}

size_t TaskScheduler::total() const
{
    return total_count.load();
}

}
