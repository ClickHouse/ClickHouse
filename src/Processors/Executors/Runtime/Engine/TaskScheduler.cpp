#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <ranges>

namespace DB
{

namespace
{

constexpr size_t max_to_take = 7;

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

std::optional<Task> TaskScheduler::takeFromLocal(GuardedQueue & own)
{
    std::lock_guard lock(own.mutex);
    if (own.queue.empty())
        return std::nullopt;

    --total;
    return own.queue.popFront();
}

std::optional<Task> TaskScheduler::keepAndPopFirst(GuardedQueue & own, WorkStealingQueue & taken)
{
    if (taken.empty())
        return std::nullopt;

    Task first = taken.popFront();
    --total;

    std::lock_guard lock(own.mutex);
    own.queue.takeAll(taken);
    return first;
}

std::optional<Task> TaskScheduler::takeFromGlobal(GuardedQueue & own)
{
    WorkStealingQueue taken;
    {
        std::lock_guard lock(global.mutex);
        taken.takeFirst(global.queue, max_to_take);
    }

    return keepAndPopFirst(own, taken);
}

std::optional<Task> TaskScheduler::steal(size_t worker_id)
{
    WorkStealingQueue taken;

    const size_t start = randomWorker(local.size());
    for (size_t i = 0; i < local.size() && taken.size() < max_to_take; ++i)
    {
        const size_t victim = (start + i) % local.size();
        if (victim == worker_id)
            continue;

        std::lock_guard lock(local[victim].mutex);
        taken.takeLast(local[victim].queue, max_to_take - taken.size());
    }

    return keepAndPopFirst(local[worker_id], taken);
}

void TaskScheduler::push(Task task, size_t worker_id)
{
    std::lock_guard lock(local[worker_id].mutex);
    local[worker_id].queue.pushBack(task);
    ++total;
}

void TaskScheduler::push(Task task)
{
    std::lock_guard lock(global.mutex);
    global.queue.pushBack(task);
    ++total;
}

void TaskScheduler::push(AsyncTask task)
{
    poller.add(*task.state, task.fd, task.events, task.timeout_ms);
}

std::optional<Task> TaskScheduler::tryPop(size_t worker_id)
{
    GuardedQueue & own = local[worker_id];

    if (auto task = takeFromLocal(own))
        return task;

    if (auto task = takeFromGlobal(own))
        return task;

    if (auto task = steal(worker_id))
        return task;

    if (poll(worker_id, 0) > 0)
        return takeFromLocal(own);

    return std::nullopt;
}

size_t TaskScheduler::poll(size_t worker_id, int timeout_ms)
{
    auto fired = poller.poll(timeout_ms);
    if (fired.empty())
        return 0;

    std::lock_guard lock(local[worker_id].mutex);

    for (auto * state : fired | std::views::reverse)
        local[worker_id].queue.pushFront(Task{.state = state, .kind = Task::Kind::AsyncReady});

    for (auto * state : fired)
        local[worker_id].queue.pushBack(Task{.state = state, .kind = Task::Kind::Work});

    total += 2 * fired.size();
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

size_t TaskScheduler::size() const
{
    return total.load();
}

}
