#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <ranges>

namespace DB
{

namespace
{

constexpr size_t max_to_steal = 7;
constexpr size_t max_local_queue_size = 128;
constexpr size_t max_sequential_full_rounds = 61;
constexpr size_t max_sequential_lifo_usages = 79;

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

void TaskScheduler::pushToLocalQueue(LocalState & own, Task task)
{
    size_t local_queue_size = 0;
    {
        std::lock_guard lock(own.mutex);
        own.queue.pushBack(task);
        own.pushed_since_last_pop = true;
        local_queue_size = own.queue.size();
        ++queued_count;
    }

    if (local_queue_size > max_local_queue_size)
        offloadToGlobalQueue(own);
}

void TaskScheduler::offloadToGlobalQueue(LocalState & own)
{
    WorkStealingQueue oldest;
    {
        std::lock_guard lock(own.mutex);
        oldest.takeFirst(own.queue, own.queue.size() / 2);
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(oldest);
}

std::optional<Task> TaskScheduler::takeFromLocal(LocalState & own)
{
    std::lock_guard lock(own.mutex);
    if (own.queue.empty())
    {
        own.lifo_used_count = 0;
        return std::nullopt;
    }

    const bool chain_continues = std::exchange(own.pushed_since_last_pop, false);
    own.lifo_used_count = chain_continues ? own.lifo_used_count + 1 : 0;

    if (own.lifo_used_count > max_sequential_lifo_usages)
    {
        own.lifo_used_count = 0;
        own.queue.moveBackToFront();
    }

    --queued_count;
    --total_count;
    return own.queue.popBack();
}

std::optional<Task> TaskScheduler::takeFromGlobal()
{
    std::lock_guard lock(global.mutex);
    if (global.queue.empty())
        return std::nullopt;

    --queued_count;
    --total_count;
    return global.queue.popFront();
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

    if (taken.empty())
        return std::nullopt;

    LocalState & own = local[worker_id];
    {
        std::lock_guard lock(own.mutex);
        own.queue.takeAll(taken);
    }

    return takeFromLocal(own);
}

void TaskScheduler::push(Task task, size_t worker_id)
{
    ++total_count;
    pushToLocalQueue(local[worker_id], task);
}

void TaskScheduler::push(Task task)
{
    ++total_count;
    std::lock_guard lock(global.mutex);
    global.queue.pushBack(task);
    ++queued_count;
}

void TaskScheduler::push(AsyncTask task)
{
    ++total_count;
    poller.add(*task.state, task.fd, task.events, task.timeout_ms);
}

std::optional<Task> TaskScheduler::tryPop(size_t worker_id)
{
    LocalState & own = local[worker_id];

    if (++own.pops_count % max_sequential_full_rounds == 0)
    {
        if (poller.pending() > 0)
            poll(worker_id, 0);

        if (auto task = takeFromGlobal())
            return task;
    }

    if (auto task = takeFromLocal(own))
        return task;

    if (auto task = takeFromGlobal())
        return task;

    if (auto task = steal(worker_id))
        return task;

    if (poll(worker_id, 0) > 0)
        return tryPop(worker_id);

    return std::nullopt;
}

size_t TaskScheduler::poll(size_t worker_id, int timeout_ms)
{
    auto fired = poller.poll(timeout_ms);
    if (fired.empty())
        return 0;

    LocalState & own = local[worker_id];
    for (auto * state : fired | std::views::reverse)
        pushToLocalQueue(own, Task{.state = state, .kind = Task::Kind::AsyncReady});

    return fired.size();
}

void TaskScheduler::drain(size_t worker_id)
{
    LocalState & own = local[worker_id];
    WorkStealingQueue taken;
    {
        std::lock_guard lock(own.mutex);
        taken.takeAll(own.queue);
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
