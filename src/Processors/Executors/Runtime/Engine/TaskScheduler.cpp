#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <ranges>

namespace DB
{

namespace
{

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
        own.queue_size.store(local_queue_size);
        own.pushed_since_last_pop = true;
        local_queue_size = own.queue.size();
    }

    if (local_queue_size > max_local_queue_size)
        offloadToGlobalQueue(own);
}

void TaskScheduler::pushToGlobalQueue(Task task)
{
    std::lock_guard lock(global.mutex);
    global.queue.pushBack(task);
    global.queue_size.store(global.queue.size());
}

void TaskScheduler::offloadToGlobalQueue(LocalState & own)
{
    WorkStealingQueue oldest;
    {
        std::lock_guard lock(own.mutex);
        oldest.takeFirst(own.queue, own.queue.size() / 2);
        own.queue_size.store(own.queue.size());
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(oldest);
    global.queue_size.store(global.queue.size());
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

    Task task = own.queue.popBack();
    own.queue_size.store(own.queue.size());
    return task;
}

std::optional<Task> TaskScheduler::takeFromGlobal()
{
    if (global.queue_size.load() == 0)
        return std::nullopt;

    std::lock_guard lock(global.mutex);
    if (global.queue.empty())
        return std::nullopt;

    Task task = global.queue.popFront();
    global.queue_size.store(global.queue.size());
    return task;
}

std::optional<Task> TaskScheduler::steal(size_t worker_id)
{
    const size_t start = randomWorker(local.size());
    for (size_t i = 0; i < local.size(); ++i)
    {
        const size_t victim = (start + i) % local.size();
        if (victim == worker_id || local[victim].queue_size.load() == 0)
            continue;

        std::lock_guard lock(local[victim].mutex);
        if (local[victim].queue.empty())
            continue;

        Task task = local[victim].queue.popFront();
        local[victim].queue_size.store(local[victim].queue.size());
        return task;
    }

    return std::nullopt;
}

void TaskScheduler::push(Task task, size_t worker_id)
{
    pushToLocalQueue(local[worker_id], std::move(task));
}

void TaskScheduler::push(Task task)
{
    pushToGlobalQueue(std::move(task));
}

void TaskScheduler::push(AsyncTask task)
{
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

    if (poller.pending() > 0)
        if (poll(worker_id, 0) > 0)
            return tryPop(worker_id);

    return std::nullopt;
}

bool TaskScheduler::hasTasksForOthers(size_t worker_id) const
{
    return local[worker_id].queue_size.load() > 0 || global.queue_size.load() > 0;
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
        own.queue_size.store(own.queue.size());
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(taken);
    global.queue_size.store(global.queue.size());
}

size_t TaskScheduler::queued() const
{
    size_t result = global.queue_size.load();
    for (const auto & state : local)
        result += state.queue_size.load();
    return result;
}

size_t TaskScheduler::total() const
{
    return queued() + poller.pending();
}

}
