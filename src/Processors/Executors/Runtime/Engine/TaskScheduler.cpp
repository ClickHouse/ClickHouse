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
    : async{.poller = poller_}
    , local(max_workers)
{
}

void TaskScheduler::pushToLocalQueue(LocalState & own, Task task)
{
    std::lock_guard lock(own.mutex);
    own.queue.pushBack(task);

    if (own.queue.size() > max_local_queue_size)
    {
        std::lock_guard global_lock(global.mutex);
        global.queue.takeFront(own.queue, own.queue.size() / 2);
        global.tasks_count.store(global.queue.size());
    }

    own.tasks_count.store(own.queue.size());
}

void TaskScheduler::pushToGlobalQueue(Task task)
{
    std::lock_guard lock(global.mutex);
    global.queue.pushBack(task);
    global.tasks_count.store(global.queue.size());
}

std::optional<Task> TaskScheduler::takeFromNext(LocalState & own)
{
    if (!own.next)
    {
        own.next_streak = 0;
        return std::nullopt;
    }

    auto task = std::move(std::exchange(own.next, std::nullopt).value());
    auto lifo_usage = own.next_streak += 1;
    if (lifo_usage <= max_sequential_lifo_usages)
        return task;

    std::lock_guard lock(own.mutex);
    own.next_streak = 0;
    own.queue.pushFront(task);
    own.tasks_count.store(own.queue.size());
    return std::nullopt;
}

std::optional<Task> TaskScheduler::takeFromLocal(LocalState & own)
{
    std::lock_guard lock(own.mutex);
    if (own.queue.empty())
        return std::nullopt;

    Task task = own.queue.popBack();
    own.tasks_count.store(own.queue.size());
    return task;
}

std::optional<Task> TaskScheduler::takeFromGlobal()
{
    if (global.tasks_count.load() == 0)
        return std::nullopt;

    std::lock_guard lock(global.mutex);
    if (global.queue.empty())
        return std::nullopt;

    Task task = global.queue.popFront();
    global.tasks_count.store(global.queue.size());
    return task;
}

std::optional<Task> TaskScheduler::takeFromOthers(const LocalState & own)
{
    const size_t start = randomWorker(local.size());
    for (size_t i = 0; i < local.size(); ++i)
    {
        LocalState & other = local[(start + i) % local.size()];
        if (&other == &own || other.tasks_count.load() == 0)
            continue;

        std::lock_guard lock(other.mutex);
        if (other.queue.empty())
            continue;

        Task task = other.queue.popFront();
        other.tasks_count.store(other.queue.size());
        return task;
    }

    return std::nullopt;
}

void TaskScheduler::push(Task task, size_t worker_id)
{
    LocalState & own = local[worker_id];
    if (own.next)
        pushToLocalQueue(own, *own.next);
    own.next = task;
}

void TaskScheduler::push(Task task)
{
    pushToGlobalQueue(std::move(task));
}

void TaskScheduler::push(AsyncTask task)
{
    ++async.tasks_count;
    async.poller.add(*task.state, task.fd, task.events, task.timeout_ms);
}

std::optional<Task> TaskScheduler::tryPop(size_t worker_id)
{
    LocalState & own = local[worker_id];
    auto picks_streak = own.picks_count += 1;

    if (picks_streak % max_sequential_full_rounds == 0)
    {
        if (async.poller.pending() > 0 && poll(worker_id, 0) > 0)
            if (auto task = takeFromLocal(own))
                return task;

        if (auto task = takeFromGlobal())
            return task;
    }

    if (auto task = takeFromNext(own))
        return task;

    if (auto task = takeFromLocal(own))
        return task;

    if (auto task = takeFromGlobal())
        return task;

    if (auto task = takeFromOthers(own))
        return task;

    if (async.poller.pending() > 0)
        if (poll(worker_id, 0) > 0)
            return tryPop(worker_id);

    return std::nullopt;
}

size_t TaskScheduler::poll(size_t worker_id, int timeout_ms)
{
    auto fired = async.poller.poll(timeout_ms);
    if (fired.empty())
        return 0;

    LocalState & own = local[worker_id];
    for (auto * state : fired | std::views::reverse)
        pushToLocalQueue(own, Task{.state = state, .kind = Task::Kind::AsyncReady});

    async.tasks_count -= fired.size();
    return fired.size();
}

void TaskScheduler::drain(size_t worker_id)
{
    LocalState & own = local[worker_id];
    TaskQueue taken;
    {
        std::lock_guard lock(own.mutex);
        taken.takeAll(own.queue);
        own.tasks_count.store(own.queue.size());
    }

    if (own.next)
    {
        taken.pushBack(*own.next);
        own.next.reset();
    }

    std::lock_guard lock(global.mutex);
    global.queue.takeAll(taken);
    global.tasks_count.store(global.queue.size());
}

size_t TaskScheduler::queued() const
{
    size_t result = global.tasks_count.load();
    for (const auto & state : local)
        result += state.tasks_count.load();
    return result;
}

size_t TaskScheduler::total() const
{
    return queued() + async.tasks_count.load();
}

}
