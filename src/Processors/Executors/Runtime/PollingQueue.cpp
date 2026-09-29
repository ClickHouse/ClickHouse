#include <Processors/Executors/Runtime/PollingQueue.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/Exception.h>
#include <Common/Scheduler/CurrentCPULease.h>
#include <algorithm>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void PollingQueue::Deadlines::arm(Key key, int64_t timeout_ms)
{
    cancel(key);
    index[key] = queue.emplace(Clock::now() + std::chrono::milliseconds(timeout_ms), key);
}

void PollingQueue::Deadlines::cancel(Key key)
{
    if (auto it = index.find(key); it != index.end())
    {
        queue.erase(it->second);
        index.erase(it);
    }
}

std::optional<PollingQueue::Clock::time_point> PollingQueue::Deadlines::nextDeadline() const
{
    if (queue.empty())
        return std::nullopt;

    return queue.begin()->first;
}

std::optional<PollingQueue::Key> PollingQueue::Deadlines::popExpired()
{
    if (queue.empty())
        return std::nullopt;

    auto it = queue.begin();
    auto [deadline, key] = *it;

    if (deadline > Clock::now())
        return std::nullopt;

    queue.erase(it);
    index.erase(key);
    return key;
}

PollingQueue::PollingQueue()
{
    epoll.add(finish_signal.fd(), &finish_signal);
    epoll.add(timer_signal.getDescriptor(), &timer_signal);
}

void PollingQueue::addTask(size_t thread_number, void * data, int fd, uint32_t events, Int64 timeout_ms)
{
    Key key = reinterpret_cast<Key>(data);
    if (tasks.contains(key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} was already added to task queue", key);

    tasks[key] = TaskData{thread_number, data, fd};
    epoll.add(fd, data, events);

    if (timeout_ms >= 0)
    {
        deadlines.arm(key, timeout_ms);
        updateTimer();
    }
}

PollingQueue::TaskData PollingQueue::popExpiredDeadlineTask()
{
    auto expired_key = deadlines.popExpired();
    if (!expired_key)
        return {};

    auto task_it = tasks.find(expired_key.value());
    if (task_it == tasks.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expired-deadline task {} missing from task map", *expired_key);

    auto res = task_it->second;
    tasks.erase(task_it);
    epoll.remove(res.fd);
    updateTimer();
    return res;
}

void PollingQueue::updateTimer()
{
    auto next = deadlines.nextDeadline();
    if (!next)
    {
        timer_signal.reset();
        return;
    }

    auto us_until_deadline = std::chrono::duration_cast<std::chrono::microseconds>(*next - Clock::now()).count();
    timer_signal.setRelative(std::max<int64_t>(1, us_until_deadline));
}

std::string PollingQueue::dumpTasks() const
{
    WriteBufferFromOwnString res;
    res << "Tasks = [";

    for (const auto & task : tasks)
    {
        res << "(id " << task.first << " thread " << task.second.thread_num << " ptr ";
        writePointerHex(task.second.data, res);
        res << " fd " << task.second.fd << ")";
    }

    res << "]";
    return res.str();
}

PollingQueue::TaskData PollingQueue::getTask(std::unique_lock<std::mutex> & lock, int timeout)
{
    while (true)
    {
        if (is_finished)
            return {};

        if (auto expired_task = popExpiredDeadlineTask())
            return expired_task;

        lock.unlock();

        epoll_event event{};
        event.data.ptr = nullptr;
        size_t num_events = 0;
        if (timeout == 0)
        {
            /// Non-blocking probe (tryGetReadyTask): it never sleeps and runs on the hot
            /// task-dispatch path even when regular work is ready, so it must not park the CPU lease.
            num_events = epoll.getManyReady(1, &event, timeout);
        }
        else
        {
            /// A blocking wait with the tasks mutex released is a non-CPU wait: park the current
            /// thread's CPU lease (if any) to free its slot for other work while we block, and unpark
            /// before the mutex is re-acquired below. The lease mutex is taken with no tasks mutex
            /// held, so parking adds no lock-order edge (a guard placed while holding the tasks mutex
            /// would invert against renew()'s resume path).
            CPULeaseParkGuard cpu_park;
            num_events = epoll.getManyReady(1, &event, timeout);
        }

        lock.lock();

        if (num_events == 0)
            return {};

        if (event.data.ptr == &finish_signal)
            return {};

        if (event.data.ptr == &timer_signal)
        {
            timer_signal.drain();
            updateTimer();
            continue;
        }

        void * ptr = event.data.ptr;
        Key key = reinterpret_cast<Key>(ptr);
        auto it = tasks.find(key);
        if (it == tasks.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} ({}) was not found in task queue: {}", key, ptr, dumpTasks());

        auto res = it->second;
        tasks.erase(it);
        deadlines.cancel(key);
        epoll.remove(res.fd);

        return res;
    }
}

void PollingQueue::finish()
{
    is_finished = true;
    finish_signal.notify();
}

}
#endif
