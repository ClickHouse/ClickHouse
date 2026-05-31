#include <Processors/Executors/PollingQueue.h>

#if defined(OS_LINUX)

#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <base/defines.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_SOCKET;
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

std::optional<PollingQueue::Key> PollingQueue::Deadlines::popMin()
{
    if (queue.empty())
        return std::nullopt;

    auto it = queue.begin();
    auto key = it->second;
    queue.erase(it);
    index.erase(key);
    return key;
}

PollingQueue::PollingQueue()
{
    if (-1 == pipe2(pipe_fd, O_NONBLOCK))
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create pipe");

    epoll.add(pipe_fd[0], pipe_fd);
}

PollingQueue::~PollingQueue()
{
    int err;
    err = close(pipe_fd[0]);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(!err || errno == EINTR);
    err = close(pipe_fd[1]);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(!err || errno == EINTR);
}

void PollingQueue::addTask(size_t thread_number, void * data, int fd, uint32_t events, Int64 timeout_ms)
{
    Key key = reinterpret_cast<Key>(data);
    if (tasks.contains(key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} was already added to task queue", key);

    tasks[key] = TaskData{thread_number, data, fd};
    epoll.add(fd, data, events);

    if (timeout_ms >= 0)
        deadlines.arm(key, timeout_ms);
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
    return res;
}

PollingQueue::TaskData PollingQueue::popMinDeadlineTask()
{
    auto min_key = deadlines.popMin();
    if (!min_key)
        return {};

    auto task_it = tasks.find(min_key.value());
    if (task_it == tasks.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min-deadline task {} missing from task map", *min_key);

    auto res = task_it->second;
    tasks.erase(task_it);
    epoll.remove(res.fd);
    return res;
}

static std::string dumpTasks(const std::unordered_map<PollingQueue::Key, PollingQueue::TaskData> & tasks)
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
    if (is_finished)
        return {};

    if (auto expired_task = popExpiredDeadlineTask())
        return expired_task;

    /// We need to wait until next task with deadline will be ready.
    int effective_timeout = timeout;
    if (auto next = deadlines.nextDeadline())
    {
        auto ms_until_deadline = std::max<Int64>(0, std::chrono::duration_cast<std::chrono::milliseconds>(*next - Clock::now()).count());
        if (timeout < 0 || ms_until_deadline < static_cast<Int64>(timeout))
            effective_timeout = static_cast<int>(ms_until_deadline);
    }

    lock.unlock();

    epoll_event event;
    event.data.ptr = nullptr;
    size_t num_events = epoll.getManyReady(1, &event, effective_timeout);

    lock.lock();

    if (num_events == 0)
    {
        if (effective_timeout != timeout)
            return popMinDeadlineTask();

        return {};
    }

    if (event.data.ptr == pipe_fd)
        return {};

    void * ptr = event.data.ptr;
    Key key = reinterpret_cast<Key>(ptr);
    auto it = tasks.find(key);
    if (it == tasks.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} ({}) was not found in task queue: {}",
                        key, ptr, dumpTasks(tasks));
    }

    auto res = it->second;
    tasks.erase(it);
    deadlines.cancel(key);
    epoll.remove(res.fd);

    return res;
}

void PollingQueue::finish()
{
    is_finished = true;

    uint64_t buf = 0;
    while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot write to pipe");
    }
}

}
#endif
