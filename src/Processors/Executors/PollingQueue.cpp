#include <Processors/Executors/PollingQueue.h>

#if defined(OS_LINUX)

#include <Common/Exception.h>
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


PollingQueue::PollingQueue()
{
    if (-1 == pipe2(pipe_fd, O_NONBLOCK))
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create pipe");

    epoll.add(pipe_fd[0], pipe_fd);
}

PollingQueue::~PollingQueue()
{
    int err;
    err = close(pipe_fd[0]);
    chassert(!err || errno == EINTR);
    err = close(pipe_fd[1]);
    chassert(!err || errno == EINTR);
}

void PollingQueue::addTask(size_t thread_number, void * data, int fd)
{
    std::uintptr_t key = reinterpret_cast<uintptr_t>(data);
    if (tasks.contains(key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} was already added to task queue", key);

    tasks[key] = TaskData{thread_number, data, fd};
    epoll.add(fd, data);
}

static std::string dumpTasks(const std::unordered_map<std::uintptr_t, PollingQueue::TaskData> & tasks)
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

    lock.unlock();

    epoll_event event;
    event.data.ptr = nullptr;
    size_t num_events = epoll.getManyReady(1, &event, timeout);

    lock.lock();

    if (num_events == 0)
        return {};

    if (event.data.ptr == pipe_fd)
        return {};

    void * ptr = event.data.ptr;
    std::uintptr_t key = reinterpret_cast<uintptr_t>(ptr);
    auto it = tasks.find(key);
    if (it == tasks.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} ({}) was not found in task queue: {}",
                        key, ptr, dumpTasks(tasks));
    }

    auto res = it->second;
    tasks.erase(it);
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
