#include <Processors/Executors/PollingQueue.h>

#if defined(OS_LINUX)

#include <Common/Exception.h>
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
    epoll_fd = epoll_create(1);
    if (-1 == epoll_fd)
        throwFromErrno("Cannot create epoll descriptor", ErrorCodes::CANNOT_OPEN_FILE);

    if (-1 == pipe2(pipe_fd, O_NONBLOCK))
        throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_OPEN_FILE);

    epoll_event socket_event;
    socket_event.events = EPOLLIN | EPOLLPRI;
    socket_event.data.ptr = pipe_fd;

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, pipe_fd[0], &socket_event))
        throwFromErrno("Cannot add pipe descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
}

PollingQueue::~PollingQueue()
{
    close(epoll_fd);
    close(pipe_fd[0]);
    close(pipe_fd[1]);
}

void PollingQueue::addTask(size_t thread_number, void * data, int fd)
{
    std::uintptr_t key = reinterpret_cast<uintptr_t>(data);
    if (tasks.count(key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task {} was already added to task queue", key);

    tasks[key] = TaskData{thread_number, data, fd};

    epoll_event socket_event;
    socket_event.events = EPOLLIN | EPOLLPRI;
    socket_event.data.ptr = data;

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &socket_event))
        throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
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

PollingQueue::TaskData PollingQueue::wait(std::unique_lock<std::mutex> & lock)
{
    if (is_finished)
        return {};

    lock.unlock();

    epoll_event event;
    event.data.ptr = nullptr;
    int num_events = 0;

    while (num_events <= 0)
    {
        num_events = epoll_wait(epoll_fd, &event, 1, -1);

        if (num_events == -1 && errno != EINTR)
                throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }

    lock.lock();

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

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, res.fd, &event))
        throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

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
            throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }
}

}
#endif
