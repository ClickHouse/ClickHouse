#include <Processors/Executors/AsyncTaskQueue.h>
#include <Common/Exception.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_SOCKET;
}


AsyncTaskQueue::AsyncTaskQueue()
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

AsyncTaskQueue::~AsyncTaskQueue()
{
    close(epoll_fd);
    close(pipe_fd[0]);
    close(pipe_fd[1]);
}

void AsyncTaskQueue::addTask(size_t thread_number, void * data, int fd)
{
    auto it = tasks.insert(tasks.end(), TaskData{thread_number, data, fd, {}});
    it->self = it;

    epoll_event socket_event;
    socket_event.events = EPOLLIN | EPOLLPRI;
    socket_event.data.ptr = &(*it);

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &socket_event))
        throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
}

AsyncTaskQueue::TaskData AsyncTaskQueue::wait(std::unique_lock<std::mutex> & lock)
{
    if (is_finished)
        return {};

    lock.unlock();

    epoll_event event;
    int num_events = 0;

    while (num_events == 0)
    {
        num_events = epoll_wait(epoll_fd, &event, 1, 0);
        if (num_events == -1)
            throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }

    lock.lock();

    if (event.data.ptr == pipe_fd)
        return {};

    auto it = static_cast<TaskData *>(event.data.ptr)->self;

    if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, it->fd, &event))
        throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

    auto res = *it;
    tasks.erase(it);
    return res;
}

void AsyncTaskQueue::finish()
{
    is_finished = true;
    tasks.clear();

    uint64_t buf = 0;
    while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            break;

        if (errno != EINTR)
            throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }
}

}
